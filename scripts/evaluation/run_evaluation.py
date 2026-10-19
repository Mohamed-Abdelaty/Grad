#!/usr/bin/env python3
"""
scripts/evaluation/run_evaluation.py
-----------------------------------
Automated, reproducible evaluation harness for ChaosFlow graduation research evidence.

Guarantees:
1. REST API only: No direct database manipulation or backdoor state mutations.
2. 100% Topic Isolation: Generates clean, run-scoped Kafka topics per repetition (eval-stream-<exp>-r<i>).
3. Zero Backlog: Fresh checkpoint and topic ensure 0 old messages are ever read.
4. Concurrency Guard: Enforces strict sequential execution (respecting uq_experiment_runs_single_active).
5. Comprehensive Cool-Down / Pre-Flight Checks:
   - System /health returns 200 OK across all platform components.
   - Active pipeline runs == 0.
   - Pending/running experiment runs == 0.
   - Spark worker cores completely free (coresUsed == 0 via Spark Master JSON API).
   - Consumer lag on previous eval topic == 0.
   - Host free physical RAM >= 1500 MB (with 3-minute settling window; ABORTS on failure).
6. Resumable & Non-Destructive: Skips completed runs without overwriting or deleting any artifacts.
7. Verbatim Raw Storage: Writes raw report.json, children.json, environment.json to docs/evaluation/raw/<name>/.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("eval-harness")


def get_git_commit_hash() -> str:
    """Retrieve the current HEAD git commit hash."""
    try:
        res = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return res.stdout.strip()
    except Exception:
        return "unknown"


def get_free_ram_mb() -> float:
    """Measure free physical RAM in MB on the host OS."""
    if sys.platform == "win32":
        try:
            cmd = "powershell -Command \"(Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory\""
            res = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
            kb = float(res.stdout.strip().splitlines()[-1])
            return kb / 1024.0
        except Exception as exc:
            logger.warning("Failed to query Windows host RAM: %s", exc)
            return 9999.0
    else:
        try:
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemAvailable:"):
                        kb = float(line.split()[1])
                        return kb / 1024.0
        except Exception:
            pass
        return 9999.0


def get_docker_stats_snapshot() -> list[dict[str, Any]]:
    """Capture a snapshot of docker stats for all running containers."""
    try:
        res = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}"],
            capture_output=True,
            text=True,
            check=True,
        )
        lines = [line.strip() for line in res.stdout.strip().splitlines() if line.strip()]
        return [json.loads(line) for line in lines]
    except Exception as exc:
        logger.warning("Failed to capture docker stats snapshot: %s", exc)
        return []


def get_kafka_metrics() -> dict[str, Any]:
    """Capture Kafka data directory size, topic count, and total message count."""
    metrics: dict[str, Any] = {
        "kafka_data_dir_size": "unknown",
        "topic_count": 0,
        "total_messages": 0,
    }
    try:
        # 1. Data directory size
        res_du = subprocess.run(
            ["docker", "exec", "platform-kafka", "du", "-sh", "/var/lib/kafka/data"],
            capture_output=True,
            text=True,
            check=True,
        )
        metrics["kafka_data_dir_size"] = res_du.stdout.strip().split()[0]
    except Exception as exc:
        logger.warning("Failed to get Kafka data dir size: %s", exc)

    try:
        # 2. Topic count
        res_topics = subprocess.run(
            ["docker", "exec", "platform-kafka", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"],
            capture_output=True,
            text=True,
            check=True,
        )
        topics = [t.strip() for t in res_topics.stdout.strip().splitlines() if t.strip()]
        metrics["topic_count"] = len(topics)

        # 3. Total message count across topics via GetOffsetShell
        res_offsets = subprocess.run(
            ["docker", "exec", "platform-kafka", "kafka-run-class", "kafka.tools.GetOffsetShell", "--bootstrap-server", "localhost:9092"],
            capture_output=True,
            text=True,
            check=True,
        )
        total_msgs = 0
        for line in res_offsets.stdout.strip().splitlines():
            parts = line.strip().split(":")
            if len(parts) >= 3 and parts[2].isdigit():
                total_msgs += int(parts[2])
        metrics["total_messages"] = total_msgs
    except Exception as exc:
        logger.warning("Failed to get Kafka topics/offsets: %s", exc)

    return metrics


def get_spark_master_metrics(spark_url: str = "http://localhost:8080/json") -> dict[str, Any]:
    """Query Spark Master REST API for cores used and alive workers."""
    try:
        req = urllib.request.Request(spark_url, headers={"User-Agent": "eval-harness"})
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            cores = data.get("cores", 0)
            cores_used = data.get("coresused", 0)
            return {
                "alive_workers": data.get("aliveworkers", 0),
                "cores_total": cores,
                "cores_used": cores_used,
                "cores_free": cores - cores_used,
            }
    except Exception as exc:
        logger.warning("Failed to query Spark Master metrics: %s", exc)
        return {"alive_workers": 0, "cores_total": 0, "cores_used": 999, "cores_free": 0}


def get_topic_consumer_lag(topic_name: str) -> int:
    """
    Check if any consumer lag exists on the specified topic.
    If topic does not exist or has no consumer groups, returns 0.
    """
    try:
        # Check consumer groups
        res_groups = subprocess.run(
            ["docker", "exec", "platform-kafka", "kafka-consumer-groups", "--bootstrap-server", "localhost:9092", "--list"],
            capture_output=True,
            text=True,
            check=True,
        )
        groups = [g.strip() for g in res_groups.stdout.strip().splitlines() if g.strip()]
        total_lag = 0
        for g in groups:
            res_desc = subprocess.run(
                ["docker", "exec", "platform-kafka", "kafka-consumer-groups", "--bootstrap-server", "localhost:9092", "--describe", "--group", g],
                capture_output=True,
                text=True,
            )
            for line in res_desc.stdout.strip().splitlines():
                if topic_name in line:
                    parts = line.split()
                    # Standard describe format: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID ...
                    if len(parts) >= 6 and parts[5].isdigit():
                        total_lag += int(parts[5])
        return total_lag
    except Exception as exc:
        logger.warning("Failed to query consumer lag for %s: %s", topic_name, exc)
        return 0


def build_pipeline_yaml(
    template: dict[str, Any],
    topic_name: str,
    pipeline_name: str,
) -> str:
    """Construct a complete, isolated streaming pipeline YAML definition."""
    content = {
        "pipeline": {
            "name": pipeline_name,
            "version": "1.0",
        },
        "source": {
            "type": "csv",
            "path": template.get("source_path", "/app/sample-data/sales_300k.csv"),
        },
        "schema": [
            {"name": "transaction_id", "type": "string"},
            {"name": "amount", "type": "double"},
            {"name": "customer_id", "type": "string"},
            {"name": "product_id", "type": "string"},
            {"name": "sale_date", "type": "string"},
        ],
        "processing": {
            "operations": [
                {"type": "remove_nulls"},
                {"type": "filter", "condition": "amount > 0"},
            ]
        },
        "streaming": {
            "enabled": True,
            "trigger_interval_seconds": template.get("trigger_interval_seconds", 2),
            "idle_timeout_seconds": template.get("idle_timeout_seconds", 20),
            "max_runtime_seconds": template.get("max_runtime_seconds", 600),
            "starting_offsets": "earliest",
            "max_offsets_per_trigger": template.get("max_offsets_per_trigger", 10000),
        },
        "messaging": {
            "broker": "kafka",
            "topic": topic_name,
            "partitions": 3,
            "replication_factor": 1,
        },
        "processing_engine": {
            "type": "spark",
        },
        "storage": {
            "type": "hdfs",
            "path": f"/data/output/{pipeline_name}",
        },
        "monitoring": {
            "enabled": True,
        },
    }
    return yaml.dump(content, sort_keys=False)


def build_experiment_yaml(
    experiment_name: str,
    target_service: str,
    fault_type: str,
    duration: int,
    baseline_seconds: int,
    max_recovery_time: float,
    expected_data_loss: int,
) -> str:
    """Construct an experiment YAML definition for a single repetition."""
    content = {
        "experiment": {
            "name": experiment_name,
        },
        "target": {
            "service": target_service,
        },
        "fault": {
            "type": fault_type,
            "duration": duration,
        },
        "validation": {
            "expected_data_loss": expected_data_loss,
            "max_recovery_time": max_recovery_time,
        },
        "repetitions": 1,
        "baseline_seconds": baseline_seconds,
    }
    return yaml.dump(content, sort_keys=False)


def verify_cool_down_status(
    health_ok: bool,
    active_runs: int,
    running_exps: int,
    cores_used: int,
    consumer_lag: int,
    free_ram_mb: float,
    min_free_ram_mb: float,
) -> tuple[bool, str]:
    """Pure decision logic for cool-down checks. Returns (passed, failure_reason)."""
    if not health_ok:
        return False, "Platform health check failed (status != ok)"
    if active_runs > 0:
        return False, f"Active pipeline runs remaining ({active_runs} running)"
    if running_exps > 0:
        return False, f"Running experiment runs remaining ({running_exps} running/pending)"
    if cores_used > 0:
        return False, f"Spark worker cores busy ({cores_used} cores in use)"
    if consumer_lag > 0:
        return False, f"Consumer lag on previous topic ({consumer_lag} messages lagged)"
    if free_ram_mb < min_free_ram_mb:
        return False, f"Free host RAM ({free_ram_mb:.1f} MB) below required threshold ({min_free_ram_mb:.1f} MB)"
    return True, "All cool-down checks passed"


class EvaluationHarness:
    """Orchestrates sequential, reproducible execution of the evaluation matrix."""

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        username: str | None = None,
        password: str | None = None,
        matrix_path: str = "scripts/evaluation/matrix.yaml",
        raw_output_dir: str = "docs/evaluation/raw",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username or os.environ.get("ADMIN_USERNAME", "admin")
        self.password = password or os.environ.get("ADMIN_PASSWORD", "123456")
        self.matrix_path = Path(matrix_path)
        self.raw_output_dir = Path(raw_output_dir)
        self.client = httpx.Client(base_url=self.base_url, timeout=30.0)
        self.token: str | None = None
        self.last_topic_name: str | None = None

    def authenticate(self) -> None:
        """Obtain JWT access token from the backend."""
        logger.info("Authenticating with platform API as '%s'...", self.username)
        res = self.client.post(
            "/api/auth/login",
            data={"username": self.username, "password": self.password},
        )
        if res.status_code != 200:
            raise RuntimeError(f"Authentication failed ({res.status_code}): {res.text}")
        data = res.json()
        self.token = data.get("access_token")
        self.client.headers["Authorization"] = f"Bearer {self.token}"
        logger.info("Successfully authenticated.")

    def cool_down_check(self, min_free_ram_mb: float, timeout_seconds: int = 180) -> None:
        """
        Verify the stack is completely quiescent and ready for an experiment.
        Must satisfy all conditions within timeout_seconds or ABORT the matrix:
        1. All services healthy (/health returns ok).
        2. No pipeline_runs in status running (activeRuns == 0).
        3. No experiment_runs in status pending/running (runningExperiments == 0).
        4. Spark worker cores completely free (coresUsed == 0 via Spark Master JSON API).
        5. Consumer lag on previous eval topic == 0.
        6. Free host RAM >= min_free_ram_mb.
        """
        logger.info("Initiating pre-experiment cool-down checks...")
        start_time = time.monotonic()
        last_reason = "No checks executed yet"

        while time.monotonic() - start_time < timeout_seconds:
            # 1. Health check
            health_ok = False
            try:
                h_res = self.client.get("/health")
                if h_res.status_code == 200 and h_res.json().get("status") == "ok":
                    health_ok = True
            except Exception:
                health_ok = False

            # 2 & 3. Active runs and running experiments
            active_runs = 999
            running_exps = 999
            try:
                d_res = self.client.get("/api/dashboard/summary")
                if d_res.status_code == 200:
                    d_data = d_res.json()
                    active_runs = d_data.get("runs", {}).get("active", 0)
                    running_exps = d_data.get("experiments", {}).get("running", 0)
            except Exception:
                pass

            # 4. Spark cores free
            spark_metrics = get_spark_master_metrics()
            cores_used = spark_metrics.get("cores_used", 999)

            # 5. Consumer lag on previous topic
            consumer_lag = 0
            if self.last_topic_name:
                consumer_lag = get_topic_consumer_lag(self.last_topic_name)

            # 6. Host free RAM
            free_ram = get_free_ram_mb()

            passed, reason = verify_cool_down_status(
                health_ok=health_ok,
                active_runs=active_runs,
                running_exps=running_exps,
                cores_used=cores_used,
                consumer_lag=consumer_lag,
                free_ram_mb=free_ram,
                min_free_ram_mb=min_free_ram_mb,
            )

            if passed:
                logger.info("Cool-down verified: Stack healthy, 0 active runs, 0 busy cores, Free RAM=%.1f MB.", free_ram)
                return

            last_reason = reason
            logger.warning("Cool-down pending (%s). Waiting 4s...", reason)
            time.sleep(4.0)

        # Abort if timeout exceeded
        raise RuntimeError(
            f"ABORTING EVALUATION: Cool-down / pre-flight checks failed to settle after {timeout_seconds} seconds. "
            f"Last failure reason: {last_reason}"
        )

    def run_repetition(
        self,
        exp_id: str,
        target_service: str,
        fault_type: str,
        rep_idx: int,
        settings: dict[str, Any],
        pipeline_template: dict[str, Any],
    ) -> dict[str, Any]:
        """Execute a single repetition as an isolated experiment."""
        exp_name = f"eval-{exp_id}-r{rep_idx}"
        topic_name = f"eval-stream-{exp_id}-r{rep_idx}"
        output_dir = self.raw_output_dir / exp_name
        output_dir.mkdir(parents=True, exist_ok=True)

        report_file = output_dir / "report.json"
        if report_file.exists():
            logger.info("Skipping already completed run: %s", exp_name)
            self.last_topic_name = topic_name
            with open(report_file, "r", encoding="utf-8") as f:
                return json.load(f)

        logger.info("==================================================================")
        logger.info("Starting Run: %s (Target: %s, Fault: %s)", exp_name, target_service, fault_type)
        logger.info("Topic: %s | Free RAM: %.1f MB", topic_name, get_free_ram_mb())
        logger.info("==================================================================")

        # Capture environment pre-flight snapshot
        ram_start = get_free_ram_mb()
        docker_stats_start = get_docker_stats_snapshot()
        kafka_metrics_start = get_kafka_metrics()
        spark_metrics_start = get_spark_master_metrics()
        git_commit = get_git_commit_hash()

        p_yaml = build_pipeline_yaml(pipeline_template, topic_name=topic_name, pipeline_name=f"pipe-{exp_name}")
        e_yaml = build_experiment_yaml(
            experiment_name=exp_name,
            target_service=target_service,
            fault_type=fault_type,
            duration=settings.get("fault_duration_seconds", 20),
            baseline_seconds=settings.get("baseline_seconds", 10),
            max_recovery_time=settings.get("max_recovery_threshold_seconds", 60.0),
            expected_data_loss=settings.get("expected_data_loss", 0),
        )

        t_start = datetime.now(tz=timezone.utc).isoformat()

        # Step 1: Launch pipeline in background (wait=false)
        p_res = self.client.post("/api/pipelines/run?wait=false", json={"yaml_content": p_yaml})
        if p_res.status_code not in (200, 202):
            raise RuntimeError(f"Pipeline launch failed ({p_res.status_code}): {p_res.text}")
        p_data = p_res.json()
        pipeline_run_id = p_data["pipeline_run_id"]
        logger.info("Launched Pipeline Run ID: %s", pipeline_run_id)

        # Step 1b: Poll pipeline progress until records are flowing
        p_flow_start = time.monotonic()
        records_flowing = False
        max_flow_wait = 90.0
        while time.monotonic() - p_flow_start < max_flow_wait:
            time.sleep(1.0)
            prog_res = self.client.get(f"/api/pipeline-runs/{pipeline_run_id}/progress")
            if prog_res.status_code == 200:
                prog = prog_res.json()
                pub = (prog.get("kafka") or {}).get("published")
                rows_read = (prog.get("source") or {}).get("rowsRead")
                recs_in = (prog.get("spark") or {}).get("recordsIn")
                if (pub is not None and pub > 0) or (rows_read is not None and rows_read > 0) or (recs_in is not None and recs_in > 0):
                    records_flowing = True
                    break

        pipeline_to_experiment_delay = round(time.monotonic() - p_flow_start, 2)
        logger.info(
            "Pipeline records flowing=%s after %ss delay. Creating experiment...",
            records_flowing,
            pipeline_to_experiment_delay,
        )

        # Step 2: Create experiment
        create_res = self.client.post(
            "/api/experiments",
            json={"yaml_content": e_yaml, "pipeline_yaml": p_yaml},
        )
        if create_res.status_code not in (200, 201):
            raise RuntimeError(f"Experiment creation failed ({create_res.status_code}): {create_res.text}")
        exp_data = create_res.json()
        experiment_uuid = exp_data["experiment_id"]
        logger.info("Created Experiment ID: %s", experiment_uuid)

        # Step 3: Trigger experiment execution linked to running pipeline
        run_res = self.client.post(
            f"/api/experiments/{experiment_uuid}/run",
            json={
                "yaml_content": e_yaml,
                "pipeline_yaml": p_yaml,
                "pipeline_run_id": pipeline_run_id,
            },
        )
        if run_res.status_code != 202:
            raise RuntimeError(f"Experiment run trigger failed ({run_res.status_code}): {run_res.text}")
        logger.info("Execution triggered with pipeline_run_id=%s. Polling for terminal state...", pipeline_run_id)

        # Step 4: Poll until terminal state
        poll_start = time.monotonic()
        terminal_status = None
        max_poll_time = 450.0

        while time.monotonic() - poll_start < max_poll_time:
            time.sleep(2.0)
            status_res = self.client.get(f"/api/experiments/{experiment_uuid}")
            if status_res.status_code == 200:
                s_data = status_res.json()
                st = s_data.get("latest_run_status") or s_data.get("status")
                if st in {"succeeded", "failed", "cancelled"}:
                    terminal_status = st
                    break

        if not terminal_status:
            raise TimeoutError(f"Experiment {exp_name} timed out after {max_poll_time}s without reaching terminal state.")

        t_end = datetime.now(tz=timezone.utc).isoformat()
        logger.info("Run %s reached terminal state: %s", exp_name, terminal_status)

        # Step 4: Fetch report and children
        rep_res = self.client.get(f"/api/reports/{experiment_uuid}")
        if rep_res.status_code != 200:
            raise RuntimeError(f"Failed to fetch report ({rep_res.status_code}): {rep_res.text}")
        report_json = rep_res.json()

        # Write raw unmodified artifacts
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report_json, f, indent=2)

        children_json = report_json.get("child_runs") or report_json.get("results", {}).get("child_runs") or []
        with open(output_dir / "children.json", "w", encoding="utf-8") as f:
            json.dump(children_json, f, indent=2)

        env_metadata = {
            "experiment_name": exp_name,
            "experiment_id": experiment_uuid,
            "target_service": target_service,
            "fault_type": fault_type,
            "repetition_index": rep_idx,
            "topic_name": topic_name,
            "pipeline_run_id": pipeline_run_id,
            "pipeline_to_experiment_delay_seconds": pipeline_to_experiment_delay,
            "git_commit": git_commit,
            "started_at": t_start,
            "finished_at": t_end,
            "terminal_status": terminal_status,
            "host_free_ram_start_mb": ram_start,
            "host_free_ram_end_mb": get_free_ram_mb(),
            "spark_worker_cores_free_start": spark_metrics_start.get("cores_free"),
            "kafka_data_dir_size_start": kafka_metrics_start.get("kafka_data_dir_size"),
            "kafka_topic_count_start": kafka_metrics_start.get("topic_count"),
            "kafka_total_messages_start": kafka_metrics_start.get("total_messages"),
            "docker_stats_start": docker_stats_start,
        }
        with open(output_dir / "environment.json", "w", encoding="utf-8") as f:
            json.dump(env_metadata, f, indent=2)

        self.last_topic_name = topic_name
        logger.info("Persisted raw outputs to %s", output_dir)
        return report_json

    def execute_matrix(self, pilot_only: bool = False, repetitions_override: int | None = None) -> list[dict[str, Any]]:
        """Run all experiments defined in matrix.yaml."""
        with open(self.matrix_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        settings = cfg.get("settings", {})
        pipeline_template = cfg.get("pipeline_template", {})
        experiments = cfg.get("experiments", [])

        min_free_ram = float(settings.get("min_host_free_ram_mb", 1500))
        cool_down_sec = int(settings.get("cool_down_seconds", 15))
        reps = repetitions_override or (2 if pilot_only else int(settings.get("repetitions_per_experiment", 5)))

        if pilot_only and experiments:
            experiments = [experiments[0]]  # Run only the first experiment for pilot

        self.authenticate()
        all_results = []

        for exp in experiments:
            exp_id = exp["id"]
            target = exp["target_service"]
            fault = exp["fault_type"]

            for r in range(1, reps + 1):
                self.cool_down_check(min_free_ram_mb=min_free_ram)
                if cool_down_sec > 0:
                    time.sleep(cool_down_sec)

                result = self.run_repetition(
                    exp_id=exp_id,
                    target_service=target,
                    fault_type=fault,
                    rep_idx=r,
                    settings=settings,
                    pipeline_template=pipeline_template,
                )
                all_results.append(result)

        logger.info("Evaluation matrix execution completed successfully. Total runs: %d", len(all_results))
        return all_results


def main():
    parser = argparse.ArgumentParser(description="ChaosFlow Graduation Research Evaluation Harness")
    parser.add_argument("--matrix", default="scripts/evaluation/matrix.yaml", help="Path to matrix.yaml")
    parser.add_argument("--pilot", action="store_true", help="Execute pilot run (1 experiment, 2 repetitions)")
    parser.add_argument("--repetitions", type=int, default=None, help="Override repetition count")
    parser.add_argument("--base-url", default="http://localhost:8000", help="Platform API base URL")
    args = parser.parse_args()

    harness = EvaluationHarness(
        base_url=args.base_url,
        matrix_path=args.matrix,
    )
    harness.execute_matrix(pilot_only=args.pilot, repetitions_override=args.repetitions)


if __name__ == "__main__":
    main()
