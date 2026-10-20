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
   - Host 30s average \\Memory\\Pages/sec <= 100.
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


class MeasurementError(RuntimeError):
    """Raised when physical HDFS measurement execution fails (no valid JSON output)."""
    pass


class FlushFileHandler(logging.FileHandler):
    """FileHandler that flushes after every record to enable live tailing."""
    def emit(self, record: logging.LogRecord) -> None:
        super().emit(record)
        self.flush()


# Configure root logging with both Console and File handlers
LOG_FILE_PATH = Path("docs/evaluation/run.log")
LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("eval-harness")
logger.setLevel(logging.INFO)

# Avoid duplicate handlers on re-entry
if not logger.handlers:
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(stream_handler)

    file_handler = FlushFileHandler(str(LOG_FILE_PATH), mode="a", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(file_handler)


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


def get_host_page_faults_per_sec() -> float:
    """Measure Windows host hard page faults per second via 1-sample Performance Counter."""
    if sys.platform == "win32":
        try:
            cmd = "powershell -Command \"(Get-Counter '\\Memory\\Pages/sec' -MaxSamples 1).CounterSamples[0].CookedValue\""
            res = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
            return float(res.stdout.strip().splitlines()[-1])
        except Exception as exc:
            logger.warning("Failed to query Windows Memory\\Pages/sec: %s", exc)
            return 0.0
    return 0.0


def get_host_page_faults_30s_avg() -> float:
    """Measure Windows host average page faults per second over 30 samples."""
    if sys.platform == "win32":
        try:
            cmd = "powershell -Command \"((Get-Counter '\\Memory\\Pages/sec' -SampleInterval 1 -MaxSamples 30).CounterSamples | Measure-Object -Property CookedValue -Average).Average\""
            res = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
            val = float(res.stdout.strip().splitlines()[-1])
            return val
        except Exception as exc:
            logger.warning("Failed to query 30s average Windows Memory\\Pages/sec: %s", exc)
            return 0.0
    return 0.0


def get_kafka_metrics() -> dict[str, Any]:
    """Capture Kafka data directory size, topic count, and total message count."""
    metrics: dict[str, Any] = {
        "kafka_data_dir_size": "unknown",
        "topic_count": 0,
        "total_messages": 0,
    }
    try:
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
        res_topics = subprocess.run(
            ["docker", "exec", "platform-kafka", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"],
            capture_output=True,
            text=True,
            check=True,
        )
        topics = [t.strip() for t in res_topics.stdout.strip().splitlines() if t.strip()]
        metrics["topic_count"] = len(topics)

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
    """Check if any consumer lag exists on the specified topic."""
    try:
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
                    if len(parts) >= 6 and parts[5].isdigit():
                        total_lag += int(parts[5])
        return total_lag
    except Exception as exc:
        logger.warning("Failed to query consumer lag for %s: %s", topic_name, exc)
        return 0


def check_hdfs_metadata_exists(hdfs_output_path: str) -> bool:
    """Return True if the HDFS output path already contains a _spark_metadata directory.

    Uses 'hdfs dfs -test -d <path>/_spark_metadata' inside platform-namenode.
    A return value of True means the path is stale and must NOT be used for a new run.
    """
    raw_path = hdfs_output_path
    if raw_path.startswith("hdfs://"):
        parts = raw_path.split("://", 1)[1]
        raw_path = "/" + parts.split("/", 1)[1] if "/" in parts else "/"
    meta_path = f"{raw_path.rstrip('/')}/_spark_metadata"
    try:
        res = subprocess.run(
            ["docker", "exec", "platform-namenode", "hdfs", "dfs", "-test", "-d", meta_path],
            capture_output=True,
            timeout=15,
        )
        return res.returncode == 0
    except Exception as exc:
        logger.warning("check_hdfs_metadata_exists failed for %s: %s", hdfs_output_path, exc)
        return False


def compute_multiset_comparison(hdfs_output_path: str) -> dict[str, Any]:
    """Compare HDFS Parquet output against expected multiset from sales_300k.csv.

    Expected set = rows in /app/sample-data/sales_300k.csv where amount IS NOT NULL
    and amount > 0 (289,744 rows). Key = (transaction_id, amount, timestamp).
    The input file itself contains 4,730 extra duplicate copies by transaction_id,
    so the expected output multiset also contains those duplicates.

    Returns:
    - missing_rows: count in expected not in output (data-loss criterion; 0 = no loss)
    - extra_rows: count in output not in expected (can legitimately occur in fault runs)
    - duplicate_beyond_baseline: rows whose transaction_id appears more times in output
      than in the expected multiset (pipeline-induced duplication)
    - expected_rows: 289744
    - physical_rows: total rows physically found in HDFS output
    - hdfs_path: normalized HDFS path
    """
    raw_path = hdfs_output_path
    if raw_path.startswith("hdfs://"):
        parts = raw_path.split("://", 1)[1]
        raw_path = "/" + parts.split("/", 1)[1] if "/" in parts else "/"

    py_cmd = (
        "import urllib.request, json, io, sys, os, csv, collections, posixpath\n"
        "import pyarrow.parquet as pq\n"
        "hdfs_path = sys.argv[1]\n"
        "csv_path = '/app/sample-data/sales_300k.csv'\n"
        "expected_counts = collections.Counter()\n"
        "try:\n"
        "    with open(csv_path, newline='', encoding='utf-8') as f:\n"
        "        reader = csv.DictReader(f)\n"
        "        for row in reader:\n"
        "            amt_str = row.get('amount', '')\n"
        "            if amt_str is None or amt_str.strip() == '' or amt_str.strip().lower() == 'null':\n"
        "                continue\n"
        "            try:\n"
        "                amt = float(amt_str)\n"
        "            except ValueError:\n"
        "                continue\n"
        "            if amt <= 0:\n"
        "                continue\n"
        "            tid = row.get('transaction_id', '').strip()\n"
        "            ts = row.get('timestamp', '').strip()\n"
        "            expected_counts[(tid, str(amt), ts)] += 1\n"
        "except Exception as e:\n"
        "    sys.stderr.write(f'ERROR: CSV read failed: {e}\\n')\n"
        "    sys.stderr.flush()\n"
        "    os._exit(1)\n"
        "output_counts = collections.Counter()\n"
        "physical_rows = 0\n"
        "try:\n"
        "    meta_url = f'http://namenode:9870/webhdfs/v1{hdfs_path}/_spark_metadata?op=LISTSTATUS'\n"
        "    try:\n"
        "        with urllib.request.urlopen(meta_url) as r:\n"
        "            meta_data = json.load(r)\n"
        "        meta_files = sorted(\n"
        "            [f['pathSuffix'] for f in meta_data.get('FileStatuses', {}).get('FileStatus', [])],\n"
        "            key=lambda x: int(x.split('.')[0]) if x.split('.')[0].isdigit() else -1\n"
        "        )\n"
        "        committed = set()\n"
        "        for mf in meta_files:\n"
        "            murl = f'http://namenode:9870/webhdfs/v1{hdfs_path}/_spark_metadata/{mf}?op=OPEN'\n"
        "            with urllib.request.urlopen(murl) as r:\n"
        "                for line in r.read().decode('utf-8').splitlines():\n"
        "                    line = line.strip()\n"
        "                    if line.startswith('{'):\n"
        "                        try:\n"
        "                            entry = json.loads(line)\n"
        "                            if entry.get('action') == 'add':\n"
        "                                committed.add(posixpath.basename(entry['path']))\n"
        "                        except Exception:\n"
        "                            pass\n"
        "        files = sorted([fn for fn in committed if fn.endswith('.parquet')])\n"
        "    except Exception:\n"
        "        url = f'http://namenode:9870/webhdfs/v1{hdfs_path}?op=LISTSTATUS'\n"
        "        with urllib.request.urlopen(url) as r:\n"
        "            data = json.load(r)\n"
        "        files = [f['pathSuffix'] for f in data.get('FileStatuses', {}).get('FileStatus', []) if f['pathSuffix'].endswith('.parquet')]\n"
        "    for fn in files:\n"
        "        furl = f'http://namenode:9870/webhdfs/v1{hdfs_path}/{fn}?op=OPEN'\n"
        "        with urllib.request.urlopen(furl) as fr:\n"
        "            content = fr.read()\n"
        "        tbl = pq.read_table(io.BytesIO(content))\n"
        "        physical_rows += len(tbl)\n"
        "        cols = tbl.column_names\n"
        "        tids = tbl['transaction_id'].to_pylist() if 'transaction_id' in cols else [''] * len(tbl)\n"
        "        amts = tbl['amount'].to_pylist() if 'amount' in cols else [0.0] * len(tbl)\n"
        "        tss = tbl['timestamp'].to_pylist() if 'timestamp' in cols else [''] * len(tbl)\n"
        "        for tid, amt, ts in zip(tids, amts, tss):\n"
        "            output_counts[(str(tid), str(amt), str(ts))] += 1\n"
        "except Exception as e:\n"
        "    sys.stderr.write(f'ERROR: HDFS read failed: {e}\\n')\n"
        "    sys.stderr.flush()\n"
        "    os._exit(1)\n"
        "missing = 0\n"
        "extra = 0\n"
        "dup_beyond = 0\n"
        "all_keys = set(expected_counts) | set(output_counts)\n"
        "for key in all_keys:\n"
        "    exp = expected_counts.get(key, 0)\n"
        "    got = output_counts.get(key, 0)\n"
        "    if got < exp:\n"
        "        missing += exp - got\n"
        "    elif got > exp:\n"
        "        extra += got - exp\n"
        "        dup_beyond += got - exp\n"
        "expected_total = sum(expected_counts.values())\n"
        "print(json.dumps({\n"
        "    'missing_rows': missing,\n"
        "    'extra_rows': extra,\n"
        "    'duplicate_beyond_baseline': dup_beyond,\n"
        "    'expected_rows': expected_total,\n"
        "    'physical_rows': physical_rows,\n"
        "}))\n"
        "sys.stdout.flush()\n"
        "os._exit(0)\n"
    )
    try:
        res = subprocess.run(
            ["docker", "exec", "platform-backend", "python", "-c", py_cmd, raw_path],
            capture_output=True,
            text=True,
            timeout=180,
        )
        stdout = (getattr(res, 'stdout', '') or '').strip()
        stderr = (getattr(res, 'stderr', '') or '').strip()
        data = None
        for line in reversed(stdout.splitlines()):
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    data = json.loads(line)
                    break
                except Exception:
                    pass

        if data is not None and "missing_rows" in data and data.get("missing_rows") != -1:
            data["hdfs_path"] = raw_path
            return data

        logger.error(
            "compute_multiset_comparison failed for %s (exit code %s).\nSTDOUT: %s\nSTDERR: %s",
            hdfs_output_path,
            res.returncode,
            stdout,
            stderr,
        )
        raise MeasurementError(
            f"compute_multiset_comparison failed for {hdfs_output_path} (exit code {res.returncode}): {stderr or stdout or 'no output'}"
        )
    except Exception as exc:
        if isinstance(exc, MeasurementError):
            raise
        logger.error("compute_multiset_comparison execution failed for %s: %s", hdfs_output_path, exc)
        raise MeasurementError(f"compute_multiset_comparison execution failed for {hdfs_output_path}: {exc}") from exc


def get_hdfs_physical_metrics(hdfs_output_path: str) -> dict[str, Any]:
    """Physically inspect all Parquet files in an HDFS output path via WebHDFS.

    Queries WebHDFS from the platform-backend container via PyArrow.
    Does NOT rely on progress trackers or Spark driver in-memory counters.
    Returns:
    - physical_rows: total number of rows across all parquet files
    - unique_transaction_ids: number of distinct transaction_id values
    - duplicate_transaction_ids: total physical rows - unique transaction IDs
    - baseline_csv_duplicates: 4730 (expected duplicate copies in valid 289,744 input rows)
    - excess_duplicates: max(0, duplicate_transaction_ids - 4730)
    - file_count: number of parquet files
    - raw_path: normalized HDFS path
    """
    raw_path = hdfs_output_path
    if raw_path.startswith("hdfs://"):
        parts = raw_path.split("://", 1)[1]
        raw_path = "/" + parts.split("/", 1)[1] if "/" in parts else "/"

    py_cmd = (
        "import urllib.request, json, io, sys, os, posixpath\n"
        "import pyarrow.parquet as pq\n"
        "path = sys.argv[1]\n"
        "try:\n"
        "    meta_url = f'http://namenode:9870/webhdfs/v1{path}/_spark_metadata?op=LISTSTATUS'\n"
        "    try:\n"
        "        with urllib.request.urlopen(meta_url) as r:\n"
        "            meta_data = json.load(r)\n"
        "        meta_files = sorted(\n"
        "            [f['pathSuffix'] for f in meta_data.get('FileStatuses', {}).get('FileStatus', [])],\n"
        "            key=lambda x: int(x.split('.')[0]) if x.split('.')[0].isdigit() else -1\n"
        "        )\n"
        "        committed = set()\n"
        "        for mf in meta_files:\n"
        "            murl = f'http://namenode:9870/webhdfs/v1{path}/_spark_metadata/{mf}?op=OPEN'\n"
        "            with urllib.request.urlopen(murl) as r:\n"
        "                for line in r.read().decode('utf-8').splitlines():\n"
        "                    line = line.strip()\n"
        "                    if line.startswith('{'):\n"
        "                        try:\n"
        "                            entry = json.loads(line)\n"
        "                            if entry.get('action') == 'add':\n"
        "                                committed.add(posixpath.basename(entry['path']))\n"
        "                        except Exception:\n"
        "                            pass\n"
        "        files = sorted([fn for fn in committed if fn.endswith('.parquet')])\n"
        "    except Exception:\n"
        "        url = f'http://namenode:9870/webhdfs/v1{path}?op=LISTSTATUS'\n"
        "        with urllib.request.urlopen(url) as r:\n"
        "            data = json.load(r)\n"
        "        files = [f['pathSuffix'] for f in data.get('FileStatuses', {}).get('FileStatus', []) if f['pathSuffix'].endswith('.parquet')]\n"
        "    total_rows = 0\n"
        "    unique_ids = set()\n"
        "    total_ids = 0\n"
        "    for fn in files:\n"
        "        furl = f'http://namenode:9870/webhdfs/v1{path}/{fn}?op=OPEN'\n"
        "        with urllib.request.urlopen(furl) as fr:\n"
        "            content = fr.read()\n"
        "            pf = pq.ParquetFile(io.BytesIO(content))\n"
        "            num_rows = pf.metadata.num_rows\n"
        "            total_rows += num_rows\n"
        "            if num_rows > 0:\n"
        "                tbl = pq.read_table(io.BytesIO(content))\n"
        "                if 'transaction_id' in tbl.column_names:\n"
        "                    col = tbl['transaction_id'].to_pylist()\n"
        "                    total_ids += len(col)\n"
        "                    unique_ids.update(col)\n"
        "    dup_ids = total_ids - len(unique_ids) if total_ids > 0 else 0\n"
        "    base_dups = 4730\n"
        "    excess = max(0, dup_ids - base_dups)\n"
        "    print(json.dumps({\n"
        "        'physical_rows': total_rows,\n"
        "        'unique_transaction_ids': len(unique_ids),\n"
        "        'duplicate_transaction_ids': dup_ids,\n"
        "        'baseline_csv_duplicates': base_dups,\n"
        "        'excess_duplicates': excess,\n"
        "        'file_count': len(files),\n"
        "    }))\n"
        "    sys.stdout.flush()\n"
        "    os._exit(0)\n"
        "except Exception as e:\n"
        "    sys.stderr.write(f'ERROR: {e}\\n')\n"
        "    sys.stderr.flush()\n"
        "    os._exit(1)\n"
    )
    try:
        res = subprocess.run(
            ["docker", "exec", "platform-backend", "python", "-c", py_cmd, raw_path],
            capture_output=True,
            text=True,
            timeout=180,
        )
        stdout = (getattr(res, 'stdout', '') or '').strip()
        stderr = (getattr(res, 'stderr', '') or '').strip()
        data = None
        for line in reversed(stdout.splitlines()):
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    data = json.loads(line)
                    break
                except Exception:
                    pass

        if data is not None and "physical_rows" in data:
            data["raw_path"] = raw_path
            return data

        logger.error(
            "Failed to count HDFS physical metrics for %s (exit code %s).\nSTDOUT: %s\nSTDERR: %s",
            hdfs_output_path,
            res.returncode,
            stdout,
            stderr,
        )
        raise MeasurementError(
            f"Failed to count HDFS physical metrics for {hdfs_output_path} (exit code {res.returncode}): {stderr or stdout or 'no output'}"
        )
    except Exception as exc:
        if isinstance(exc, MeasurementError):
            raise
        logger.error("Failed to execute physical metrics command for %s: %s", hdfs_output_path, exc)
        raise MeasurementError(f"Failed to execute physical metrics command for {hdfs_output_path}: {exc}") from exc


def count_hdfs_physical_rows(hdfs_output_path: str) -> int:
    """Physically count data rows across all Parquet part files in an HDFS output path.

    Queries WebHDFS from the platform-backend container via PyArrow metadata inspection.
    Does NOT rely on progress trackers or Spark driver in-memory counters.
    Returns 0 if the path does not exist, has no files, or contains only empty files.
    """
    raw_path = hdfs_output_path
    if raw_path.startswith("hdfs://"):
        parts = raw_path.split("://", 1)[1]
        raw_path = "/" + parts.split("/", 1)[1] if "/" in parts else "/"

    py_cmd = (
        "import urllib.request, json, io, sys, os, posixpath\n"
        "import pyarrow.parquet as pq\n"
        "path = sys.argv[1]\n"
        "try:\n"
        "    meta_url = f'http://namenode:9870/webhdfs/v1{path}/_spark_metadata?op=LISTSTATUS'\n"
        "    try:\n"
        "        with urllib.request.urlopen(meta_url) as r:\n"
        "            meta_data = json.load(r)\n"
        "        meta_files = sorted(\n"
        "            [f['pathSuffix'] for f in meta_data.get('FileStatuses', {}).get('FileStatus', [])],\n"
        "            key=lambda x: int(x.split('.')[0]) if x.split('.')[0].isdigit() else -1\n"
        "        )\n"
        "        committed = set()\n"
        "        for mf in meta_files:\n"
        "            murl = f'http://namenode:9870/webhdfs/v1{path}/_spark_metadata/{mf}?op=OPEN'\n"
        "            with urllib.request.urlopen(murl) as r:\n"
        "                for line in r.read().decode('utf-8').splitlines():\n"
        "                    line = line.strip()\n"
        "                    if line.startswith('{'):\n"
        "                        try:\n"
        "                            entry = json.loads(line)\n"
        "                            if entry.get('action') == 'add':\n"
        "                                committed.add(posixpath.basename(entry['path']))\n"
        "                        except Exception:\n"
        "                            pass\n"
        "        files = sorted([fn for fn in committed if fn.endswith('.parquet')])\n"
        "    except Exception:\n"
        "        url = f'http://namenode:9870/webhdfs/v1{path}?op=LISTSTATUS'\n"
        "        with urllib.request.urlopen(url) as r:\n"
        "            data = json.load(r)\n"
        "        files = [f['pathSuffix'] for f in data.get('FileStatuses', {}).get('FileStatus', []) if f['pathSuffix'].endswith('.parquet')]\n"
        "    total = 0\n"
        "    for fn in files:\n"
        "        furl = f'http://namenode:9870/webhdfs/v1{path}/{fn}?op=OPEN'\n"
        "        with urllib.request.urlopen(furl) as fr:\n"
        "            total += pq.ParquetFile(io.BytesIO(fr.read())).metadata.num_rows\n"
        "    print(json.dumps({'physical_rows': total}))\n"
        "    sys.stdout.flush()\n"
        "    os._exit(0)\n"
        "except Exception as e:\n"
        "    sys.stderr.write(f'ERROR: {e}\\n')\n"
        "    sys.stderr.flush()\n"
        "    os._exit(1)\n"
    )
    try:
        res = subprocess.run(
            ["docker", "exec", "platform-backend", "python", "-c", py_cmd, raw_path],
            capture_output=True,
            text=True,
            timeout=180,
        )
        stdout = (getattr(res, 'stdout', '') or '').strip()
        stderr = (getattr(res, 'stderr', '') or '').strip()
        for line in reversed(stdout.splitlines()):
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    data = json.loads(line)
                    if "physical_rows" in data:
                        return int(data["physical_rows"])
                except Exception:
                    pass
            elif line.isdigit():
                return int(line)

        logger.error(
            "Failed to count HDFS physical rows for %s (exit code %s).\nSTDOUT: %s\nSTDERR: %s",
            hdfs_output_path,
            res.returncode,
            stdout,
            stderr,
        )
        raise MeasurementError(
            f"Failed to count HDFS physical rows for {hdfs_output_path} (exit code {res.returncode}): {stderr or stdout or 'no output'}"
        )
    except Exception as exc:
        if isinstance(exc, MeasurementError):
            raise
        logger.error("Failed to execute physical row count for %s: %s", hdfs_output_path, exc)
        raise MeasurementError(f"Failed to execute physical row count for {hdfs_output_path}: {exc}") from exc


def build_pipeline_yaml(
    template: dict[str, Any],
    topic_name: str,
    pipeline_name: str,
    output_path: str | None = None,
) -> str:
    """Construct a complete, isolated streaming pipeline YAML definition."""
    effective_output_path = output_path if output_path is not None else f"/data/output/{pipeline_name}"
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
            {"name": "timestamp", "type": "string"},
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
            "max_offsets_per_trigger": template.get("max_offsets_per_trigger", 3000),
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
            "path": effective_output_path,
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
    pages_sec_30s_avg: float | None = None,
    max_pages_sec: float = 100.0,
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
    if pages_sec_30s_avg is not None and pages_sec_30s_avg > max_pages_sec:
        return False, f"Host 30s average page faults ({pages_sec_30s_avg:.1f} Pages/sec) above threshold ({max_pages_sec:.1f})"
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
        7. 30s average host Memory\\Pages/sec <= 100.
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

            # First verify lightweight items
            passed_basic, reason_basic = verify_cool_down_status(
                health_ok=health_ok,
                active_runs=active_runs,
                running_exps=running_exps,
                cores_used=cores_used,
                consumer_lag=consumer_lag,
                free_ram_mb=free_ram,
                min_free_ram_mb=min_free_ram_mb,
            )

            if not passed_basic:
                last_reason = reason_basic
                logger.warning("Cool-down pending (%s). Waiting 4s...", reason_basic)
                time.sleep(4.0)
                continue

            # 7. Check 30s average Pages/sec
            logger.info("Basic pre-flight checks passed. Measuring 30s average Pages/sec...")
            pages_sec_30s = get_host_page_faults_30s_avg()
            passed_all, reason_all = verify_cool_down_status(
                health_ok=health_ok,
                active_runs=active_runs,
                running_exps=running_exps,
                cores_used=cores_used,
                consumer_lag=consumer_lag,
                free_ram_mb=free_ram,
                min_free_ram_mb=min_free_ram_mb,
                pages_sec_30s_avg=pages_sec_30s,
                max_pages_sec=100.0,
            )

            if passed_all:
                logger.info(
                    "Cool-down verified: Stack healthy, 0 active runs, 0 busy cores, Free RAM=%.1f MB, Pages/sec=%.1f.",
                    free_ram,
                    pages_sec_30s,
                )
                return

            last_reason = reason_all
            logger.warning("Cool-down pending (%s). Waiting 4s...", reason_all)
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
        logger.info("EXPERIMENT START: %s (Target: %s, Fault: %s)", exp_name, target_service, fault_type)
        logger.info("Topic: %s | Free RAM: %.1f MB", topic_name, get_free_ram_mb())
        logger.info("==================================================================")

        # Capture environment pre-flight snapshot
        ram_start = get_free_ram_mb()
        docker_stats_start = get_docker_stats_snapshot()
        kafka_metrics_start = get_kafka_metrics()
        spark_metrics_start = get_spark_master_metrics()
        git_commit = get_git_commit_hash()

        rep_output_path = f"/data/output/pipe-{exp_name}-{int(time.time())}"

        # Guard: refuse to start if _spark_metadata already exists at the target path
        if check_hdfs_metadata_exists(rep_output_path):
            raise RuntimeError(
                f"RUN ABORTED: Target HDFS path {rep_output_path} already contains "
                "_spark_metadata. This run would be INVALID. "
                "Generate a new unique path."
            )

        p_yaml = build_pipeline_yaml(
            pipeline_template,
            topic_name=topic_name,
            pipeline_name=f"pipe-{exp_name}",
            output_path=rep_output_path,
        )
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
        logger.info("Launched Pipeline Run ID: %s for %s", pipeline_run_id, exp_name)

        # Step 1b: Poll pipeline progress until at least one micro-batch is finished
        p_wait_start = time.monotonic()
        first_batch_finished = False
        max_micro_batch_wait = 90.0

        while time.monotonic() - p_wait_start < max_micro_batch_wait:
            time.sleep(1.0)
            prog_res = self.client.get(f"/api/pipeline-runs/{pipeline_run_id}/progress")
            if prog_res.status_code == 200:
                prog = prog_res.json()
                spark_dict = prog.get("spark") or {}
                recs_in = spark_dict.get("recordsIn") or 0
                recs_out = spark_dict.get("recordsOut") or 0
                if recs_in > 0 or recs_out > 0:
                    first_batch_finished = True
                    break

        first_micro_batch_wait_seconds = round(time.monotonic() - p_wait_start, 2)
        if not first_batch_finished:
            raise TimeoutError(
                f"ABORTING RUN: Pipeline {pipeline_run_id} failed to complete its first micro-batch "
                f"within {max_micro_batch_wait}s."
            )

        logger.info(
            "PHASE CHANGE: First micro-batch completed (wait=%.2fs). Creating experiment %s...",
            first_micro_batch_wait_seconds,
            exp_name,
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
        logger.info("Created Experiment ID: %s for %s", experiment_uuid, exp_name)

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
        logger.info("PHASE CHANGE: Execution triggered for %s. Monitoring until terminal state...", exp_name)

        # Step 4: Poll until terminal state with 15s telemetry sampling
        poll_start = time.monotonic()
        terminal_status = None
        max_poll_time = 450.0

        telemetry_samples: list[dict[str, Any]] = []
        last_sample_time = 0.0
        consecutive_high_pages_sec_count = 0
        host_memory_pressure_flag = False

        while time.monotonic() - poll_start < max_poll_time:
            now = time.monotonic()
            if now - last_sample_time >= 15.0:
                cur_ram = get_free_ram_mb()
                cur_pages = get_host_page_faults_per_sec()
                last_sample_time = now
                telemetry_samples.append({
                    "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                    "elapsed_seconds": round(now - poll_start, 1),
                    "free_ram_mb": cur_ram,
                    "pages_per_sec": cur_pages,
                })
                logger.info("Telemetry [t+%.0fs]: Free RAM=%.1f MB | Pages/sec=%.1f", now - poll_start, cur_ram, cur_pages)
                if cur_ram < 1500.0:
                    host_memory_pressure_flag = True
                if cur_pages > 100.0:
                    consecutive_high_pages_sec_count += 1
                    if consecutive_high_pages_sec_count >= 2:
                        host_memory_pressure_flag = True
                else:
                    consecutive_high_pages_sec_count = 0

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
        logger.info(
            "EXPERIMENT FINISHED: %s | Status: %s | Memory Pressure: %s",
            exp_name,
            terminal_status,
            host_memory_pressure_flag,
        )

        # Step 5: Fetch report and children
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

        # Independent physical HDFS metrics
        rep_expected_rows = 289744
        measurement_error = None
        try:
            physical_metrics = get_hdfs_physical_metrics(rep_output_path)
            rep_phys_rows = physical_metrics.get("physical_rows", 0)
            rep_unique_ids = physical_metrics.get("unique_transaction_ids", 0)
            rep_dup_ids = physical_metrics.get("duplicate_transaction_ids", 0)
            rep_baseline_dups = physical_metrics.get("baseline_csv_duplicates", 4730)
            rep_excess_dups = physical_metrics.get("excess_duplicates", 0)
            rep_data_loss = max(0, rep_expected_rows - rep_phys_rows)
            rep_multiset = compute_multiset_comparison(rep_output_path)
        except MeasurementError as me:
            measurement_error = str(me)
            physical_metrics = {
                "raw_path": rep_output_path,
                "physical_rows": -1,
                "unique_transaction_ids": -1,
                "duplicate_transaction_ids": -1,
                "baseline_csv_duplicates": 4730,
                "excess_duplicates": -1,
                "file_count": 0,
                "error": measurement_error,
            }
            rep_phys_rows = -1
            rep_unique_ids = -1
            rep_dup_ids = -1
            rep_baseline_dups = 4730
            rep_excess_dups = -1
            rep_data_loss = -1
            rep_multiset = {
                "hdfs_path": rep_output_path,
                "missing_rows": -1,
                "extra_rows": -1,
                "duplicate_beyond_baseline": -1,
                "expected_rows": rep_expected_rows,
                "physical_rows": -1,
                "error": measurement_error,
            }

        results_dict = report_json.get("results") or {}

        if measurement_error:
            rep_run_valid = False
            run_classification = "MEASUREMENT_FAILED"
        elif rep_phys_rows == 0:
            rep_run_valid = False
            run_classification = "INVALID: empty output"
        else:
            rep_run_valid = (
                rep_phys_rows > 0
                and rep_multiset.get("missing_rows", -1) >= 0
                and not rep_multiset.get("error")
            )
            run_classification = "VALID" if rep_run_valid else "INVALID"

        logger.info(
            "RUN MULTISET [%s]: missing=%s extra=%s dup_beyond=%s classification=%s",
            exp_name,
            rep_multiset.get("missing_rows"),
            rep_multiset.get("extra_rows"),
            rep_multiset.get("duplicate_beyond_baseline"),
            run_classification,
        )

        physical_audit = {
            "output_path": rep_output_path,
            "expected_rows": rep_expected_rows,
            "physical_rows": rep_phys_rows,
            "unique_transaction_ids": rep_unique_ids,
            "duplicate_transaction_ids": rep_dup_ids,
            "baseline_csv_duplicates": rep_baseline_dups,
            "excess_duplicates": rep_excess_dups,
            "physical_data_loss": rep_data_loss,
            "file_count": physical_metrics.get("file_count", 0),
            "run_valid": rep_run_valid,
            "run_classification": run_classification,
            "multiset_comparison": rep_multiset,
            "platform_validator": {
                "label": "platform validator (unreliable, uses tracker counters)",
                "data_loss": results_dict.get("dataLoss"),
                "duplicates": results_dict.get("duplicates"),
                "expected_records": results_dict.get("expectedRecords"),
                "processed_records": results_dict.get("processedRecords"),
            },
        }
        with open(output_dir / "physical_metrics.json", "w", encoding="utf-8") as f:
            json.dump(physical_audit, f, indent=2)

        env_metadata = {
            "experiment_name": exp_name,
            "experiment_id": experiment_uuid,
            "target_service": target_service,
            "fault_type": fault_type,
            "repetition_index": rep_idx,
            "topic_name": topic_name,
            "pipeline_run_id": pipeline_run_id,
            "output_path": rep_output_path,
            "physical_metrics": physical_audit,
            "first_micro_batch_wait_seconds": first_micro_batch_wait_seconds,
            "git_commit": git_commit,
            "started_at": t_start,
            "finished_at": t_end,
            "terminal_status": terminal_status,
            "host_free_ram_start_mb": ram_start,
            "host_free_ram_end_mb": get_free_ram_mb(),
            "host_memory_pressure": host_memory_pressure_flag,
            "telemetry_samples": telemetry_samples,
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

    def run_calibration(self) -> dict[str, Any]:
        """
        Run the streaming pipeline alone with NO fault injection.
        Measures:
        - Time from start to first non-zero recordsIn (first micro-batch)
        - Rows per second per micro-batch and per-batch offsets
        - Total duration and duration after first non-zero counters
        - Free host RAM, Pages/sec, and docker stats peak memory
        Saves all outputs to docs/evaluation/raw/calibration/.
        """
        self.authenticate()
        cal_dir = self.raw_output_dir / "calibration"
        cal_dir.mkdir(parents=True, exist_ok=True)

        with open(self.matrix_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        settings = cfg.get("settings", {})
        pipeline_template = cfg.get("pipeline_template", {})
        min_free_ram = float(settings.get("min_host_free_ram_mb", 1500))

        logger.info("==================================================================")
        logger.info("STARTING CALIBRATION RUN (Pipeline only, NO fault injection)")
        logger.info("==================================================================")

        self.cool_down_check(min_free_ram_mb=min_free_ram)

        cal_ts = int(time.time())
        cal_topic = f"eval-stream-calibration-{cal_ts}"
        p_name = "pipe-eval-calibration"
        cal_output_path = f"/data/output/{p_name}-{cal_ts}"

        # Guard: refuse to start if _spark_metadata already exists at the target path
        if check_hdfs_metadata_exists(cal_output_path):
            raise RuntimeError(
                f"CALIBRATION ABORTED: Target HDFS path {cal_output_path} already contains "
                "_spark_metadata. This run would be INVALID (Spark skips all batches). "
                "Generate a new unique path."
            )

        p_yaml = build_pipeline_yaml(
            pipeline_template,
            topic_name=cal_topic,
            pipeline_name=p_name,
            output_path=cal_output_path,
        )

        ram_start = get_free_ram_mb()
        docker_stats_start = get_docker_stats_snapshot()
        git_commit = get_git_commit_hash()
        t_start_iso = datetime.now(tz=timezone.utc).isoformat()
        t_start_mono = time.monotonic()

        # Step 1: Launch pipeline
        p_res = self.client.post("/api/pipelines/run?wait=false", json={"yaml_content": p_yaml})
        if p_res.status_code not in (200, 202):
            raise RuntimeError(f"Calibration pipeline launch failed ({p_res.status_code}): {p_res.text}")
        p_data = p_res.json()
        pipeline_run_id = p_data["pipeline_run_id"]
        logger.info("Calibration pipeline launched. Run ID: %s | Topic: %s", pipeline_run_id, cal_topic)

        # Step 2: Poll progress until terminal state while logging micro-batches and telemetry
        first_nonzero_time: float | None = None
        first_records_in: int = 0
        micro_batches: list[dict[str, Any]] = []
        telemetry_samples: list[dict[str, Any]] = []

        last_sample_t = 0.0
        last_seen_records_in = -1
        terminal_status: str | None = None
        max_cal_wait = 600.0

        while time.monotonic() - t_start_mono < max_cal_wait:
            now_mono = time.monotonic()
            elapsed = now_mono - t_start_mono

            # 15s telemetry
            if now_mono - last_sample_t >= 15.0:
                cur_ram = get_free_ram_mb()
                cur_pages = get_host_page_faults_per_sec()
                last_sample_t = now_mono
                telemetry_samples.append({
                    "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                    "elapsed_seconds": round(elapsed, 1),
                    "free_ram_mb": cur_ram,
                    "pages_per_sec": cur_pages,
                })
                logger.info("Calibration Telemetry [t+%.0fs]: Free RAM=%.1f MB | Pages/sec=%.1f", elapsed, cur_ram, cur_pages)

            time.sleep(1.0)
            prog_res = self.client.get(f"/api/pipeline-runs/{pipeline_run_id}/progress")
            if prog_res.status_code == 200:
                prog = prog_res.json()
                st = prog.get("status")
                spark_info = prog.get("spark") or {}
                recs_in = spark_info.get("recordsIn") or 0
                recs_out = spark_info.get("recordsOut") or 0
                rate = (prog.get("throughput") or {}).get("recordsPerSecond") or 0.0

                if (recs_in > 0 or recs_out > 0) and first_nonzero_time is None:
                    first_nonzero_time = now_mono
                    first_records_in = recs_in
                    logger.info("CALIBRATION MILESTONE: First non-zero counters observed at t+%.2fs (recordsIn=%d)", elapsed, recs_in)

                if recs_in != last_seen_records_in and recs_in > 0:
                    batch_record = {
                        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                        "elapsed_seconds": round(elapsed, 2),
                        "records_in": recs_in,
                        "records_out": recs_out,
                        "rate_rps": rate,
                        "consumer_lag": (prog.get("kafka") or {}).get("consumerLag"),
                    }
                    micro_batches.append(batch_record)
                    last_seen_records_in = recs_in
                    logger.info("Batch observed at t+%.1fs: recordsIn=%d, rate=%.1f rps", elapsed, recs_in, rate)

                if st in ("succeeded", "failed", "cancelled"):
                    terminal_status = st
                    break

        t_end_iso = datetime.now(tz=timezone.utc).isoformat()
        total_duration = round(time.monotonic() - t_start_mono, 2)
        duration_after_first_counters = (
            round(time.monotonic() - first_nonzero_time, 2) if first_nonzero_time else 0.0
        )
        time_to_first_batch = (
            round(first_nonzero_time - t_start_mono, 2) if first_nonzero_time else 0.0
        )

        logger.info("==================================================================")
        logger.info(
            "CALIBRATION COMPLETE: Status=%s | Total Duration=%.2fs | Active Processing=%.2fs | First Batch Delay=%.2fs",
            terminal_status,
            total_duration,
            duration_after_first_counters,
            time_to_first_batch,
        )
        logger.info("==================================================================")

        docker_stats_end = get_docker_stats_snapshot()

        calibration_data = {
            "type": "calibration",
            "git_commit": git_commit,
            "pipeline_run_id": pipeline_run_id,
            "topic_name": cal_topic,
            "started_at": t_start_iso,
            "finished_at": t_end_iso,
            "terminal_status": terminal_status,
            "total_duration_seconds": total_duration,
            "time_to_first_micro_batch_seconds": time_to_first_batch,
            "duration_after_first_counters_seconds": duration_after_first_counters,
            "host_free_ram_start_mb": ram_start,
            "host_free_ram_end_mb": get_free_ram_mb(),
            "telemetry_samples": telemetry_samples,
            "micro_batches": micro_batches,
            "docker_stats_start": docker_stats_start,
            "docker_stats_end": docker_stats_end,
        }

        with open(cal_dir / "calibration_report.json", "w", encoding="utf-8") as f:
            json.dump(calibration_data, f, indent=2)

        # Sanity gate: verify HDFS output row count matches physical expected count
        output_path = cal_output_path
        expected_physical_rows = 289744
        try:
            physical_metrics = get_hdfs_physical_metrics(output_path)
            physical_rows = physical_metrics.get("physical_rows", 0)
            unique_ids = physical_metrics.get("unique_transaction_ids", 0)
            dup_ids = physical_metrics.get("duplicate_transaction_ids", 0)
            baseline_dups = physical_metrics.get("baseline_csv_duplicates", 4730)
            excess_dups = physical_metrics.get("excess_duplicates", 0)
            physical_data_loss = max(0, expected_physical_rows - physical_rows)

            logger.info("CALIBRATION: Running multiset comparison against expected data...")
            multiset = compute_multiset_comparison(output_path)
        except MeasurementError as me:
            calibration_data["hdfs_sanity_gate"] = "MEASUREMENT_FAILED"
            calibration_data["run_valid"] = False
            calibration_data["measurement_error"] = str(me)
            with open(cal_dir / "calibration_report.json", "w", encoding="utf-8") as f:
                json.dump(calibration_data, f, indent=2)
            logger.error("CALIBRATION MEASUREMENT FAILED: %s", me)
            raise

        # Retrieve tracker numbers for auditing and logging
        tracker_rows_written = None
        tracker_records_out = None
        tracker_records_in = None
        cal_pipeline_run = self.client.get(f"/api/pipeline-runs/{pipeline_run_id}")
        if cal_pipeline_run.status_code == 200:
            progress = self.client.get(f"/api/pipeline-runs/{pipeline_run_id}/progress")
            if progress.status_code == 200:
                prog_data = progress.json()
                hdfs_info = prog_data.get("hdfs") or {}
                spark_info = prog_data.get("spark") or {}
                tracker_rows_written = hdfs_info.get("rowsWritten")
                tracker_records_out = spark_info.get("recordsOut")
                tracker_records_in = spark_info.get("recordsIn")

        calibration_data["output_path"] = output_path
        calibration_data["physical_metrics"] = {
            "expected_rows": expected_physical_rows,
            "physical_rows": physical_rows,
            "unique_transaction_ids": unique_ids,
            "duplicate_transaction_ids": dup_ids,
            "baseline_csv_duplicates": baseline_dups,
            "excess_duplicates": excess_dups,
            "physical_data_loss": physical_data_loss,
            "file_count": physical_metrics.get("file_count", 0),
        }
        calibration_data["multiset_comparison"] = multiset
        calibration_data["platform_validator_unreliable"] = {
            "label": "platform validator (unreliable, uses tracker counters)",
            "records_in": tracker_records_in,
            "records_out": tracker_records_out,
            "rows_written": tracker_rows_written,
        }
        # Backward-compat fields in calibration_data:
        calibration_data["hdfs_rows_physical"] = physical_rows
        calibration_data["hdfs_rows_expected"] = expected_physical_rows
        calibration_data["tracker_hdfs_rows_written"] = tracker_rows_written
        calibration_data["tracker_spark_records_out"] = tracker_records_out

        logger.info(
            "CALIBRATION SANITY CHECK: Physical HDFS rows=%d (expected=%d, diff/loss=%d) | Unique IDs=%d | Dups=%d (excess=%d) | "
            "Platform Tracker (unreliable): recordsIn=%s, recordsOut=%s, rowsWritten=%s",
            physical_rows,
            expected_physical_rows,
            physical_data_loss,
            unique_ids,
            dup_ids,
            excess_dups,
            tracker_records_in,
            tracker_records_out,
            tracker_rows_written,
        )

        if physical_rows == 0:
            logger.error(
                "CALIBRATION SANITY GATE FAILED: Physical HDFS output has 0 rows (INVALID: empty output). "
                "Platform Tracker (unreliable) reported hdfs.rowsWritten=%s, spark.recordsOut=%s.",
                tracker_rows_written,
                tracker_records_out,
            )
            calibration_data["hdfs_sanity_gate"] = "INVALID: empty output"
            calibration_data["run_valid"] = False
            with open(cal_dir / "calibration_report.json", "w", encoding="utf-8") as f:
                json.dump(calibration_data, f, indent=2)
            raise RuntimeError(
                "Calibration SANITY GATE FAILED: Physical HDFS row count is 0 (INVALID: empty output)."
            )
        elif physical_rows != expected_physical_rows or multiset.get("missing_rows", 0) > 0 or multiset.get("duplicate_beyond_baseline", 0) > 0:
            logger.error(
                "CALIBRATION SANITY GATE FAILED: Physical HDFS rows=%d (expected %d), missing=%s, excess_dups=%s. "
                "Platform Tracker (unreliable) reported hdfs.rowsWritten=%s, spark.recordsOut=%s.",
                physical_rows,
                expected_physical_rows,
                multiset.get("missing_rows"),
                multiset.get("duplicate_beyond_baseline"),
                tracker_rows_written,
                tracker_records_out,
            )
            calibration_data["hdfs_sanity_gate"] = "FAILED"
            calibration_data["run_valid"] = False
            with open(cal_dir / "calibration_report.json", "w", encoding="utf-8") as f:
                json.dump(calibration_data, f, indent=2)
            raise RuntimeError(
                f"Calibration SANITY GATE FAILED: Physical HDFS row count is {physical_rows} "
                f"(expected {expected_physical_rows}), missing={multiset.get('missing_rows')}, "
                f"excess_dups={multiset.get('duplicate_beyond_baseline')}."
            )

        calibration_data["hdfs_sanity_gate"] = "PASSED"
        calibration_data["run_valid"] = True
        logger.info(
            "CALIBRATION SANITY GATE PASSED: Physical HDFS rows=%d matches expected %d, 0 missing, 0 excess duplicates",
            physical_rows,
            expected_physical_rows,
        )
        logger.info(
            "CALIBRATION MULTISET: missing=%s extra=%s dup_beyond_baseline=%s error=%s",
            multiset.get("missing_rows"),
            multiset.get("extra_rows"),
            multiset.get("duplicate_beyond_baseline"),
            multiset.get("error"),
        )
        with open(cal_dir / "calibration_report.json", "w", encoding="utf-8") as f:
            json.dump(calibration_data, f, indent=2)

        self.last_topic_name = cal_topic
        return calibration_data

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
            experiments = [experiments[0]]

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
    parser.add_argument("--calibrate", action="store_true", help="Run calibration pipeline only (no fault injection)")
    parser.add_argument("--repetitions", type=int, default=None, help="Override repetition count")
    parser.add_argument("--base-url", default="http://localhost:8000", help="Platform API base URL")
    args = parser.parse_args()

    harness = EvaluationHarness(
        base_url=args.base_url,
        matrix_path=args.matrix,
    )

    if args.calibrate:
        harness.run_calibration()
    else:
        harness.execute_matrix(pilot_only=args.pilot, repetitions_override=args.repetitions)


if __name__ == "__main__":
    main()
