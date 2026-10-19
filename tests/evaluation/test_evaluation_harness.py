"""
tests/evaluation/test_evaluation_harness.py
------------------------------------------
Unit and contract tests for evaluation harness and deterministic aggregator:
- matrix.yaml schema and structure validation
- build_pipeline_yaml generator output and parameter isolation
- build_experiment_yaml generator output and parameter validation
- verify_cool_down_status decision logic (all passing, health failure, active runs, cores busy, lag, RAM below threshold)
- Statistical aggregation: N=1, N=3 hand-calculated values, None exclusion, all-None, identical values give std 0
- Deterministic CSV writing (running twice gives identical bytes)
"""

import sys
from pathlib import Path
import pytest
import yaml

# Add scripts/evaluation to sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "evaluation"))

from run_evaluation import (
    build_pipeline_yaml,
    build_experiment_yaml,
    get_free_ram_mb,
    verify_cool_down_status,
)
from aggregate import (
    compute_sample_stats,
    write_aggregated_csv,
    format_val,
)


def test_matrix_yaml_validity():
    """Verify scripts/evaluation/matrix.yaml is well-formed and meets evaluation constraints."""
    matrix_path = REPO_ROOT / "scripts" / "evaluation" / "matrix.yaml"
    assert matrix_path.exists(), f"matrix.yaml missing at {matrix_path}"

    with open(matrix_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    assert "settings" in cfg
    settings = cfg["settings"]
    assert settings.get("baseline_seconds") == 10
    assert settings.get("fault_duration_seconds") == 20
    assert settings.get("repetitions_per_experiment") == 5
    assert settings.get("min_host_free_ram_mb") == 1500

    assert "pipeline_template" in cfg
    pt = cfg["pipeline_template"]
    assert "source_path" in pt
    assert pt.get("max_offsets_per_trigger") == 10000

    assert "experiments" in cfg
    exps = cfg["experiments"]
    assert len(exps) == 5

    allowed_services = {"kafka", "spark-worker", "datanode", "namenode", "spark-master"}
    allowed_faults = {"container_stop", "container_restart", "network_partition", "service_delay"}

    exp_ids = set()
    for e in exps:
        assert "id" in e
        assert e["id"] not in exp_ids, f"Duplicate experiment id: {e['id']}"
        exp_ids.add(e["id"])
        assert e["target_service"] in allowed_services
        assert e["fault_type"] in allowed_faults
        if e["target_service"] == "kafka":
            assert e["fault_type"] != "service_delay", "service_delay cannot run on kafka (missing tc)"


def test_build_pipeline_yaml():
    """Verify generated streaming pipeline YAML matches executor requirements."""
    template = {
        "source_path": "/app/sample-data/sales_300k.csv",
        "max_offsets_per_trigger": 10000,
        "trigger_interval_seconds": 2,
        "idle_timeout_seconds": 20,
        "max_runtime_seconds": 600,
    }
    raw_yaml = build_pipeline_yaml(
        template=template,
        topic_name="eval-stream-kafka-stop-r1",
        pipeline_name="pipe-eval-kafka-stop-r1",
    )

    parsed = yaml.safe_load(raw_yaml)
    assert parsed["pipeline"]["name"] == "pipe-eval-kafka-stop-r1"
    assert parsed["source"]["type"] == "csv"
    assert parsed["messaging"]["topic"] == "eval-stream-kafka-stop-r1"
    assert parsed["messaging"]["broker"] == "kafka"
    assert parsed["streaming"]["enabled"] is True
    assert parsed["streaming"]["starting_offsets"] == "earliest"
    assert parsed["streaming"]["max_offsets_per_trigger"] == 10000
    assert parsed["storage"]["type"] == "hdfs"
    assert parsed["storage"]["path"] == "/data/output/pipe-eval-kafka-stop-r1"


def test_build_experiment_yaml():
    """Verify generated experiment YAML configures isolated single repetition with baseline."""
    raw_yaml = build_experiment_yaml(
        experiment_name="eval-kafka-stop-r1",
        target_service="kafka",
        fault_type="container_stop",
        duration=20,
        baseline_seconds=10,
        max_recovery_time=60.0,
        expected_data_loss=0,
    )

    parsed = yaml.safe_load(raw_yaml)
    assert parsed["experiment"]["name"] == "eval-kafka-stop-r1"
    assert parsed["target"]["service"] == "kafka"
    assert parsed["fault"]["type"] == "container_stop"
    assert parsed["fault"]["duration"] == 20
    assert parsed["validation"]["max_recovery_time"] == 60.0
    assert parsed["validation"]["expected_data_loss"] == 0
    assert parsed["repetitions"] == 1
    assert parsed["baseline_seconds"] == 10


def test_get_free_ram_mb_positive():
    """Verify free RAM measurement function returns a valid positive number."""
    ram = get_free_ram_mb()
    assert isinstance(ram, float)
    assert ram > 0.0


def test_cool_down_decision_logic():
    """Verify verify_cool_down_status decision logic for all expected failure modes."""
    # 1. All pass
    passed, reason = verify_cool_down_status(
        health_ok=True,
        active_runs=0,
        running_exps=0,
        cores_used=0,
        consumer_lag=0,
        free_ram_mb=2000.0,
        min_free_ram_mb=1500.0,
    )
    assert passed is True
    assert "passed" in reason

    # 2. Health check failed
    passed, reason = verify_cool_down_status(
        health_ok=False,
        active_runs=0,
        running_exps=0,
        cores_used=0,
        consumer_lag=0,
        free_ram_mb=2000.0,
        min_free_ram_mb=1500.0,
    )
    assert passed is False
    assert "health" in reason

    # 3. Active runs remaining
    passed, reason = verify_cool_down_status(
        health_ok=True,
        active_runs=2,
        running_exps=0,
        cores_used=0,
        consumer_lag=0,
        free_ram_mb=2000.0,
        min_free_ram_mb=1500.0,
    )
    assert passed is False
    assert "Active pipeline runs" in reason

    # 4. Running experiments remaining
    passed, reason = verify_cool_down_status(
        health_ok=True,
        active_runs=0,
        running_exps=1,
        cores_used=0,
        consumer_lag=0,
        free_ram_mb=2000.0,
        min_free_ram_mb=1500.0,
    )
    assert passed is False
    assert "Running experiment runs" in reason

    # 5. Spark worker cores busy
    passed, reason = verify_cool_down_status(
        health_ok=True,
        active_runs=0,
        running_exps=0,
        cores_used=2,
        consumer_lag=0,
        free_ram_mb=2000.0,
        min_free_ram_mb=1500.0,
    )
    assert passed is False
    assert "cores busy" in reason

    # 6. Consumer lag
    passed, reason = verify_cool_down_status(
        health_ok=True,
        active_runs=0,
        running_exps=0,
        cores_used=0,
        consumer_lag=45,
        free_ram_mb=2000.0,
        min_free_ram_mb=1500.0,
    )
    assert passed is False
    assert "Consumer lag" in reason

    # 7. Host RAM below threshold
    passed, reason = verify_cool_down_status(
        health_ok=True,
        active_runs=0,
        running_exps=0,
        cores_used=0,
        consumer_lag=0,
        free_ram_mb=1200.0,
        min_free_ram_mb=1500.0,
    )
    assert passed is False
    assert "below required threshold" in reason


def test_aggregate_stats_computations():
    """Verify sample statistics computation against exact hand-calculated values."""
    # Case 1: N = 1 -> mean=val, std=0.0
    s1 = compute_sample_stats([42.5])
    assert s1["n"] == 1
    assert s1["mean"] == 42.5
    assert s1["std"] == 0.0
    assert s1["min"] == 42.5
    assert s1["max"] == 42.5

    # Case 2: N = 3 hand-calculated: [10.0, 20.0, 30.0]
    # mean = 20.0
    # var = ((10-20)^2 + (20-20)^2 + (30-20)^2) / 2 = (100 + 0 + 100) / 2 = 100.0
    # std = sqrt(100.0) = 10.0
    s3 = compute_sample_stats([10.0, 20.0, 30.0])
    assert s3["n"] == 3
    assert s3["mean"] == 20.0
    assert s3["std"] == 10.0
    assert s3["min"] == 10.0
    assert s3["max"] == 30.0

    # Case 3: None exclusion: [10.0, None, 20.0, None, 30.0] -> same as Case 2
    s_none = compute_sample_stats([10.0, None, 20.0, None, 30.0])
    assert s_none["n"] == 3
    assert s_none["mean"] == 20.0
    assert s_none["std"] == 10.0

    # Case 4: All None -> n=0, all fields None
    s_all_none = compute_sample_stats([None, None])
    assert s_all_none["n"] == 0
    assert s_all_none["mean"] is None
    assert s_all_none["std"] is None
    assert s_all_none["min"] is None
    assert s_all_none["max"] is None

    # Case 5: Identical values -> std must be exactly 0.0
    s_ident = compute_sample_stats([15.0, 15.0, 15.0, 15.0])
    assert s_ident["n"] == 4
    assert s_ident["mean"] == 15.0
    assert s_ident["std"] == 0.0


def test_deterministic_csv_output(tmp_path):
    """Verify running write_aggregated_csv twice produces bit-for-bit identical bytes."""
    rows = [
        {
            "scenario": "eval-kafka-stop",
            "metric": "recovery_time_seconds",
            "repetitions_total": 5,
            "n_measured": 5,
            "null_count": 0,
            "mean": 12.34567,
            "std": 1.23456,
            "min": 10.1234,
            "max": 14.5678,
        },
        {
            "scenario": "eval-kafka-stop",
            "metric": "data_loss_records",
            "repetitions_total": 5,
            "n_measured": 5,
            "null_count": 0,
            "mean": 0.0,
            "std": 0.0,
            "min": 0.0,
            "max": 0.0,
        },
    ]

    p1 = tmp_path / "out1.csv"
    p2 = tmp_path / "out2.csv"

    write_aggregated_csv(rows, p1)
    write_aggregated_csv(rows, p2)

    bytes1 = p1.read_bytes()
    bytes2 = p2.read_bytes()

    assert bytes1 == bytes2
    assert b"12.3457" in bytes1  # Rounded to 4 decimal places


def test_harness_pipeline_first_orchestration_contract(monkeypatch, tmp_path):
    """Verify evaluation harness launches pipeline first, waits for progress, then triggers experiment."""
    from run_evaluation import EvaluationHarness
    from unittest.mock import MagicMock

    call_order = []

    mock_client = MagicMock()

    # Mock POST /api/pipelines/run?wait=false
    def mock_post(url, *args, **kwargs):
        resp = MagicMock()
        if "api/pipelines/run" in url:
            call_order.append(("pipeline_launch", kwargs.get("json", {})))
            resp.status_code = 202
            resp.json.return_value = {"pipeline_run_id": "test-p-run-123", "status": "running"}
        elif url == "/api/experiments":
            call_order.append(("experiment_create", kwargs.get("json", {})))
            resp.status_code = 201
            resp.json.return_value = {"experiment_id": "test-exp-uuid-456"}
        elif "/api/experiments/test-exp-uuid-456/run" in url:
            call_order.append(("experiment_run", kwargs.get("json", {})))
            resp.status_code = 202
            resp.json.return_value = {"status": "pending"}
        return resp

    def mock_get(url, *args, **kwargs):
        resp = MagicMock()
        if "progress" in url:
            call_order.append(("progress_poll", url))
            resp.status_code = 200
            resp.json.return_value = {
                "runId": "test-p-run-123",
                "kafka": {"published": 500},
                "source": {"rowsRead": 500},
            }
        elif "/api/experiments/test-exp-uuid-456" in url:
            resp.status_code = 200
            resp.json.return_value = {"latest_run_status": "succeeded"}
        elif "/api/reports/test-exp-uuid-456" in url:
            resp.status_code = 200
            resp.json.return_value = {"experiment_id": "test-exp-uuid-456", "comparison_table": []}
        return resp

    mock_client.post = mock_post
    mock_client.get = mock_get

    harness = EvaluationHarness(raw_output_dir=str(tmp_path / "raw"))
    harness.client = mock_client

    monkeypatch.setattr("run_evaluation.get_free_ram_mb", lambda: 3000.0)
    monkeypatch.setattr("run_evaluation.get_docker_stats_snapshot", lambda: [])
    monkeypatch.setattr("run_evaluation.get_kafka_metrics", lambda: {})
    monkeypatch.setattr("run_evaluation.get_spark_master_metrics", lambda: {})

    res = harness.run_repetition(
        exp_id="kafka-stop",
        target_service="kafka",
        fault_type="container_stop",
        rep_idx=1,
        settings={"baseline_seconds": 10, "fault_duration_seconds": 20},
        pipeline_template={"source_path": "/test/data.csv"},
    )

    # Verify execution order:
    # 1. pipeline_launch
    # 2. progress_poll
    # 3. experiment_create
    # 4. experiment_run with pipeline_run_id
    actions = [c[0] for c in call_order]
    assert actions[0] == "pipeline_launch"
    assert "progress_poll" in actions
    assert actions.index("progress_poll") < actions.index("experiment_create")
    assert actions.index("experiment_create") < actions.index("experiment_run")

    # Check payload of experiment_run has pipeline_run_id
    exp_run_call = next(c for c in call_order if c[0] == "experiment_run")
    assert exp_run_call[1].get("pipeline_run_id") == "test-p-run-123"

