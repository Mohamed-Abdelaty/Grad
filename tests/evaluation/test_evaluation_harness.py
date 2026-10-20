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
    count_hdfs_physical_rows,
    get_hdfs_physical_metrics,
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
    assert pt.get("max_offsets_per_trigger") in (3000, 10000)

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

    # 8. Host page faults above threshold
    passed, reason = verify_cool_down_status(
        health_ok=True,
        active_runs=0,
        running_exps=0,
        cores_used=0,
        consumer_lag=0,
        free_ram_mb=2000.0,
        min_free_ram_mb=1500.0,
        pages_sec_30s_avg=140.0,
        max_pages_sec=100.0,
    )
    assert passed is False
    assert "page faults" in reason


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
                "spark": {"recordsIn": 500, "recordsOut": 500},
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



def test_count_hdfs_physical_rows_unit(monkeypatch):
    """Verify count_hdfs_physical_rows command invocation and fallback semantics."""
    import subprocess

    captured_cmds = []

    def mock_run(cmd, *args, **kwargs):
        captured_cmds.append(cmd)
        class MockRes:
            stdout = "289744\n"
            returncode = 0
        return MockRes()

    monkeypatch.setattr(subprocess, "run", mock_run)

    # Test full HDFS URI stripping
    rows = count_hdfs_physical_rows("hdfs://namenode:9000/data/output/pipe-eval-calibration")
    assert rows == 289744
    assert captured_cmds[0][-1] == "/data/output/pipe-eval-calibration"

    # Test failure raises MeasurementError
    def mock_run_fail(cmd, *args, **kwargs):
        raise RuntimeError("docker failed")

    monkeypatch.setattr(subprocess, "run", mock_run_fail)
    import pytest
    from run_evaluation import MeasurementError
    with pytest.raises(MeasurementError):
        count_hdfs_physical_rows("/data/output/empty")


def test_build_pipeline_yaml_custom_output_path():
    """Verify build_pipeline_yaml respects explicitly passed unique output path."""
    template = {
        "source_path": "/app/sample-data/sales_300k.csv",
        "max_offsets_per_trigger": 3000,
    }
    raw_yaml = build_pipeline_yaml(
        template=template,
        topic_name="eval-test-topic",
        pipeline_name="pipe-eval-test",
        output_path="/data/output/pipe-eval-test-1771618000",
    )
    parsed = yaml.safe_load(raw_yaml)
    assert parsed["storage"]["path"] == "/data/output/pipe-eval-test-1771618000"


def test_get_hdfs_physical_metrics_unit(monkeypatch):
    """Verify get_hdfs_physical_metrics parsing, duplicate detection, and fallback."""
    import subprocess
    import json

    def mock_run(cmd, *args, **kwargs):
        class MockRes:
            stdout = json.dumps({
                "physical_rows": 289744,
                "unique_transaction_ids": 285014,
                "duplicate_transaction_ids": 4730,
                "baseline_csv_duplicates": 4730,
                "excess_duplicates": 0,
                "file_count": 98,
            }) + "\n"
            returncode = 0
        return MockRes()

    monkeypatch.setattr(subprocess, "run", mock_run)
    metrics = get_hdfs_physical_metrics("hdfs://namenode:9000/data/output/pipe-eval-custom")
    assert metrics["physical_rows"] == 289744
    assert metrics["unique_transaction_ids"] == 285014
    assert metrics["duplicate_transaction_ids"] == 4730
    assert metrics["baseline_csv_duplicates"] == 4730
    assert metrics["excess_duplicates"] == 0
    assert metrics["file_count"] == 98
    assert metrics["raw_path"] == "/data/output/pipe-eval-custom"

    # Test failure raises MeasurementError
    def mock_run_fail(cmd, *args, **kwargs):
        raise RuntimeError("docker exec failed")

    monkeypatch.setattr(subprocess, "run", mock_run_fail)
    import pytest
    from run_evaluation import MeasurementError
    with pytest.raises(MeasurementError):
        get_hdfs_physical_metrics("/data/output/nonexistent")


def test_check_hdfs_metadata_exists_unit(monkeypatch):
    """Verify check_hdfs_metadata_exists detects stale _spark_metadata via hdfs dfs -test."""
    import subprocess
    from run_evaluation import check_hdfs_metadata_exists

    # Simulate: path HAS _spark_metadata (returncode 0 means exists)
    def mock_run_exists(cmd, *args, **kwargs):
        class MockRes:
            returncode = 0
        return MockRes()

    monkeypatch.setattr(subprocess, "run", mock_run_exists)
    assert check_hdfs_metadata_exists("/data/output/pipe-test-123") is True

    # Simulate: path does NOT have _spark_metadata (returncode 1 means not exists)
    def mock_run_not_exists(cmd, *args, **kwargs):
        class MockRes:
            returncode = 1
        return MockRes()

    monkeypatch.setattr(subprocess, "run", mock_run_not_exists)
    assert check_hdfs_metadata_exists("/data/output/pipe-test-new") is False

    # Simulate: docker exec fails (timeout etc.) -> treat as not-exists (conservative)
    def mock_run_fail(cmd, *args, **kwargs):
        raise RuntimeError("docker exec failed")

    monkeypatch.setattr(subprocess, "run", mock_run_fail)
    assert check_hdfs_metadata_exists("/data/output/pipe-test-fail") is False


def test_compute_multiset_comparison_unit(monkeypatch):
    """Verify compute_multiset_comparison parses docker exec output and handles errors."""
    import subprocess
    import json
    from run_evaluation import compute_multiset_comparison

    # Perfect match: 0 missing, 0 extra
    perfect = {"missing_rows": 0, "extra_rows": 0, "duplicate_beyond_baseline": 0,
               "expected_rows": 289744, "physical_rows": 289744}

    def mock_run_perfect(cmd, *args, **kwargs):
        class MockRes:
            stdout = json.dumps(perfect) + "\n"
            returncode = 0
        return MockRes()

    monkeypatch.setattr(subprocess, "run", mock_run_perfect)
    result = compute_multiset_comparison("hdfs://namenode:9000/data/output/pipe-cal")
    assert result["missing_rows"] == 0
    assert result["extra_rows"] == 0
    assert result["duplicate_beyond_baseline"] == 0
    assert result["physical_rows"] == 289744
    assert result["hdfs_path"] == "/data/output/pipe-cal"

    # Data loss scenario: 100 missing rows
    lossy = {"missing_rows": 100, "extra_rows": 0, "duplicate_beyond_baseline": 0,
             "expected_rows": 289744, "physical_rows": 289644}

    def mock_run_lossy(cmd, *args, **kwargs):
        class MockRes:
            stdout = json.dumps(lossy) + "\n"
            returncode = 0
        return MockRes()

    monkeypatch.setattr(subprocess, "run", mock_run_lossy)
    result2 = compute_multiset_comparison("/data/output/pipe-fault")
    assert result2["missing_rows"] == 100

    # Fault run with replay (extra rows are OK, not a harness failure)
    replay = {"missing_rows": 0, "extra_rows": 250, "duplicate_beyond_baseline": 250,
              "expected_rows": 289744, "physical_rows": 289994}

    def mock_run_replay(cmd, *args, **kwargs):
        class MockRes:
            stdout = json.dumps(replay) + "\n"
            returncode = 0
        return MockRes()

    monkeypatch.setattr(subprocess, "run", mock_run_replay)
    result3 = compute_multiset_comparison("/data/output/pipe-fault2")
    assert result3["missing_rows"] == 0  # no data loss
    assert result3["extra_rows"] == 250  # legitimately from at-least-once replay

    # Failure fallback: must raise MeasurementError
    def mock_run_fail(cmd, *args, **kwargs):
        raise RuntimeError("docker exec failed")

    monkeypatch.setattr(subprocess, "run", mock_run_fail)
    import pytest
    from run_evaluation import MeasurementError
    with pytest.raises(MeasurementError):
        compute_multiset_comparison("/data/output/pipe-err")


def test_get_hdfs_physical_metrics_success(monkeypatch):
    """Verify get_hdfs_physical_metrics returns metrics on exit code 0."""
    import subprocess
    import json
    from run_evaluation import get_hdfs_physical_metrics

    data = {
        "physical_rows": 289744,
        "unique_transaction_ids": 285014,
        "duplicate_transaction_ids": 4730,
        "baseline_csv_duplicates": 4730,
        "excess_duplicates": 0,
        "file_count": 304,
    }

    def mock_run(cmd, *args, **kwargs):
        class MockRes:
            stdout = json.dumps(data) + "\n"
            stderr = ""
            returncode = 0
        return MockRes()

    monkeypatch.setattr(subprocess, "run", mock_run)
    res = get_hdfs_physical_metrics("/data/output/pipe-test")
    assert res["physical_rows"] == 289744
    assert res["unique_transaction_ids"] == 285014


def test_get_hdfs_physical_metrics_exit_134_with_json(monkeypatch):
    """Verify get_hdfs_physical_metrics accepts valid JSON even if exit code is 134 (PyArrow teardown)."""
    import subprocess
    import json
    from run_evaluation import get_hdfs_physical_metrics

    data = {
        "physical_rows": 289744,
        "unique_transaction_ids": 285014,
        "duplicate_transaction_ids": 4730,
        "baseline_csv_duplicates": 4730,
        "excess_duplicates": 0,
        "file_count": 304,
    }

    def mock_run(cmd, *args, **kwargs):
        class MockRes:
            stdout = "some log\n" + json.dumps(data) + "\nterminate called without an active exception\n"
            stderr = ""
            returncode = 134  # SIGABRT
        return MockRes()

    monkeypatch.setattr(subprocess, "run", mock_run)
    res = get_hdfs_physical_metrics("/data/output/pipe-test")
    assert res["physical_rows"] == 289744
    assert res["unique_transaction_ids"] == 285014


def test_get_hdfs_physical_metrics_no_json_raises_measurement_error(monkeypatch):
    """Verify get_hdfs_physical_metrics raises MeasurementError when no valid JSON is produced."""
    import subprocess
    import pytest
    from run_evaluation import get_hdfs_physical_metrics, MeasurementError

    def mock_run(cmd, *args, **kwargs):
        class MockRes:
            stdout = "Traceback (most recent call last):\n..."
            stderr = "Error connecting to namenode"
            returncode = 1
        return MockRes()

    monkeypatch.setattr(subprocess, "run", mock_run)
    with pytest.raises(MeasurementError) as exc_info:
        get_hdfs_physical_metrics("/data/output/pipe-fail")
    assert "Failed to count HDFS physical metrics" in str(exc_info.value)


def test_compute_multiset_comparison_exit_134_with_json(monkeypatch):
    """Verify compute_multiset_comparison accepts valid JSON even if exit code is 134."""
    import subprocess
    import json
    from run_evaluation import compute_multiset_comparison

    perfect = {"missing_rows": 0, "extra_rows": 0, "duplicate_beyond_baseline": 0,
               "expected_rows": 289744, "physical_rows": 289744}

    def mock_run(cmd, *args, **kwargs):
        class MockRes:
            stdout = json.dumps(perfect) + "\nterminate called without an active exception\n"
            stderr = ""
            returncode = 134
        return MockRes()

    monkeypatch.setattr(subprocess, "run", mock_run)
    res = compute_multiset_comparison("/data/output/pipe-test")
    assert res["missing_rows"] == 0
    assert res["physical_rows"] == 289744


def test_compute_multiset_comparison_no_json_raises_measurement_error(monkeypatch):
    """Verify compute_multiset_comparison raises MeasurementError when no valid JSON is produced."""
    import subprocess
    import pytest
    from run_evaluation import compute_multiset_comparison, MeasurementError

    def mock_run(cmd, *args, **kwargs):
        class MockRes:
            stdout = "SIGSEGV"
            stderr = "Fatal error"
            returncode = 139
        return MockRes()

    monkeypatch.setattr(subprocess, "run", mock_run)
    with pytest.raises(MeasurementError) as exc_info:
        compute_multiset_comparison("/data/output/pipe-fail")
    assert "compute_multiset_comparison failed" in str(exc_info.value)


def test_run_classification_rules():
    """Verify run classifications: VALID vs INVALID (empty output) vs MEASUREMENT_FAILED."""
    # 1. Real zero rows -> INVALID: empty output
    phys_zero = 0
    multiset_zero = {"missing_rows": 289744, "extra_rows": 0, "duplicate_beyond_baseline": 0}
    measurement_error = None
    if measurement_error:
        run_valid = False
        classification = "MEASUREMENT_FAILED"
    elif phys_zero == 0:
        run_valid = False
        classification = "INVALID: empty output"
    else:
        run_valid = phys_zero > 0 and multiset_zero.get("missing_rows", -1) >= 0
        classification = "VALID" if run_valid else "INVALID"
    assert run_valid is False
    assert classification == "INVALID: empty output"

    # 2. Measurement error -> MEASUREMENT_FAILED (distinct from empty output)
    phys_err = -1
    measurement_error = "Connection refused to namenode"
    if measurement_error:
        run_valid = False
        classification = "MEASUREMENT_FAILED"
    elif phys_err == 0:
        run_valid = False
        classification = "INVALID: empty output"
    else:
        run_valid = phys_err > 0
        classification = "VALID" if run_valid else "INVALID"
    assert run_valid is False
    assert classification == "MEASUREMENT_FAILED"

    # 3. Correct rows -> VALID
    phys_ok = 289744
    multiset_ok = {"missing_rows": 0, "extra_rows": 0, "duplicate_beyond_baseline": 0}
    measurement_error = None
    if measurement_error:
        run_valid = False
        classification = "MEASUREMENT_FAILED"
    elif phys_ok == 0:
        run_valid = False
        classification = "INVALID: empty output"
    else:
        rep_run_valid = (
            phys_ok > 0
            and multiset_ok.get("missing_rows", -1) >= 0
            and not multiset_ok.get("error")
        )
        classification = "VALID" if rep_run_valid else "INVALID"
    assert rep_run_valid is True
    assert classification == "VALID"


def test_get_hdfs_physical_metrics_spark_metadata_filter(monkeypatch):
    """Verify get_hdfs_physical_metrics py_cmd inspects _spark_metadata to exclude uncommitted stubs."""
    import subprocess
    import json
    from run_evaluation import get_hdfs_physical_metrics

    captured_cmds = []

    def mock_run(cmd, *args, **kwargs):
        captured_cmds.append(cmd)
        class MockRes:
            stdout = json.dumps({
                "physical_rows": 40523,
                "unique_transaction_ids": 39855,
                "duplicate_transaction_ids": 668,
                "baseline_csv_duplicates": 4730,
                "excess_duplicates": 0,
                "file_count": 42,
            }) + "\n"
            stderr = ""
            returncode = 0
        return MockRes()

    monkeypatch.setattr(subprocess, "run", mock_run)
    res = get_hdfs_physical_metrics("/data/output/pipe-eval-kafka-stop-r1-1792533072")
    assert res["physical_rows"] == 40523
    assert res["file_count"] == 42
    # Verify the executed command passed to container inspects _spark_metadata
    assert len(captured_cmds) == 1
    py_code = captured_cmds[0][5]  # python -c <code> <path>
    assert "_spark_metadata" in py_code
    assert "posixpath.basename" in py_code


def test_get_kafka_topic_total_records_unit(monkeypatch):
    """Verify get_kafka_topic_total_records parses GetOffsetShell output or returns 0 on non-zero returncode."""
    import subprocess
    from run_evaluation import get_kafka_topic_total_records

    # Case 1: Topic with records
    def mock_run_with_records(cmd, *args, **kwargs):
        class MockRes:
            returncode = 0
            stdout = "my-topic:0:100\nmy-topic:1:250\nmy-topic:2:150\n"
            stderr = ""
        return MockRes()

    monkeypatch.setattr(subprocess, "run", mock_run_with_records)
    assert get_kafka_topic_total_records("my-topic") == 500

    # Case 2: Non-existent topic (returncode != 0)
    def mock_run_nonexistent(cmd, *args, **kwargs):
        class MockRes:
            returncode = 1
            stdout = ""
            stderr = "Error: topic does not exist"
        return MockRes()

    monkeypatch.setattr(subprocess, "run", mock_run_nonexistent)
    assert get_kafka_topic_total_records("no-such-topic") == 0

    # Case 3: Empty topic (0 offsets)
    def mock_run_empty(cmd, *args, **kwargs):
        class MockRes:
            returncode = 0
            stdout = "empty-topic:0:0\nempty-topic:1:0\n"
            stderr = ""
        return MockRes()

    monkeypatch.setattr(subprocess, "run", mock_run_empty)
    assert get_kafka_topic_total_records("empty-topic") == 0


def test_unique_topic_name_and_reuse_guard(monkeypatch):
    """Verify topic names have timestamp suffix and run refuses non-empty topics."""
    import re
    import pytest
    from pathlib import Path
    from run_evaluation import EvaluationHarness

    orch = EvaluationHarness(
        base_url="http://localhost:8000",
    )

    # Mock get_kafka_topic_total_records returning 500 records -> must abort
    monkeypatch.setattr("run_evaluation.get_kafka_topic_total_records", lambda topic: 500)
    monkeypatch.setattr("run_evaluation.check_hdfs_metadata_exists", lambda path: False)
    monkeypatch.setattr("run_evaluation.get_free_ram_mb", lambda: 4000.0)
    monkeypatch.setattr("run_evaluation.get_docker_stats_snapshot", lambda: {})
    monkeypatch.setattr("run_evaluation.get_kafka_metrics", lambda: {})
    monkeypatch.setattr("run_evaluation.get_spark_master_metrics", lambda: {})

    with pytest.raises(RuntimeError) as exc_info:
        orch.run_repetition(
            exp_id="test-exp",
            target_service="kafka",
            fault_type="container_stop",
            rep_idx=1,
            settings={},
            pipeline_template={"source_path": "/app/sample-data/sales_300k.csv"},
        )
    assert "RUN ABORTED: Kafka topic" in str(exc_info.value)
    assert "already contains 500 records" in str(exc_info.value)
