"""
backend/tests/evaluation/test_evaluation_harness.py
--------------------------------------------------
Unit and integration contract tests for the evaluation harness:
- matrix.yaml schema and structure validation
- build_pipeline_yaml generator output and parameter isolation
- build_experiment_yaml generator output and parameter validation
- Metric aggregation logic across repetitions
"""

import sys
from pathlib import Path
import pytest
import yaml

# Add scripts/evaluation to sys.path so we can import harness functions directly
REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "evaluation"))

from run_evaluation import (
    build_pipeline_yaml,
    build_experiment_yaml,
    get_free_ram_mb,
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
        # Crucial constraint: Kafka does not have tc, verify service_delay is not configured on kafka
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
