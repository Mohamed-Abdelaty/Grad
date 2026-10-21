"""Unit tests for evaluation aggregation and Gap 3 table generation."""

import csv
import json
import os
import tempfile
import pytest

from scripts.evaluation.aggregate import (
    compute_stats,
    load_run_metrics,
    load_calibration_metrics,
    load_control_campaign_metrics,
    aggregate_campaign,
    build_gap3_rows,
    build_gap3_primary_rows,
    export_gap3_csv,
    export_gap3_markdown,
)


def test_compute_stats_empty():
    res = compute_stats([])
    assert res == {"mean": 0.0, "std": 0.0, "min": 0.0, "max": 0.0}


def test_compute_stats_single():
    res = compute_stats([10.5])
    assert res == {"mean": 10.5, "std": 0.0, "min": 10.5, "max": 10.5}


def test_compute_stats_multiple():
    res = compute_stats([10.0, 20.0, 30.0])
    assert res["mean"] == 20.0
    assert res["std"] == 10.0
    assert res["min"] == 10.0
    assert res["max"] == 30.0


def test_load_run_metrics_mock():
    with tempfile.TemporaryDirectory() as tmpdir:
        report_data = {
            "recovery_time_seconds": 12.5,
            "throughputDuringFault": 135.0,
            "throughputAfter": 1150.0,
            "status": "passed",
        }
        phys_data = {
            "physical_rows": 289744,
            "excess_duplicates": 0,
            "multiset_comparison": {
                "missing_rows": 0,
                "extra_rows": 0,
            },
            "platform_validator": {
                "data_loss": 10256,
            },
        }
        env_data = {
            "host_memory_pressure": True,
            "terminal_status": "passed",
        }

        with open(os.path.join(tmpdir, "report.json"), "w", encoding="utf-8") as f:
            json.dump(report_data, f)
        with open(os.path.join(tmpdir, "physical_metrics.json"), "w", encoding="utf-8") as f:
            json.dump(phys_data, f)
        with open(os.path.join(tmpdir, "environment.json"), "w", encoding="utf-8") as f:
            json.dump(env_data, f)

        metrics = load_run_metrics(tmpdir)
        assert metrics["physical_rows"] == 289744
        assert metrics["missing_rows"] == 0
        assert metrics["extra_rows"] == 0
        assert metrics["excess_duplicates"] == 0
        assert metrics["recovery_time_seconds"] == 12.5
        assert metrics["throughput_during_fault"] == 135.0
        assert metrics["throughput_post_recovery"] == 1150.0
        assert metrics["platform_data_loss"] == 10256
        assert metrics["host_memory_pressure"] is True


def test_load_calibration_metrics_mock():
    with tempfile.TemporaryDirectory() as tmpdir:
        cal_path = os.path.join(tmpdir, "calibration_report.json")
        with open(cal_path, "w", encoding="utf-8") as f:
            json.dump({
                "duration_after_first_counters_seconds": 204.77,
                "physical_metrics": {"physical_rows": 289744},
            }, f)
        cal = load_calibration_metrics(cal_path)
        assert cal["throughput_normal"] == 1472.0
        assert cal["physical_rows"] == 289744


def test_aggregate_campaign_and_build_gap3():
    with tempfile.TemporaryDirectory() as tmpdir:
        cal_path = os.path.join(tmpdir, "calibration.json")
        with open(cal_path, "w", encoding="utf-8") as f:
            json.dump({"average_throughput": 1472.0}, f)

        # Create one mock experiment
        exp_dir = os.path.join(tmpdir, "exp_rep1")
        os.makedirs(exp_dir, exist_ok=True)
        with open(os.path.join(exp_dir, "report.json"), "w", encoding="utf-8") as f:
            json.dump({"recovery_time_seconds": 15.0, "throughputDuringFault": 100.0, "throughputAfter": 1200.0}, f)
        with open(os.path.join(exp_dir, "physical_metrics.json"), "w", encoding="utf-8") as f:
            json.dump({
                "physical_rows": 289744,
                "excess_duplicates": 0,
                "multiset_comparison": {"missing_rows": 0, "extra_rows": 0},
                "platform_validator": {"data_loss": 10256},
            }, f)

        run_paths_map = {"kafka-stop": [exp_dir]}
        agg = aggregate_campaign(tmpdir, run_paths_map=run_paths_map, cal_file=cal_path)

        assert "kafka-stop" in agg["experiments"]
        assert agg["experiments"]["kafka-stop"]["target_container"] == "platform-kafka"

        rows = build_gap3_rows(agg)
        assert len(rows) == 1
        row = rows[0]
        assert row["Experiment name"] == "kafka-stop"
        assert row["Target container"] == "platform-kafka"
        assert "1472.0 rec/s" in row["Normal throughput (from Control / baseline window)"]
        assert row["During-fault throughput (records/sec)"] == "100.00"
        assert "15.00 ± 0.00 s" in row["Recovery time (mean +/- std)"]
        assert row["Data loss: missing rows (mean), extra rows (mean), duplicates beyond baseline (mean)"] == "0.0 / 0.0 / 0.0"

        # Test CSV export
        csv_file = os.path.join(tmpdir, "gap3.csv")
        export_gap3_csv(rows, csv_file)
        assert os.path.exists(csv_file)
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = list(csv.DictReader(f))
            assert len(reader) == 1
            assert reader[0]["Experiment name"] == "kafka-stop"

        # Test Markdown export
        md_file = os.path.join(tmpdir, "gap3.md")
        export_gap3_markdown(rows, md_file)
        assert os.path.exists(md_file)
        with open(md_file, "r", encoding="utf-8") as f:
            content = f.read()
            assert "| kafka-stop |" in content


def test_load_control_campaign_metrics_and_primary_rows():
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl_dir = os.path.join(tmpdir, "control")
        os.makedirs(ctrl_dir, exist_ok=True)

        for i in range(1, 3):
            r_dir = os.path.join(ctrl_dir, f"eval-control-r{i}")
            os.makedirs(r_dir, exist_ok=True)
            with open(os.path.join(r_dir, "report.json"), "w", encoding="utf-8") as f:
                json.dump({
                    "baseline": {"throughput_records_per_sec": 1600.0},
                    "throughputDuringFault": 1500.0,
                    "throughputAfter": 1600.0,
                    "total_duration_seconds": 220.0,
                    "time_to_first_micro_batch_seconds": 20.0,
                }, f)
            with open(os.path.join(r_dir, "physical_metrics.json"), "w", encoding="utf-8") as f:
                json.dump({
                    "physical_rows": 289744,
                    "physical_data_loss": 0,
                    "excess_duplicates": 0,
                    "multiset_comparison": {"missing_rows": 0, "extra_rows": 0},
                }, f)

        ctrl_metrics = load_control_campaign_metrics(ctrl_dir)
        assert ctrl_metrics is not None
        assert ctrl_metrics["n_runs"] == 2
        assert ctrl_metrics["baseline_throughput"]["mean"] == 1600.0
        assert ctrl_metrics["throughput_during_simulated_fault"]["mean"] == 1500.0

        agg_data = {
            "control_baseline": {"throughput_normal": 1472.0},
            "control_empirical": ctrl_metrics,
            "experiments": {},
        }
        primary_rows = build_gap3_primary_rows(agg_data)
        assert len(primary_rows) == 1
        assert "Control" in primary_rows[0]["Experiment Name"]
        assert "1600.0 ± 0.0" in primary_rows[0]["Post-Warmup Baseline (rec/s) [Primary]"]
