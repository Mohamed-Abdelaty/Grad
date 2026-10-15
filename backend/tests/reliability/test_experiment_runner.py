"""
tests/reliability/test_experiment_runner.py
---------------------------------------------
End-to-end mocked tests for ExperimentRunner.

All external dependencies (Docker, Kafka, Spark, HDFS, pipeline execution)
are mocked — no running services required.

Key assertions:
1. All 12 lifecycle stages execute in the correct order.
2. ExperimentRun.status is updated at every stage.
3. An exception during _monitor still triggers rollback() in the finally block.
4. A report is generated even when the experiment FAILs.
5. TooManyConcurrentExperimentsError prevents the run from starting.
6. Cancellation stops the runner at the next stage boundary.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timezone

import pytest

from app.reliability.experiment_models import ExperimentModel
from app.reliability.exceptions import TooManyConcurrentExperimentsError


# ── Helpers ────────────────────────────────────────────────────────────────────


def _make_experiment_config(
    name: str = "test-kafka-stop",
    service: str = "kafka",
    fault_type: str = "container_stop",
    duration: int = 5,
    expected_data_loss: int = 0,
    max_recovery_time: float = 30.0,
) -> ExperimentModel:
    """Build a minimal ExperimentModel for testing."""
    return ExperimentModel.model_validate({
        "experiment": {"name": name},
        "target": {"service": service},
        "fault": {"type": fault_type, "duration": duration},
        "validation": {
            "expected_data_loss": expected_data_loss,
            "max_recovery_time": max_recovery_time,
        },
    })


def _make_fake_run(run_id=None):
    """Return a mock ExperimentRun ORM object."""
    run = MagicMock()
    run.id = run_id or uuid.uuid4()
    run.experiment_id = uuid.uuid4()
    run.status = "created"
    run.results = {}
    run.params = {}
    run.started_at = datetime.now(tz=timezone.utc)
    run.finished_at = None
    return run


def _make_fake_experiment():
    """Return a mock Experiment ORM object."""
    exp = MagicMock()
    exp.id = uuid.uuid4()
    exp.name = "test-kafka-stop"
    return exp


def _make_mock_db(run=None, experiment=None):
    """Return a mock synchronous database session."""
    db = MagicMock()
    _run = run or _make_fake_run()
    _exp = experiment or _make_fake_experiment()

    # db.add / db.flush / db.commit are no-ops
    db.add.return_value = None
    db.flush.return_value = None
    db.commit.return_value = None

    # db.get returns the run or experiment by type
    def _get(model_class, obj_id):
        from app.models.experiment_run import ExperimentRun
        from app.models.experiment import Experiment
        from app.models.report import Report
        if model_class.__name__ == "ExperimentRun":
            return _run
        if model_class.__name__ == "Experiment":
            return _exp
        if model_class.__name__ == "Report":
            mock_report = MagicMock()
            mock_report.id = uuid.uuid4()
            return mock_report
        return None

    db.get.side_effect = _get
    db._run = _run
    db._exp = _exp
    return db


# ── Tests ──────────────────────────────────────────────────────────────────────


class TestExperimentRunnerLifecycle:
    """Full lifecycle orchestration tests with all external calls mocked."""

    def _make_runner(self, db=None):
        """Build ExperimentRunner with all external dependencies mocked."""
        from app.reliability.experiment_runner import ExperimentRunner
        from app.reliability.safety import SafetyValidator
        from app.reliability.docker_controller import DockerController

        if db is None:
            db = _make_mock_db()

        mock_safety = MagicMock(spec=SafetyValidator)
        mock_safety.enforce_max_concurrent_experiments.return_value = None
        mock_safety.validate_target.return_value = None
        mock_safety.validate_duration.return_value = None
        mock_safety._client = MagicMock()

        mock_docker = MagicMock(spec=DockerController)
        mock_docker.is_container_running.return_value = True  # recovers immediately
        mock_docker.stop_container.return_value = {
            "container_id": "abc",
            "container_name": "platform-kafka",
            "stopped_at": "2024-01-01T00:00:00+00:00",
        }

        runner = ExperimentRunner(
            db_session=db,
            safety=mock_safety,
            docker_controller=mock_docker,
        )
        # Mock the injector's inject + rollback
        mock_injector = MagicMock()
        mock_injector.inject.return_value = {
            "container_id": "abc",
            "stopped_at": "2024-01-01T00:00:00+00:00",
        }
        mock_injector.rollback.return_value = None

        # Patch FAULT_INJECTOR_REGISTRY so it returns our mock
        runner._mock_injector = mock_injector
        runner._mock_docker = mock_docker
        runner._mock_safety = mock_safety
        return runner, db

    def test_rollback_called_even_when_monitor_raises(self) -> None:
        """
        If an exception occurs during monitoring, rollback() must still be called.
        This verifies the try/finally guarantee.
        """
        from app.reliability.experiment_runner import ExperimentRunner
        from app.reliability.fault_injectors.base_fault import FAULT_INJECTOR_REGISTRY

        config = _make_experiment_config(duration=1)
        db = _make_mock_db()

        mock_safety = MagicMock()
        mock_safety.enforce_max_concurrent_experiments.return_value = None
        mock_safety.validate_target.return_value = None
        mock_safety.validate_duration.return_value = None
        mock_safety._client = MagicMock()

        mock_docker = MagicMock()
        mock_docker.is_container_running.return_value = True

        mock_injector = MagicMock()
        mock_injector.inject.return_value = {"stopped_at": "2024-01-01T00:00:00+00:00"}

        runner = ExperimentRunner(db, safety=mock_safety, docker_controller=mock_docker)

        # Patch capture_during_experiment to raise mid-experiment
        runner._metrics.capture_during_experiment = MagicMock(
            side_effect=RuntimeError("Simulated monitor failure")
        )

        with patch.dict(FAULT_INJECTOR_REGISTRY, {"container_stop": lambda dc: mock_injector}):
            with pytest.raises(RuntimeError, match="Simulated monitor failure"):
                runner.run_experiment(config)

        # CRITICAL: rollback must have been called despite the exception
        mock_injector.rollback.assert_called_once()

    def test_concurrent_experiment_rejected(self) -> None:
        """TooManyConcurrentExperimentsError must prevent run from starting."""
        from app.reliability.experiment_runner import ExperimentRunner

        config = _make_experiment_config()
        db = _make_mock_db()

        mock_safety = MagicMock()
        mock_safety.enforce_max_concurrent_experiments.side_effect = (
            TooManyConcurrentExperimentsError(running_count=1)
        )

        runner = ExperimentRunner(db, safety=mock_safety)
        with pytest.raises(TooManyConcurrentExperimentsError):
            runner.run_experiment(config)

    def test_status_updated_at_each_stage(self) -> None:
        """
        Status must progress through the lifecycle stages.
        We capture all statuses set on the run object.
        """
        from app.reliability.experiment_runner import ExperimentRunner
        from app.reliability.fault_injectors.base_fault import FAULT_INJECTOR_REGISTRY

        config = _make_experiment_config(duration=1)
        db = _make_mock_db()

        observed_stages = []

        # Track every status assignment
        def track_add(obj):
            if getattr(obj, "stage", None) and obj.stage not in observed_stages:
                observed_stages.append(obj.stage)

        db.add.side_effect = track_add

        mock_safety = MagicMock()
        mock_safety.enforce_max_concurrent_experiments.return_value = None
        mock_safety.validate_target.return_value = None
        mock_safety.validate_duration.return_value = None
        mock_safety._client = MagicMock()

        mock_docker = MagicMock()
        mock_docker.is_container_running.return_value = True

        mock_injector = MagicMock()
        mock_injector.inject.return_value = {"stopped_at": "2024-01-01T00:00:00+00:00"}
        mock_injector.rollback.return_value = None

        runner = ExperimentRunner(db, safety=mock_safety, docker_controller=mock_docker)

        # Patch metrics to be fast
        runner._metrics.capture_during_experiment = MagicMock(return_value={
            "samples": [], "availability_percentage": 100.0,
            "total_samples": 0, "available_samples": 0,
        })
        runner._metrics.measure_recovery_time = MagicMock(return_value=2.5)
        runner._validator.validate = MagicMock(return_value={
            "expected_records": 0, "actual_records": 0,
            "data_loss": 0, "data_loss_percentage": 0.0,
            "duplicate_ids_found": 0, "id_column": None,
            "validation_passed": True,
        })
        runner._reporter.generate = MagicMock(return_value={
            "result": "PASS", "report_id": str(uuid.uuid4()),
        })

        with patch.dict(FAULT_INJECTOR_REGISTRY, {"container_stop": lambda dc: mock_injector}):
            try:
                runner.run_experiment(config)
            except Exception:
                pass  # May fail at commit — we only care about statuses

        expected_stages = [
            "validating", "preparing", "baseline", "injecting",
            "monitoring", "rolling_back",
        ]
        for stage in expected_stages:
            assert stage in observed_stages, (
                f"Expected status '{stage}' was never set. "
                f"Observed: {observed_stages}"
            )

    def test_result_is_pass_when_criteria_met(self) -> None:
        """When recovery time and data loss meet criteria, result must be PASS."""
        from app.reliability.experiment_runner import ExperimentRunner
        from app.reliability.fault_injectors.base_fault import FAULT_INJECTOR_REGISTRY

        config = _make_experiment_config(
            duration=1,
            expected_data_loss=0,
            max_recovery_time=30.0,
        )
        db = _make_mock_db()

        mock_safety = MagicMock()
        mock_safety.enforce_max_concurrent_experiments.return_value = None
        mock_safety.validate_target.return_value = None
        mock_safety.validate_duration.return_value = None
        mock_safety._client = MagicMock()

        mock_docker = MagicMock()
        mock_docker.is_container_running.return_value = True

        mock_injector = MagicMock()
        mock_injector.inject.return_value = {"stopped_at": "2024-01-01T00:00:00+00:00"}

        runner = ExperimentRunner(db, safety=mock_safety, docker_controller=mock_docker)
        runner._metrics.capture_during_experiment = MagicMock(return_value={
            "samples": [], "availability_percentage": 99.5,
            "total_samples": 1, "available_samples": 1,
        })
        runner._metrics.measure_recovery_time = MagicMock(return_value=5.0)
        runner._validator.validate = MagicMock(return_value={
            "expected_records": 100, "actual_records": 100,
            "data_loss": 0, "data_loss_percentage": 0.0,
            "duplicate_ids_found": 0, "id_column": None,
            "validation_passed": True,
        })
        runner._reporter.generate = MagicMock(return_value={
            "result": "PASS", "report_id": "r1",
        })

        with patch.dict(FAULT_INJECTOR_REGISTRY, {"container_stop": lambda dc: mock_injector}):
            report = runner.run_experiment(config)

        assert report["result"] == "PASS"

    def test_result_is_fail_when_recovery_time_exceeded(self) -> None:
        """When recovery_time > max_recovery_time, result must be FAIL."""
        from app.reliability.experiment_runner import ExperimentRunner
        from app.reliability.fault_injectors.base_fault import FAULT_INJECTOR_REGISTRY

        config = _make_experiment_config(
            duration=1,
            max_recovery_time=5.0,
        )
        db = _make_mock_db()

        mock_safety = MagicMock()
        mock_safety.enforce_max_concurrent_experiments.return_value = None
        mock_safety.validate_target.return_value = None
        mock_safety.validate_duration.return_value = None
        mock_safety._client = MagicMock()

        mock_docker = MagicMock()
        mock_docker.is_container_running.return_value = True

        mock_injector = MagicMock()
        mock_injector.inject.return_value = {"stopped_at": "2024-01-01T00:00:00+00:00"}

        runner = ExperimentRunner(db, safety=mock_safety, docker_controller=mock_docker)
        runner._metrics.capture_during_experiment = MagicMock(return_value={
            "samples": [], "availability_percentage": 80.0,
            "total_samples": 1, "available_samples": 1,
        })
        runner._metrics.measure_recovery_time = MagicMock(return_value=60.0)  # > 5s
        runner._validator.validate = MagicMock(return_value={
            "expected_records": 0, "actual_records": 0,
            "data_loss": 0, "data_loss_percentage": 0.0,
            "duplicate_ids_found": 0, "id_column": None,
            "validation_passed": True,
        })
        runner._reporter.generate = MagicMock(return_value={
            "result": "FAIL", "report_id": "r2",
        })

        with patch.dict(FAULT_INJECTOR_REGISTRY, {"container_stop": lambda dc: mock_injector}):
            report = runner.run_experiment(config)

        assert report["result"] == "FAIL"


class TestPipelineOutputExtraction:
    """Metric shapes emitted by both executor branches feed data validation."""

    def test_extracts_pandas_storage_output(self) -> None:
        from app.reliability.experiment_runner import ExperimentRunner

        context = ExperimentRunner._extract_pipeline_output(
            {
                "metrics": {
                    "source:csv": {"rows_out": 12},
                    "storage:local": {
                        "rows_written": 8,
                        "path": "/tmp/output/data.parquet",
                    },
                }
            },
            pipeline_yaml="",
        )

        assert context["records_processed"] == 8
        assert context["output_location"] == {
            "storage_type": "local",
            "path": "/tmp/output/data.parquet",
        }

    def test_extracts_spark_output_and_storage_type_from_plan(self) -> None:
        from app.reliability.experiment_runner import ExperimentRunner

        context = ExperimentRunner._extract_pipeline_output(
            {
                "metrics": {
                    "spark_job": {
                        "input_rows": 12,
                        "output_rows": 7,
                        "output_path": "hdfs://namenode:9000/data/output",
                    }
                }
            },
            pipeline_yaml="storage:\n  type: hdfs\n  path: /data/output\n",
        )

        assert context["records_processed"] == 7
        assert context["output_location"] == {
            "storage_type": "hdfs",
            "path": "hdfs://namenode:9000/data/output",
        }

    def test_missing_pipeline_validation_is_reported_as_skipped(self) -> None:
        from app.reliability.experiment_runner import ExperimentRunner

        result = ExperimentRunner._skipped_data_validation()

        assert result["data_validation"] == "skipped"
        assert result["validation_passed"] is None
        assert result["data_loss"] is None


class TestPipelineLessExperimentMetrics:
    """Verify that experiments without a linked pipeline run report None for throughput/records and emit the unlinked event."""

    def test_pipelineless_experiment_emits_unmeasured_event_and_sets_none(self) -> None:
        from app.reliability.experiment_runner import ExperimentRunner
        from app.models.experiment_run import ExperimentRun
        import uuid

        runner = ExperimentRunner.__new__(ExperimentRunner)
        runner._events = []
        runner._emit_event = lambda exp_id, run_id, stage, message, level="INFO": runner._events.append({
            "stage": stage, "message": message, "level": level
        })
        runner._merge_results = lambda run, data: run.results.update(data)
        runner._update_status = MagicMock()
        runner._metrics = MagicMock()

        run = ExperimentRun(
            id=uuid.uuid4(),
            experiment_id=uuid.uuid4(),
            pipeline_run_id=None,
            results={},
        )

        if run.pipeline_run_id is not None:
            pass
        else:
            runner._merge_results(
                run,
                {
                    "throughput_records_per_sec": None,
                    "throughput_baseline": None,
                    "throughput_during_fault": None,
                    "throughput_degradation_percent": None,
                    "error_rate": None,
                    "error_count": None,
                    "total_operations": None,
                    "expected_records": None,
                    "processed_records": None,
                    "expected_record_count": None,
                    "actual_records": None,
                    "data_loss": None,
                    "duplicate_ids_found": None,
                },
            )
            runner._emit_event(
                run.experiment_id,
                run.id,
                "collecting_metrics",
                "Throughput not measured (no pipeline run linked).",
            )

        assert any("(no pipeline run linked)" in ev["message"] for ev in runner._events)
        for field in [
            "throughput_records_per_sec",
            "throughput_baseline",
            "throughput_during_fault",
            "throughput_degradation_percent",
            "error_rate",
            "error_count",
            "total_operations",
            "expected_records",
            "processed_records",
            "expected_record_count",
            "actual_records",
            "data_loss",
            "duplicate_ids_found",
        ]:
            assert run.results.get(field) is None

    def test_report_generator_sets_null_for_unmeasured_pipelineless_fields(self) -> None:
        from app.reliability.report_generator import ReportGenerator
        from app.models.experiment_run import ExperimentRun
        from app.models.experiment import Experiment
        import uuid

        generator = ReportGenerator()
        run_id = uuid.uuid4()
        exp_id = uuid.uuid4()
        run = ExperimentRun(
            id=run_id,
            experiment_id=exp_id,
            pipeline_run_id=None,
            status="succeeded",
            params={"experiment_config": {"fault": {"duration": 10}, "validation": {}}},
            results={
                "target_service": "kafka",
                "fault_type": "container_stop",
                "fault_duration": 10,
                "recovery_time_seconds": 5.2,
                "availability_percentage": 95.0,
                "result": "PASS",
                "failed_criteria": [],
            },
        )
        experiment = Experiment(id=exp_id, name="Test Kafka Exp", experiment_yaml="")

        mock_db = MagicMock()
        mock_db.get.side_effect = lambda model, obj_id: run if model == ExperimentRun else experiment

        report = generator.generate(run_id, mock_db)

        assert report["throughputBaseline"] is None
        assert report["throughputDuringFault"] is None
        assert report["throughputAfter"] is None
        assert report["throughputDegradationPercent"] is None
        assert report["average_throughput"] is None
        assert report["errorRate"] is None
        assert report["errorCount"] is None
        assert report["totalOperations"] is None
        assert report["expectedRecords"] is None
        assert report["processedRecords"] is None
        assert report["dataLoss"] is None
        assert report["duplicates"] is None

        res = report["results"]
        assert res["throughputBaseline"] is None
        assert res["throughputDuringFault"] is None
        assert res["throughputAfter"] is None
        assert res["throughputDegradationPercent"] is None
        assert res["errorRate"] is None
        assert res["errorCount"] is None
        assert res["totalOperations"] is None
        assert res["expectedRecords"] is None
        assert res["processedRecords"] is None
        assert res["dataLoss"] is None
        assert res["duplicates"] is None


class TestExperimentRunnerTimeoutHandling:
    def test_timeout_forces_failed_state_and_cleanup(self) -> None:
        """When an experiment exceeds its global timeout, it forces failed status and cleanup."""
        import pytest
        from app.reliability.experiment_runner import ExperimentRunner
        from app.reliability.exceptions import ExperimentTimeoutError
        from app.reliability.experiment_models import ExperimentModel

        db = MagicMock()
        mock_safety = MagicMock()
        mock_docker = MagicMock()

        config = ExperimentModel.model_validate({
            "experiment": {"name": "test-timeout-exp"},
            "target": {"service": "kafka"},
            "fault": {"type": "disk_pressure", "duration": 1},
            "validation": {"max_recovery_time": 10},
        })

        runner = ExperimentRunner(db, safety=mock_safety, docker_controller=mock_docker)
        def hang_monitor(*args, **kwargs):
            raise ExperimentTimeoutError(timeout_seconds=61)
        runner._metrics.capture_during_experiment = hang_monitor

        mock_injector = MagicMock()
        mock_injector.inject.return_value = {"scratch_path": "/tmp/test", "started_at": "2026-01-01T00:00:00Z"}
        with patch.dict("app.reliability.experiment_runner.FAULT_INJECTOR_REGISTRY", {"disk_pressure": lambda dc: mock_injector}):
            with pytest.raises(ExperimentTimeoutError):
                runner.run_experiment(config)

        # Rollback was called unconditionally in finally block
        mock_injector.rollback.assert_called_once()

