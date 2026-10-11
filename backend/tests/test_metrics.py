from app.metrics import pipeline_records_processed_total
from app.pipeline.config_models import PipelineConfig
from app.pipeline.executor import PipelineExecutor
from app.pipeline.generator import PipelineGenerator
from pathlib import Path


def test_pipeline_execution_emits_record_metric(tmp_path: Path) -> None:
    name = "metric-test"
    before = pipeline_records_processed_total.labels(name)._value.get()
    storage_path = str((tmp_path / "metric-test").resolve())
    config = PipelineConfig.model_validate(
        {
            "pipeline": {"name": name, "version": "1.0"},
            "source": {"type": "csv", "path": str(Path("sample-data/sales_sample.csv").resolve())},
            "schema": [
                {"name": "transaction_id", "type": "string"},
                {"name": "amount", "type": "double"},
                {"name": "customer_id", "type": "string"},
                {"name": "product_id", "type": "string"},
                {"name": "sale_date", "type": "string"},
            ],
            "processing": {"operations": [{"type": "filter", "condition": "amount > 0"}]},
            "streaming": {"enabled": False},
            "storage": {"type": "local", "path": storage_path},
        }
    )
    assert PipelineExecutor().execute(PipelineGenerator().generate(config)).status == "success"
    assert pipeline_records_processed_total.labels(name)._value.get() > before
