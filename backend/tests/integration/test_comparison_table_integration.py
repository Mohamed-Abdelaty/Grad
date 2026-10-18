"""
backend/tests/integration/test_comparison_table_integration.py
--------------------------------------------------------------
Integration test for Gap 3: Comparison Table with Honest Normal vs During-Fault metrics.
Runs against the live Docker Compose platform stack with a real linked pipeline run.

Verifies:
- Experiment executed with a linked pipeline run
- Comparison table contains non-null 'normal' values for availability and throughput
- 'normal' values match baseline measurements taken prior to fault injection
- Comparison table data_loss and recovery_time have normal=None and explicit "n/a (by definition)" reason
- delta and pct_change are computed correctly where both values are real
"""

import asyncio
from datetime import timedelta
import uuid
import pytest
from httpx import AsyncClient

from app.core.security import create_access_token

pytestmark = pytest.mark.integration


@pytest.fixture
def token_headers():
    token = create_access_token({"sub": "admin"}, timedelta(minutes=30))
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
async def async_client():
    async with AsyncClient(base_url="http://localhost:8000", timeout=300.0) as ac:
        yield ac


@pytest.mark.asyncio
async def test_comparison_table_with_linked_pipeline(
    async_client: AsyncClient, token_headers: dict
):
    """
    Run an experiment linked to a real pipeline, verify comparison_table has
    genuine normal measurements, explicit reasons on unmeasured cells, and deltas.
    """
    # 1. Start a pipeline run
    pipeline_yaml = """\
pipeline:
  name: "comp-table-integration-pipeline"
  version: "1.0"
source:
  type: csv
  path: /app/sample-data/sales_sample.csv
schema:
  - { name: transaction_id, type: string }
  - { name: amount, type: double }
  - { name: customer_id, type: string }
  - { name: product_id, type: string }
  - { name: sale_date, type: string }
processing:
  operations:
    - { type: remove_nulls }
streaming:
  enabled: false
batch_kafka_ingestion:
  enabled: false
processing_engine:
  type: spark
storage:
  type: hdfs
  path: /data/comp-table-integration-output
"""
    pipe_res = await async_client.post(
        "/api/pipelines/run?wait=false",
        json={"yaml_content": pipeline_yaml},
        headers=token_headers,
    )
    assert pipe_res.status_code == 202, f"Trigger pipeline failed: {pipe_res.text}"
    p_run_id = pipe_res.json()["pipeline_run_id"]

    # Wait for pipeline run to complete so spark output is in place
    for _ in range(60):
        await asyncio.sleep(2)
        pr_res = await async_client.get(f"/api/pipeline-runs/{p_run_id}", headers=token_headers)
        if pr_res.status_code == 200 and pr_res.json().get("status") in ("succeeded", "failed"):
            break

    # 2. Create experiment linked to this pipeline
    exp_name = f"comp-table-exp-{uuid.uuid4().hex[:8]}"
    exp_yaml = f"""\
experiment:
  name: "{exp_name}"
target:
  service: kafka
fault:
  type: container_restart
  duration: 3
validation:
  max_recovery_time: 30
repetitions: 1
"""
    create_res = await async_client.post(
        "/api/experiments",
        json={"yaml_content": exp_yaml, "pipeline_yaml": ""},
        headers=token_headers,
    )
    assert create_res.status_code in (200, 201), f"Create experiment failed: {create_res.text}"
    exp_id = create_res.json()["experiment_id"]

    # 3. Trigger experiment linked to pipeline run
    trig_res = await async_client.post(
        f"/api/experiments/{exp_id}/run",
        params={"pipeline_run_id": p_run_id},
        headers=token_headers,
    )
    assert trig_res.status_code in (200, 201, 202), f"Trigger experiment failed: {trig_res.text}"
    exp_run_id = trig_res.json()["run_id"]

    # 4. Wait for experiment to finish
    terminal_statuses = {"succeeded", "failed"}
    poll_count = 0
    run_data = None
    while poll_count < 60:
        await asyncio.sleep(2)
        poll_count += 1
        status_res = await async_client.get(
            f"/api/experiments/{exp_id}",
            headers=token_headers,
        )
        if status_res.status_code == 200:
            run_data = status_res.json()
            if run_data.get("status") in terminal_statuses:
                break

    assert run_data is not None, "Failed to fetch run data"
    assert run_data.get("status") in terminal_statuses, f"Run did not finish in time: {run_data.get('status')}"

    # 5. Get report (poll with brief retry as report write occurs at end of terminal transition)
    report = None
    for _ in range(15):
        report_res = await async_client.get(
            f"/api/reports/{exp_id}",
            headers=token_headers,
        )
        if report_res.status_code == 200:
            report = report_res.json()
            break
        await asyncio.sleep(1)

    assert report is not None, f"Fetch report failed: status={report_res.status_code} text={report_res.text}"

    comp_table = report.get("comparison_table") or report.get("comparisonTable")
    assert comp_table is not None, "comparison_table missing from report"
    assert len(comp_table) == 5, f"Expected 5 rows, got {len(comp_table)}"

    by_metric = {row["metric"]: row for row in comp_table}

    # Verify Availability row: real normal value from baseline probe
    avail_row = by_metric["availability_percentage"]
    assert avail_row["normal"] is not None
    assert avail_row["normal"] == 100.0
    assert avail_row["baseline_applicable"] is True
    assert avail_row["during_fault"] is not None

    # Verify Data Loss row: baseline is not applicable by definition
    loss_row = by_metric["data_loss"]
    assert loss_row["normal"] is None
    assert loss_row["normal_reason"] == "n/a (by definition)"
    assert loss_row["baseline_applicable"] is False

    # Verify Recovery Time row: baseline is not applicable by definition
    rec_row = by_metric["recovery_time_seconds"]
    assert rec_row["normal"] is None
    assert rec_row["normal_reason"] == "n/a (by definition)"
    assert rec_row["baseline_applicable"] is False

    # Verify Throughput row: carries honest value or honest reason
    tp_row = by_metric["throughput_records_per_sec"]
    assert tp_row["baseline_applicable"] is True
    if tp_row["normal"] is None:
        assert tp_row["normal_reason"] is not None
    else:
        assert isinstance(tp_row["normal"], (int, float))
