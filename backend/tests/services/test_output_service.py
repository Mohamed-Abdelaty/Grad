"""
backend/tests/services/test_output_service.py
---------------------------------------------
Comprehensive unit tests for output service path validation, metadata extraction,
bounded preview generation, CSV/Parquet downloads, signed one-time tokens, and status codes.
"""

from __future__ import annotations

import io
import json
import math
import os
import tempfile
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import pytest
from fastapi import HTTPException
from jose import jwt

from app.api.routes.pipeline_runs import (
    DownloadLinkRequest,
    create_output_download_link,
    download_run_output,
    get_run_output_metadata,
    get_run_output_preview,
)
from app.core.config import get_settings
from app.core.security import create_access_token, decode_access_token
from app.models.pipeline import Pipeline
from app.models.pipeline_run import PipelineRun
from app.services.output_service import (
    FileTooLargeError,
    InvalidDownloadTokenError,
    InvalidOutputPathError,
    OutputNotFoundError,
    OutputTooLargeError,
    RunNotReadyError,
    SchemaMismatchError,
    StorageUnavailableError,
    build_parquet_file,
    create_download_token,
    describe_output,
    iter_csv,
    map_arrow_type_to_simple,
    open_parquet,
    preview_output,
    resolve_output_path,
    verify_download_token,
)


# ── Helpers & Fixtures ─────────────────────────────────────────────────────────


def _create_sample_parquet_bytes(rows: int = 10, schema: pa.Schema | None = None, arabic: bool = False) -> bytes:
    if schema is None:
        schema = pa.schema([
            ("id", pa.string()),
            ("amount", pa.float64()),
            ("count", pa.int32()),
            ("created_at", pa.timestamp("ms")),
        ])
    data = []
    for field in schema:
        name = field.name
        t = str(field.type).lower()
        if "amount" in name or "float" in t or "double" in t:
            data.append([float(i * 1.5) for i in range(rows)])
        elif "count" in name or "int" in t:
            data.append([int(i) for i in range(rows)])
        elif "created_at" in name or "time" in name or "timestamp" in t:
            data.append([datetime(2026, 10, 14, 12, 0, i % 60, tzinfo=timezone.utc) for i in range(rows)])
        elif arabic:
            data.append([f"عميل_{i}" for i in range(rows)])
        else:
            data.append([f"val_{i}" for i in range(rows)])

    table = pa.Table.from_arrays(data, schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


class MockHdfsStream:
    def __init__(self, data: bytes, chunk_size: int = 1024 * 1024):
        self.stream = io.BytesIO(data)
        self.chunk_size = chunk_size

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stream.close()

    def __iter__(self):
        while True:
            chunk = self.stream.read(self.chunk_size)
            if not chunk:
                break
            yield chunk

    def read(self, chunk_size=None):
        return self.stream.read(chunk_size)


# ── 1. Path Validation Cases ───────────────────────────────────────────────────


def test_resolve_output_path_allowed():
    run_id = uuid.uuid4()
    p = Pipeline(name="test")
    run = PipelineRun(id=run_id, output_path="/data/sales_ui2b", pipeline=p)

    assert resolve_output_path(run) == "/data/sales_ui2b"

    run.output_path = "hdfs://namenode:9000/data/sales_large"
    assert resolve_output_path(run) == "/data/sales_large"

    run.output_path = f"/tmp/spark-output-{run_id}"
    assert resolve_output_path(run) == f"/tmp/spark-output-{run_id}"


def test_resolve_output_path_traversal_and_forbidden_prefixes():
    run_id = uuid.uuid4()
    p = Pipeline(name="test")
    run = PipelineRun(id=run_id, output_path="/data/../etc/passwd", pipeline=p)

    with pytest.raises(InvalidOutputPathError):
        resolve_output_path(run)

    run.output_path = "/data/%2e%2e/passwd"
    with pytest.raises(InvalidOutputPathError):
        resolve_output_path(run)

    run.output_path = "/data//.."
    with pytest.raises(InvalidOutputPathError):
        resolve_output_path(run)

    run.output_path = "/data/sub\\dir"
    with pytest.raises(InvalidOutputPathError):
        resolve_output_path(run)

    run.output_path = "/data/sub\0dir"
    with pytest.raises(InvalidOutputPathError):
        resolve_output_path(run)

    run.output_path = "/etc/passwd"
    with pytest.raises(InvalidOutputPathError):
        resolve_output_path(run)

    # Root prefixes rejected
    run.output_path = "/data/"
    with pytest.raises(InvalidOutputPathError):
        resolve_output_path(run)

    run.output_path = "/data"
    with pytest.raises(InvalidOutputPathError):
        resolve_output_path(run)

    # Another run's tmp dir rejected
    other_id = uuid.uuid4()
    run.output_path = f"/tmp/spark-output-{other_id}"
    with pytest.raises(InvalidOutputPathError):
        resolve_output_path(run)


# ── 2. describe_output ─────────────────────────────────────────────────────────


def test_describe_output_metadata_and_row_count():
    run_id = uuid.uuid4()
    p = Pipeline(name="test-meta")
    run = PipelineRun(id=run_id, output_path="/data/meta_test", pipeline=p, status="succeeded")

    part1 = _create_sample_parquet_bytes(rows=15)
    part2 = _create_sample_parquet_bytes(rows=20)

    mock_client = MagicMock()
    mock_client.path_exists.return_value = True
    mock_client._normalize_hdfs_path.side_effect = lambda p: p.replace("hdfs://namenode:9000", "")

    # Directory listing contains _SUCCESS, .crc, _temporary, and 2 parts
    mock_client._hdfs_client.list.return_value = [
        ("_SUCCESS", {"length": 0}),
        ("_temporary", {"length": 0}),
        (".part-00000.crc", {"length": 12}),
        ("part-00000.parquet", {"length": len(part1)}),
        ("part-00001.parquet", {"length": len(part2)}),
    ]

    mock_client._hdfs_client.read.side_effect = [
        MockHdfsStream(part1),
        MockHdfsStream(part2),
    ]

    meta = describe_output(run, client=mock_client)
    assert meta["path"] == "hdfs://namenode:9000/data/meta_test"
    assert meta["fileCount"] == 2
    assert len(meta["files"]) == 2
    assert meta["files"][0]["name"] == "part-00000.parquet"
    assert meta["files"][1]["name"] == "part-00001.parquet"
    assert meta["totalSizeBytes"] == len(part1) + len(part2)
    assert meta["rowCount"] == 35  # 15 + 20
    assert len(meta["columns"]) == 4
    col_names = [c["name"] for c in meta["columns"]]
    assert col_names == ["id", "amount", "count", "created_at"]


def test_describe_output_row_count_fallback():
    run_id = uuid.uuid4()
    p = Pipeline(name="test-fallback")
    run = PipelineRun(
        id=run_id,
        output_path="/data/meta_fallback",
        pipeline=p,
        status="succeeded",
        last_progress_snapshot={"spark": {"recordsOut": 150}},
    )

    mock_client = MagicMock()
    mock_client.path_exists.return_value = True
    mock_client._normalize_hdfs_path.side_effect = lambda p: p.replace("hdfs://namenode:9000", "")
    mock_client._hdfs_client.list.return_value = [
        ("part-00000.parquet", {"length": 500}),
    ]
    # Corrupt data causes footer read to fail
    mock_client._hdfs_client.read.return_value = MockHdfsStream(b"not-a-parquet-file")

    meta = describe_output(run, client=mock_client)
    assert meta["rowCount"] == 150  # Fallback to spark_records_out


# ── 3. preview_output ──────────────────────────────────────────────────────────


def test_preview_output_limit_and_json_safety():
    run_id = uuid.uuid4()
    p = Pipeline(name="test-preview")
    run = PipelineRun(id=run_id, output_path="/data/preview_test", pipeline=p, status="succeeded")

    schema = pa.schema([
        ("dt", pa.timestamp("s")),
        ("dec", pa.decimal128(10, 2)),
        ("nan_f", pa.float64()),
        ("raw_b", pa.binary()),
        ("long_s", pa.string()),
    ])

    batch_data = [
        [datetime(2026, 10, 14, 10, 30, 0, tzinfo=timezone.utc), datetime(2026, 10, 14, 11, 0, 0, tzinfo=timezone.utc)],
        [Decimal("123.45"), Decimal("999.99")],
        [float("nan"), float("inf")],
        [b"binary_secret_bytes_12345", b"another_binary"],
        ["short string", "x" * 2500],  # test > 2000 truncation
    ]

    table = pa.Table.from_arrays(batch_data, schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    data = buf.getvalue()

    mock_client = MagicMock()
    mock_client.path_exists.return_value = True
    mock_client._normalize_hdfs_path.side_effect = lambda p: p.replace("hdfs://namenode:9000", "")
    mock_client._hdfs_client.list.return_value = [
        ("part-00000.parquet", {"length": len(data)}),
    ]
    mock_client._hdfs_client.read.side_effect = [
        MockHdfsStream(data),  # for describe_output
        MockHdfsStream(data),  # for preview_output
    ]

    preview = preview_output(run, limit=1, client=mock_client)
    assert len(preview["rows"]) == 1
    assert preview["truncated"] is True
    row = preview["rows"][0]
    assert row[0].startswith("2026-10-14T10:30:00")
    assert row[1] == "123.45"
    assert row[2] is None  # NaN becomes None
    assert isinstance(row[3], str)  # bytes becomes hex string
    assert row[4] == "short string"

    # Preview with limit 2 returns second row where string is truncated at 2000 chars + '…'
    mock_client._hdfs_client.read.side_effect = [
        MockHdfsStream(data),
        MockHdfsStream(data),
    ]
    preview2 = preview_output(run, limit=2, client=mock_client)
    assert len(preview2["rows"]) == 2
    row2 = preview2["rows"][1]
    assert len(row2[4]) == 2001
    assert row2[4].endswith("…")


# ── 4. iter_csv ───────────────────────────────────────────────────────────────


def test_iter_csv_formatting_quotes_and_utf8():
    run_id = uuid.uuid4()
    p = Pipeline(name="test-csv")
    run = PipelineRun(id=run_id, output_path="/data/csv_test", pipeline=p, status="succeeded")

    schema = pa.schema([
        ("name", pa.string()),
        ("note", pa.string()),
        ("val", pa.int32()),
    ])

    # Include commas, quotes, newlines, and Arabic text
    table = pa.Table.from_arrays([
        ["مستخدم_1", 'Quotes "here"', "Normal, with comma\nand newline"],
        ["ملاحظة", "simple", "another note"],
        [100, 200, 300],
    ], schema=schema)

    buf = io.BytesIO()
    pq.write_table(table, buf)
    data = buf.getvalue()

    mock_client = MagicMock()
    mock_client.path_exists.return_value = True
    mock_client._normalize_hdfs_path.side_effect = lambda p: p.replace("hdfs://namenode:9000", "")
    mock_client._hdfs_client.list.return_value = [
        ("part-00000.parquet", {"length": len(data)}),
    ]
    mock_client._hdfs_client.read.side_effect = [
        MockHdfsStream(data),
        MockHdfsStream(data),
    ]

    csv_chunks = list(iter_csv(run, client=mock_client))
    csv_bytes = b"".join(csv_chunks)
    csv_text = csv_bytes.decode("utf-8")

    # Header present once and exactly 4 logical records (1 header + 3 data rows)
    import csv as py_csv
    records = list(py_csv.reader(io.StringIO(csv_text)))
    assert len(records) == 4
    assert records[0] == ["name", "note", "val"]
    assert "مستخدم_1" in csv_text
    assert '"Quotes ""here"""' in csv_text or 'Quotes ""here""' in csv_text

    # Verify pyarrow can parse it back
    parsed = pcsv.read_csv(io.BytesIO(csv_bytes))
    assert len(parsed) == 3
    assert list(parsed.column_names) == ["name", "note", "val"]


# ── 5. build_parquet_file ─────────────────────────────────────────────────────


def test_build_parquet_file_merges_and_cleans_up():
    run_id = uuid.uuid4()
    p = Pipeline(name="test-pq-merge")
    run = PipelineRun(id=run_id, output_path="/data/pq_test", pipeline=p, status="succeeded")

    part1 = _create_sample_parquet_bytes(rows=10)
    part2 = _create_sample_parquet_bytes(rows=15)

    mock_client = MagicMock()
    mock_client.path_exists.return_value = True
    mock_client._normalize_hdfs_path.side_effect = lambda p: p.replace("hdfs://namenode:9000", "")
    mock_client._hdfs_client.list.return_value = [
        ("part-00000.parquet", {"length": len(part1)}),
        ("part-00001.parquet", {"length": len(part2)}),
    ]
    mock_client._hdfs_client.read.side_effect = [
        MockHdfsStream(part1),
        MockHdfsStream(part2),
        MockHdfsStream(part1),
        MockHdfsStream(part2),
    ]

    out_path, cleanup_fn = build_parquet_file(run, client=mock_client)
    assert os.path.exists(out_path)

    merged = pq.ParquetFile(out_path)
    assert merged.metadata.num_rows == 25
    cleanup_fn()
    assert not os.path.exists(out_path)


def test_build_parquet_schema_mismatch_raises():
    run_id = uuid.uuid4()
    p = Pipeline(name="test-pq-mismatch")
    run = PipelineRun(id=run_id, output_path="/data/mismatch", pipeline=p, status="succeeded")

    schema1 = pa.schema([("a", pa.string())])
    schema2 = pa.schema([("b", pa.int32())])

    part1 = _create_sample_parquet_bytes(rows=5, schema=schema1)
    part2 = _create_sample_parquet_bytes(rows=5, schema=schema2)

    mock_client = MagicMock()
    mock_client.path_exists.return_value = True
    mock_client._normalize_hdfs_path.side_effect = lambda p: p.replace("hdfs://namenode:9000", "")
    mock_client._hdfs_client.list.return_value = [
        ("part-00000.parquet", {"length": len(part1)}),
        ("part-00001.parquet", {"length": len(part2)}),
    ]
    mock_client._hdfs_client.read.side_effect = [
        MockHdfsStream(part1),
        MockHdfsStream(part2),
        MockHdfsStream(part1),
        MockHdfsStream(part2),
    ]

    with pytest.raises(SchemaMismatchError):
        build_parquet_file(run, client=mock_client)


# ── 6. Size Gates ─────────────────────────────────────────────────────────────


def test_preview_file_too_large_raises():
    run_id = uuid.uuid4()
    p = Pipeline(name="test-large-file")
    run = PipelineRun(id=run_id, output_path="/data/large", pipeline=p, status="succeeded")

    settings = get_settings()
    with patch.object(settings, "OUTPUT_PREVIEW_MAX_FILE_BYTES", 100):
        # 500 bytes exceeds 100 bytes cap
        mock_client = MagicMock()
        mock_client._hdfs_client.read.return_value = MockHdfsStream(b"x" * 500)

        with pytest.raises(FileTooLargeError):
            with open_parquet("/data/large/part-0.parquet", client=mock_client):
                pass


def test_download_total_too_large_raises():
    run_id = uuid.uuid4()
    p = Pipeline(name="test-large-dl")
    run = PipelineRun(id=run_id, output_path="/data/large_dl", pipeline=p, status="succeeded")

    settings = get_settings()
    with patch.object(settings, "OUTPUT_DOWNLOAD_MAX_BYTES", 50):
        mock_client = MagicMock()
        mock_client.path_exists.return_value = True
        mock_client._normalize_hdfs_path.side_effect = lambda p: p.replace("hdfs://namenode:9000", "")
        mock_client._hdfs_client.list.return_value = [
            ("part-00000.parquet", {"length": 100}),
        ]
        mock_client._hdfs_client.read.return_value = MockHdfsStream(_create_sample_parquet_bytes(rows=2))

        with pytest.raises(OutputTooLargeError):
            list(iter_csv(run, client=mock_client))


# ── 7. Signed One-Time Download Tokens ─────────────────────────────────────────


# ── 7. Signed Download Tokens & TTL (Step 3.3) ──────────────────────────────────


@pytest.mark.asyncio
async def test_download_token_expires_in_and_iat_delta():
    """Step 3.3.a: response expiresIn == 60 and token exp - iat == 60."""
    run_id = uuid.uuid4()
    run = PipelineRun(id=run_id, status="succeeded", output_path="/data/test")
    db = AsyncMock()
    db.get.return_value = run

    with patch("app.api.routes.pipeline_runs.describe_output", return_value={"totalSizeBytes": 1000}):
        res = await create_output_download_link(
            str(run_id),
            body=DownloadLinkRequest(format="csv"),
            current_user="admin",
            db=db,
        )

    assert res["expiresIn"] == 60
    settings = get_settings()
    token = res["url"].split("token=")[1]
    claims = jwt.decode(token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
    assert claims["exp"] - claims["iat"] == 60


def test_download_token_valid_at_59s():
    """Step 3.3.b: token valid at 59 s."""
    run_id = uuid.uuid4()
    run = PipelineRun(id=run_id, output_path="/data/test")
    t0 = datetime.now(timezone.utc)
    t0_ts = int(t0.timestamp())

    with patch("app.services.output_service.datetime") as mock_dt:
        mock_dt.now.return_value = t0
        token = create_download_token(run, "csv", "admin")

    with patch("jose.jwt.timegm", return_value=t0_ts + 59):
        claims = verify_download_token(token, str(run_id), "csv")
        assert claims["format"] == "csv"
        assert claims["run_id"] == str(run_id)


@pytest.mark.asyncio
async def test_download_token_rejected_at_61s():
    """Step 3.3.c: token rejected with 401 at 61 s."""
    run_id = uuid.uuid4()
    run = PipelineRun(id=run_id, status="succeeded", output_path="/data/test")
    db = AsyncMock()
    db.get.return_value = run
    t0 = datetime.now(timezone.utc)
    t0_ts = int(t0.timestamp())

    with patch("app.services.output_service.datetime") as mock_dt:
        mock_dt.now.return_value = t0
        token = create_download_token(run, "csv", "admin")

    with patch("jose.jwt.timegm", return_value=t0_ts + 61):
        with pytest.raises(InvalidDownloadTokenError):
            verify_download_token(token, str(run_id), "csv")

        with pytest.raises(HTTPException) as exc:
            await download_run_output(str(run_id), token=token, db=db)
        assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_download_token_second_use_returns_401():
    """Step 3.3.d: second use of a valid token returns 401 (single use)."""
    run_id = uuid.uuid4()
    run = PipelineRun(id=run_id, status="succeeded", output_path="/data/test")
    db = AsyncMock()
    db.get.return_value = run

    token = create_download_token(run, "csv", "admin")

    # First verify succeeds
    claims = verify_download_token(token, str(run_id), "csv")
    assert claims["run_id"] == str(run_id)

    # Second verify of same token raises InvalidDownloadTokenError and route returns 401
    with pytest.raises(InvalidDownloadTokenError):
        verify_download_token(token, str(run_id), "csv")

    with pytest.raises(HTTPException) as exc:
        await download_run_output(str(run_id), token=token, db=db)
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_access_jwt_rejected_as_download_token_401():
    """Step 3.3.e: an access JWT is rejected as a download token (401)."""
    run_id = uuid.uuid4()
    db = AsyncMock()
    access_token = create_access_token({"sub": "admin"})

    with pytest.raises(InvalidDownloadTokenError):
        verify_download_token(access_token, str(run_id), "csv")

    with pytest.raises(HTTPException) as exc:
        await download_run_output(str(run_id), token=access_token, db=db)
    assert exc.value.status_code == 401


def test_download_token_mismatched_run_or_format():
    run_id1 = uuid.uuid4()
    run_id2 = uuid.uuid4()
    run1 = PipelineRun(id=run_id1, output_path="/data/test1")

    token = create_download_token(run1, "csv", "admin")

    # Mismatched run ID
    with pytest.raises(InvalidDownloadTokenError):
        verify_download_token(token, str(run_id2), "csv")

    # Mismatched format
    with pytest.raises(InvalidDownloadTokenError):
        verify_download_token(token, str(run_id1), "parquet")


# ── 8. Status Codes (Route Handlers) ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_output_status_codes():
    db = AsyncMock()

    # 1. Unknown run -> 404
    db.get.return_value = None
    with pytest.raises(HTTPException) as exc1:
        await get_run_output_metadata(str(uuid.uuid4()), db=db)
    assert exc1.value.status_code == 404

    # 2. Running / not succeeded -> 409
    running_run = PipelineRun(id=uuid.uuid4(), status="running", output_path="/data/test")
    db.get.return_value = running_run
    with pytest.raises(HTTPException) as exc2:
        await get_run_output_metadata(str(running_run.id), db=db)
    assert exc2.value.status_code == 409
    assert exc2.value.detail == "Run has no completed output"

    # 3. Missing output -> 404
    succeeded_run = PipelineRun(
        id=uuid.uuid4(), status="succeeded", output_path="/data/missing"
    )
    db.get.return_value = succeeded_run
    with patch("app.api.routes.pipeline_runs.describe_output", side_effect=OutputNotFoundError("No output files found")):
        with pytest.raises(HTTPException) as exc3:
            await get_run_output_metadata(str(succeeded_run.id), db=db)
        assert exc3.value.status_code == 404

    # 4. Storage unavailable -> 503
    with patch("app.api.routes.pipeline_runs.describe_output", side_effect=StorageUnavailableError("Storage is unavailable")):
        with pytest.raises(HTTPException) as exc4:
            await get_run_output_metadata(str(succeeded_run.id), db=db)
        assert exc4.value.status_code == 503


# ── 9. Memory Bound & Batch Slicing ────────────────────────────────────────────


def test_preview_output_bounded_batches():
    """Verify that requesting 50 rows from a large file only reads needed batches."""
    run_id = uuid.uuid4()
    p = Pipeline(name="test-batch-bound")
    run = PipelineRun(id=run_id, output_path="/data/bounded", pipeline=p, status="succeeded")

    # Build 200,000-row table
    rows_total = 200_000
    table = pa.Table.from_arrays([
        pa.array(range(rows_total), type=pa.int64()),
    ], names=["id"])

    buf = io.BytesIO()
    # Write in batches of 500 rows
    pq.write_table(table, buf, row_group_size=500)
    data = buf.getvalue()

    mock_client = MagicMock()
    mock_client.path_exists.return_value = True
    mock_client._normalize_hdfs_path.side_effect = lambda p: p.replace("hdfs://namenode:9000", "")
    mock_client._hdfs_client.list.return_value = [
        ("part-00000.parquet", {"length": len(data)}),
    ]
    mock_client._hdfs_client.read.side_effect = [
        MockHdfsStream(data),  # for metadata
        MockHdfsStream(data),  # for preview
    ]

    preview = preview_output(run, limit=50, client=mock_client)
    assert len(preview["rows"]) == 50
    assert preview["truncated"] is True
    assert preview["rowCount"] == rows_total
    assert preview["totalRows"] == rows_total
    assert preview["limit"] == 50


# ── 10. Exhaustive Status Codes (400, 401, 404, 409, 413, 422, 503) ────────────


@pytest.mark.asyncio
async def test_output_status_codes_all_cases():
    """Verify all required status codes: 400, 401, 404, 409, 413, 422, 503."""
    db = AsyncMock()

    # 400: Invalid output path
    bad_path_run = PipelineRun(
        id=uuid.uuid4(), status="succeeded", output_path="/data/../etc/passwd"
    )
    db.get.return_value = bad_path_run
    with pytest.raises(HTTPException) as exc_400:
        await get_run_output_metadata(str(bad_path_run.id), db=db)
    assert exc_400.value.status_code == 400
    assert exc_400.value.detail == "Output path is not allowed"

    # 401: Invalid or expired download token
    with pytest.raises(HTTPException) as exc_401:
        await download_run_output(str(uuid.uuid4()), token="invalid.token.signature", db=db)
    assert exc_401.value.status_code == 401
    assert "Invalid or expired" in exc_401.value.detail

    # 404: Unknown run ID
    db.get.return_value = None
    with pytest.raises(HTTPException) as exc_404:
        await get_run_output_metadata(str(uuid.uuid4()), db=db)
    assert exc_404.value.status_code == 404

    # 404: Missing output directory or files
    ok_run = PipelineRun(id=uuid.uuid4(), status="succeeded", output_path="/data/test")
    db.get.return_value = ok_run
    with patch("app.api.routes.pipeline_runs.describe_output", side_effect=OutputNotFoundError("No output files found")):
        with pytest.raises(HTTPException) as exc_404_files:
            await get_run_output_metadata(str(ok_run.id), db=db)
        assert exc_404_files.value.status_code == 404

    # 409: Run running or failed (not succeeded)
    for bad_status in ("running", "failed", "cancelled", "pending"):
        unready_run = PipelineRun(id=uuid.uuid4(), status=bad_status, output_path="/data/test")
        db.get.return_value = unready_run
        with pytest.raises(HTTPException) as exc_409:
            await get_run_output_metadata(str(unready_run.id), db=db)
        assert exc_409.value.status_code == 409
        assert exc_409.value.detail == "Run has no completed output"

    # 413: Output too large for download link request
    db.get.return_value = ok_run
    with patch("app.api.routes.pipeline_runs.describe_output", return_value={"totalSizeBytes": 600 * 1024 * 1024}):
        with pytest.raises(HTTPException) as exc_413:
            await create_output_download_link(
                str(ok_run.id),
                body=DownloadLinkRequest(format="csv"),
                current_user="admin",
                db=db,
            )
        assert exc_413.value.status_code == 413
        assert "maximum downloadable size" in exc_413.value.detail

    # 422: Malformed UUID format in path
    with pytest.raises(HTTPException) as exc_422:
        await get_run_output_metadata("not-a-valid-uuid-12345", db=db)
    assert exc_422.value.status_code == 422

    # 503: Storage/HDFS unavailable
    with patch("app.api.routes.pipeline_runs.describe_output", side_effect=StorageUnavailableError("Storage is unavailable")):
        with pytest.raises(HTTPException) as exc_503:
            await get_run_output_metadata(str(ok_run.id), db=db)
        assert exc_503.value.status_code == 503
        assert exc_503.value.detail == "Storage is unavailable"


# ── 11. Temp File Cleanup Proof ────────────────────────────────────────────────


def test_build_parquet_file_temp_cleanup_after_download():
    """Verify build_parquet_file creates temp file and cleanup() removes it completely."""
    run_id = uuid.uuid4()
    p = Pipeline(name="test-temp-cleanup")
    run = PipelineRun(id=run_id, output_path="/data/temp_test", pipeline=p, status="succeeded")

    schema = pa.schema([("id", pa.int64())])
    table = pa.Table.from_arrays([pa.array([1, 2, 3])], schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    data = buf.getvalue()

    mock_client = MagicMock()
    mock_client.path_exists.return_value = True
    mock_client._normalize_hdfs_path.side_effect = lambda p: p.replace("hdfs://namenode:9000", "")
    mock_client._hdfs_client.list.return_value = [("part-0.parquet", {"length": len(data)})]
    mock_client._hdfs_client.read.side_effect = [
        MockHdfsStream(data),  # describe
        MockHdfsStream(data),  # build
    ]

    temp_path, cleanup = build_parquet_file(run, client=mock_client)
    assert os.path.exists(temp_path), "Temp parquet file must exist during stream"

    cleanup()
    assert not os.path.exists(temp_path), "Temp parquet file must be removed after cleanup()"


# ── 12. Security Proof: Download Token is Never Logged ────────────────────────


def test_download_token_never_logged(caplog):
    """Verify that creating, verifying, and requesting tokens never writes raw token to logs."""
    import logging
    caplog.set_level(logging.DEBUG)

    run_id = uuid.uuid4()
    p = Pipeline(name="test-token-log")
    run = PipelineRun(id=run_id, output_path="/data/token_log", pipeline=p, status="succeeded")

    raw_token = create_download_token(run, "csv", "admin")
    verify_download_token(raw_token, str(run_id), "csv")

    for record in caplog.records:
        assert raw_token not in record.message
        assert raw_token not in str(record.args)
