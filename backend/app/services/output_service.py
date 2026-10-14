"""
app/services/output_service.py
------------------------------
Service for pipeline output inspection, metadata extraction, preview generation,
and bounded CSV/Parquet downloads.
"""

from __future__ import annotations

import io
import json
import logging
import math
import os
import re
import tempfile
import time
import urllib.parse
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Generator, Iterator

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
from jose import JWTError, jwt

from app.core.config import get_settings
from app.models.pipeline_run import PipelineRun
from app.storage.hdfs_client import HdfsStorageClient

logger = logging.getLogger(__name__)


# ── Exceptions ─────────────────────────────────────────────────────────────────


class OutputServiceError(Exception):
    """Base exception for output service."""


class InvalidOutputPathError(OutputServiceError):
    """Raised when an output path violates security or prefix boundaries."""


class RunNotReadyError(OutputServiceError):
    """Raised when an output is requested for a non-succeeded run."""


class OutputNotFoundError(OutputServiceError):
    """Raised when no output files or directory exist on storage."""


class FileTooLargeError(OutputServiceError):
    """Raised when a single part file exceeds the maximum allowed preview size."""


class OutputTooLargeError(OutputServiceError):
    """Raised when total output size exceeds the download threshold."""


class StorageUnavailableError(OutputServiceError):
    """Raised when HDFS / storage backend cannot be reached."""


class InvalidDownloadTokenError(OutputServiceError):
    """Raised when a download token is missing, expired, tampered, or already used."""


class SchemaMismatchError(OutputServiceError):
    """Raised when output part files have conflicting schemas."""


# ── In-memory state & caches ───────────────────────────────────────────────────

# Cache: run_id -> (expiry_monotonic_time, metadata_dict)
_METADATA_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_METADATA_CACHE_TTL = 30.0  # seconds

# Token JTI replay protection: jti -> insertion_time
_USED_JTIS: dict[str, float] = {}
_JTI_EXPIRY_SECONDS = 300.0  # 5 minutes retention


def _clean_expired_jtis() -> None:
    now = time.time()
    expired = [jti for jti, ts in _USED_JTIS.items() if now - ts > _JTI_EXPIRY_SECONDS]
    for jti in expired:
        _USED_JTIS.pop(jti, None)


# ── Path validation ────────────────────────────────────────────────────────────


def resolve_output_path(run: PipelineRun) -> str:
    """
    Derive the HDFS output path strictly from the run record.

    Enforces:
    - Must start with 'hdfs://namenode:9000/' or '/'
    - After normalization must start with an allowed prefix:
      '/data/' or '/tmp/spark-output-<run_id>'
    - Must not contain '..', backslashes, NUL, or URL-encoded traversal
    - Must not equal an allowed prefix root itself (e.g. '/data/' or '/data')
    """
    settings = get_settings()
    raw_path = getattr(run, "actual_output_path", None) or run.output_path
    if not raw_path or not isinstance(raw_path, str):
        logger.warning("Invalid output path for run %s: path is empty", run.id)
        raise InvalidOutputPathError("Output path is not allowed")

    # Reject backslash, NUL, and direct '..'
    if "\\" in raw_path or "\0" in raw_path or ".." in raw_path:
        logger.warning("Invalid output path for run %s: contains illegal characters", run.id)
        raise InvalidOutputPathError("Output path is not allowed")

    # URL decoding traversal checks (%2e%2e, %2f, etc.)
    unquoted = urllib.parse.unquote(raw_path)
    if ".." in unquoted or "\\" in unquoted or "\0" in unquoted:
        logger.warning("Invalid output path for run %s: url-encoded traversal detected", run.id)
        raise InvalidOutputPathError("Output path is not allowed")

    # Scheme / prefix check
    hdfs_prefix = f"{settings.HDFS_NAMENODE_URL}/"  # e.g. http://namenode:9870/ or hdfs://namenode:9000/
    allowed_uri_prefixes = ("hdfs://namenode:9000/", "hdfs://", "/")
    if not any(raw_path.startswith(p) for p in allowed_uri_prefixes):
        logger.warning("Invalid output path for run %s: rejected prefix", run.id)
        raise InvalidOutputPathError("Output path is not allowed")

    # Normalize path (strip scheme and host)
    norm = HdfsStorageClient._normalize_hdfs_path(raw_path)
    if not norm.startswith("/"):
        norm = f"/{norm}"

    # Re-verify normalized path for double slashes or traversal
    if "/../" in norm or norm.endswith("/..") or "//" in norm:
        logger.warning("Invalid output path for run %s: traversal in normalized path", run.id)
        raise InvalidOutputPathError("Output path is not allowed")

    # Must not be the root or base prefix
    if norm in ("/", "/data", "/data/", "/tmp", "/tmp/"):
        logger.warning("Invalid output path for run %s: path equals base prefix root", run.id)
        raise InvalidOutputPathError("Output path is not allowed")

    run_id_str = str(run.id)
    allowed_tmp_prefix = f"/tmp/spark-output-{run_id_str}"
    
    # Must start with /data/ or /tmp/spark-output-<run_id>
    if norm.startswith("/data/"):
        if norm == "/data/":
            logger.warning("Invalid output path for run %s: equals /data/", run.id)
            raise InvalidOutputPathError("Output path is not allowed")
    elif norm.startswith(allowed_tmp_prefix):
        if norm == allowed_tmp_prefix or norm == f"{allowed_tmp_prefix}/":
            pass  # The specific run's spark output folder is allowed
    else:
        logger.warning("Invalid output path for run %s: prefix not in allowed list", run.id)
        raise InvalidOutputPathError("Output path is not allowed")

    return norm


# ── Parquet reading & footer parsing ──────────────────────────────────────────


@contextmanager
def open_parquet(norm_path: str, client: HdfsStorageClient | None = None) -> Generator[pq.ParquetFile, None, None]:
    """
    Stream a part file from WebHDFS in 1 MiB chunks into a temporary file.

    Aborts and raises FileTooLargeError if size exceeds OUTPUT_PREVIEW_MAX_FILE_BYTES.
    Guarantees the temporary file is unlinked on exit.
    """
    settings = get_settings()
    max_bytes = settings.OUTPUT_PREVIEW_MAX_FILE_BYTES
    hdfs = client or HdfsStorageClient()

    tmp_file = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp_path = tmp_file.name
    total_bytes = 0

    try:
        try:
            # hdfs InsecureClient.read yields chunks when chunk_size is provided, or a file-like object
            with hdfs._hdfs_client.read(norm_path, chunk_size=1024 * 1024) as reader:
                chunks = reader if not hasattr(reader, "read") else iter(lambda: reader.read(1024 * 1024), b"")
                for chunk in chunks:
                    total_bytes += len(chunk)
                    if total_bytes > max_bytes:
                        raise FileTooLargeError(
                            f"Part file exceeds preview limit of {max_bytes // (1024 * 1024)} MiB"
                        )
                    tmp_file.write(chunk)
            tmp_file.flush()
            tmp_file.close()
        except FileTooLargeError:
            raise
        except Exception as exc:
            logger.warning("Failed streaming HDFS file %s: %s", norm_path, exc)
            raise StorageUnavailableError("Storage is unavailable") from exc

        pq_file = pq.ParquetFile(tmp_path)
        try:
            yield pq_file
        finally:
            try:
                pq_file.close()
            except Exception:
                pass
    finally:
        if not tmp_file.closed:
            try:
                tmp_file.close()
            except Exception:
                pass
        if os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


def map_arrow_type_to_simple(arrow_type: pa.DataType) -> str:
    """Map PyArrow data types to simple frontend type names."""
    s = str(arrow_type).lower()
    if "string" in s or "utf8" in s or "large_string" in s:
        return "string"
    if any(s.startswith(x) for x in ("int8", "int16", "int32", "uint8", "uint16", "uint32")):
        return "int32"
    if any(s.startswith(x) for x in ("int64", "uint64")):
        return "int64"
    if any(s.startswith(x) for x in ("double", "float", "halffloat")):
        return "double"
    if "bool" in s:
        return "bool"
    if "timestamp" in s or "time" in s:
        return "timestamp"
    if "date" in s:
        return "date"
    if "decimal" in s:
        return "decimal"
    return "other"


# ── Metadata extraction ────────────────────────────────────────────────────────


def describe_output(run: PipelineRun, client: HdfsStorageClient | None = None) -> dict[str, Any]:
    """
    Query WebHDFS to describe output part files, schema, and row count.

    Caches the result per run ID for 30 seconds.
    """
    now = time.monotonic()
    run_id_str = str(run.id)
    cached = _METADATA_CACHE.get(run_id_str)
    if cached and cached[0] > now:
        return cached[1]

    norm_dir = resolve_output_path(run)
    hdfs = client or HdfsStorageClient()

    try:
        if not hdfs.path_exists(norm_dir):
            raise OutputNotFoundError("No output files found")

        # Query directory listing with status
        entries = hdfs._hdfs_client.list(norm_dir, status=True)
    except OutputNotFoundError:
        raise
    except Exception as exc:
        logger.warning("Error accessing HDFS directory %s for run %s: %s", norm_dir, run.id, exc)
        raise StorageUnavailableError("Storage is unavailable") from exc

    # Filter part files, ignore _SUCCESS, _temporary, .crc, hidden files
    part_files = []
    total_size = 0
    for name, stat in entries:
        if name.startswith("_") or name.startswith("."):
            continue
        if not name.endswith(".parquet"):
            continue
        size = int(stat.get("length", 0))
        part_files.append({"name": name, "sizeBytes": size})
        total_size += size

    part_files.sort(key=lambda x: x["name"])

    if not part_files:
        raise OutputNotFoundError("No output files found")

    # Read schema and row counts from Parquet footers
    total_rows: int | None = 0
    columns: list[dict[str, str]] = []

    for idx, f in enumerate(part_files):
        f_path = f"{norm_dir.rstrip('/')}/{f['name']}"
        try:
            with open_parquet(f_path, client=hdfs) as pq_file:
                if idx == 0:
                    for field in pq_file.schema_arrow:
                        columns.append({
                            "name": field.name,
                            "type": map_arrow_type_to_simple(field.type),
                        })
                num_rows = pq_file.metadata.num_rows
                if total_rows is not None:
                    total_rows += num_rows
        except Exception as exc:
            logger.debug("Failed to read footer from part file %s: %s", f_path, exc)
            total_rows = None

    # Fallback for row count if footers failed
    if total_rows is None:
        snap = getattr(run, "last_progress_snapshot", None) or {}
        if isinstance(snap, dict):
            total_rows = (
                snap.get("spark", {}).get("recordsOut")
                or snap.get("hdfs", {}).get("rowsWritten")
            )
        if total_rows is None:
            if getattr(run, "spark_records_out", None) is not None:
                total_rows = run.spark_records_out
            elif getattr(run, "storage_metrics", None) is not None and isinstance(run.storage_metrics, int):
                total_rows = run.storage_metrics
            elif getattr(run, "spark_metrics", None) and isinstance(run.spark_metrics, dict):
                total_rows = run.spark_metrics.get("output_rows")

    verified = True
    if hasattr(run, "hdfs_verified") and run.hdfs_verified is not None:
        verified = bool(run.hdfs_verified)

    metadata = {
        "path": f"hdfs://namenode:9000{norm_dir}",
        "format": "parquet",
        "files": part_files,
        "fileCount": len(part_files),
        "totalSizeBytes": total_size,
        "rowCount": total_rows,
        "columns": columns,
        "verified": verified,
    }

    _METADATA_CACHE[run_id_str] = (now + _METADATA_CACHE_TTL, metadata)
    return metadata


# ── Value sanitization for JSON preview ────────────────────────────────────────


def _to_json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, bytes):
        hex_str = value[:64].hex()
        return hex_str + ("…" if len(value) > 64 else "")
    if isinstance(value, (list, dict)):
        try:
            s = json.dumps(value, default=str)
            return s[:500]
        except Exception:
            return str(value)[:500]
    if isinstance(value, str):
        if len(value) > 2000:
            return value[:2000] + "…"
        return value
    return value


# ── Preview generation ─────────────────────────────────────────────────────────


def preview_output(
    run: PipelineRun,
    limit: int = 50,
    client: HdfsStorageClient | None = None,
) -> dict[str, Any]:
    """
    Read up to limit rows across part files and return JSON-safe rows.
    """
    settings = get_settings()
    if limit < 1 or limit > settings.OUTPUT_PREVIEW_MAX_ROWS:
        raise ValueError(f"Limit must be between 1 and {settings.OUTPUT_PREVIEW_MAX_ROWS}")

    meta = describe_output(run, client=client)
    norm_dir = resolve_output_path(run)
    hdfs = client or HdfsStorageClient()

    rows: list[list[Any]] = []
    files_read = 0
    truncated = False

    for file_info in meta["files"]:
        if len(rows) >= limit:
            break
        file_path = f"{norm_dir.rstrip('/')}/{file_info['name']}"
        with open_parquet(file_path, client=hdfs) as pq_file:
            files_read += 1
            needed = limit - len(rows)
            for batch in pq_file.iter_batches(batch_size=needed):
                batch_dict = batch.to_pydict()
                col_names = [c["name"] for c in meta["columns"]]
                batch_len = len(batch)
                for row_idx in range(batch_len):
                    if len(rows) >= limit:
                        break
                    row_vals = [
                        _to_json_safe(batch_dict[c][row_idx]) for c in col_names
                    ]
                    rows.append(row_vals)
                if len(rows) >= limit:
                    break

    if meta["rowCount"] is not None:
        truncated = meta["rowCount"] > len(rows)
    else:
        truncated = len(rows) == limit

    return {
        "columns": meta["columns"],
        "rows": rows,
        "rowCount": meta["rowCount"],
        "totalRows": meta["rowCount"],
        "limit": limit,
        "truncated": truncated,
        "filesRead": files_read,
    }


# ── Download streaming (CSV & Parquet) ─────────────────────────────────────────


def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_-]", "-", name.lower()).strip("-")
    return cleaned or "pipeline-output"


def iter_csv(run: PipelineRun, client: HdfsStorageClient | None = None) -> Iterator[bytes]:
    """
    Stream output as CSV in batches without buffering the whole file in memory.
    """
    settings = get_settings()
    meta = describe_output(run, client=client)
    if meta["totalSizeBytes"] > settings.OUTPUT_DOWNLOAD_MAX_BYTES:
        mb_total = round(meta["totalSizeBytes"] / (1024 * 1024), 1)
        mb_max = round(settings.OUTPUT_DOWNLOAD_MAX_BYTES / (1024 * 1024), 1)
        raise OutputTooLargeError(
            f"Output is {mb_total} MB; maximum downloadable size is {mb_max} MB. Read it from HDFS directly."
        )

    norm_dir = resolve_output_path(run)
    hdfs = client or HdfsStorageClient()
    header_written = False

    for file_info in meta["files"]:
        file_path = f"{norm_dir.rstrip('/')}/{file_info['name']}"
        with open_parquet(file_path, client=hdfs) as pq_file:
            for batch in pq_file.iter_batches(batch_size=10_000):
                buf = io.BytesIO()
                opts = pcsv.WriteOptions(include_header=not header_written)
                pcsv.write_csv(batch, buf, write_options=opts)
                header_written = True
                yield buf.getvalue()


def build_parquet_file(run: PipelineRun, client: HdfsStorageClient | None = None) -> tuple[str, Callable[[], None]]:
    """
    Build a single combined Parquet file from all part files and return its path and cleanup callback.
    """
    settings = get_settings()
    meta = describe_output(run, client=client)
    if meta["totalSizeBytes"] > settings.OUTPUT_DOWNLOAD_MAX_BYTES:
        mb_total = round(meta["totalSizeBytes"] / (1024 * 1024), 1)
        mb_max = round(settings.OUTPUT_DOWNLOAD_MAX_BYTES / (1024 * 1024), 1)
        raise OutputTooLargeError(
            f"Output is {mb_total} MB; maximum downloadable size is {mb_max} MB. Read it from HDFS directly."
        )

    norm_dir = resolve_output_path(run)
    hdfs = client or HdfsStorageClient()

    temp_out = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    temp_path = temp_out.name
    temp_out.close()

    writer: pq.ParquetWriter | None = None
    expected_schema = None

    try:
        for file_info in meta["files"]:
            file_path = f"{norm_dir.rstrip('/')}/{file_info['name']}"
            with open_parquet(file_path, client=hdfs) as pq_file:
                if writer is None:
                    expected_schema = pq_file.schema_arrow
                    writer = pq.ParquetWriter(temp_path, expected_schema)
                else:
                    if not pq_file.schema_arrow.equals(expected_schema):
                        raise SchemaMismatchError("Part files have different schemas")

                for batch in pq_file.iter_batches(batch_size=10_000):
                    writer.write_batch(batch)
    except Exception:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        if os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except Exception:
                pass
        raise
    finally:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass

    def cleanup() -> None:
        if os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except Exception:
                pass

    return temp_path, cleanup


# ── Signed Download Tokens ─────────────────────────────────────────────────────


def create_download_token(run: PipelineRun, format_type: str, username: str) -> str:
    """Generate a short-lived (60 s) signed single-use download token."""
    settings = get_settings()
    now = datetime.now(timezone.utc)
    iat = int(now.timestamp())
    exp = iat + settings.OUTPUT_DOWNLOAD_TOKEN_TTL_SECONDS
    claims = {
        "sub": username,
        "purpose": "output_download",
        "run_id": str(run.id),
        "format": format_type,
        "jti": str(uuid.uuid4()),
        "iat": iat,
        "exp": exp,
    }
    return jwt.encode(claims, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def verify_download_token(token: str, expected_run_id: str, expected_format: str | None = None) -> dict[str, Any]:
    """
    Validate signature, purpose, run_id, expiry, and single-use jti.
    """
    settings = get_settings()
    _clean_expired_jtis()

    if not token:
        raise InvalidDownloadTokenError("Missing download token.")

    try:
        payload = jwt.decode(
            token,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM],
        )
    except JWTError as exc:
        raise InvalidDownloadTokenError("Invalid or expired download token.") from exc

    if payload.get("purpose") != "output_download":
        raise InvalidDownloadTokenError("Invalid token purpose.")

    if payload.get("run_id") != expected_run_id:
        raise InvalidDownloadTokenError("Token run ID mismatch.")

    if expected_format and payload.get("format") != expected_format:
        raise InvalidDownloadTokenError("Token format mismatch.")

    jti = payload.get("jti")
    if not jti or jti in _USED_JTIS:
        raise InvalidDownloadTokenError("Download link has already been used.")

    # Mark jti as consumed immediately
    _USED_JTIS[jti] = time.time()
    return payload
