"""
app/reliability/data_consistency_validator.py
----------------------------------------------
Validates that pipeline output data is complete and internally consistent
after a chaos experiment.

The validator reads back the actual pipeline output using the Phase 3/6
storage plugin registry (local or HDFS) — the same plugins the pipeline
executor used to write the data — ensuring the validation path is identical
to the production write path.

Consistency checks performed:
1. **Record count** — actual vs. expected, computes data_loss and data_loss_%.
2. **Duplicate ID detection** — if the schema contains a column whose name
   ends in ``_id`` (heuristic for unique identifier columns), counts
   duplicate values and reports them as a secondary consistency signal.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class DataConsistencyValidator:
    """
    Read back pipeline output and validate record count and uniqueness.

    Uses the storage plugin registry so that local, HDFS, and future storage
    backends are supported automatically without modifying this class.
    """

    def validate(
        self,
        expected_records: int | None,
        output_location: dict,
        source_location: dict | str | None = None,
        filter_accounting: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        """
        Read the pipeline output and verify data completeness and consistency.

        Parameters
        ----------
        expected_records : int | None
            Expected number of records (from the baseline capture or raw source count).
        output_location : dict
            Describes where to find the output.
            Must contain at least:
            - ``"storage_type"`` (str): ``"local"`` or ``"hdfs"``
            - ``"path"`` (str): file or directory path
        source_location : dict | str | None
            Optional input source location to distinguish pre-existing source
            duplicates from fault-induced duplicates.
        filter_accounting : dict[str, int] | None
            Optional transformation filter counts (e.g. ``{"null_filtered": 5124, "condition_filtered": 5132}``).
            When provided, effective expected records are computed as:
            ``max(0, expected_records - sum(filter_accounting.values()))``,
            preventing false positive data loss from intentional pipeline filtering.

        Returns
        -------
        dict
            ``{"expected_records": int, "actual_records": int,
            "data_loss": int, "data_loss_percentage": float,
            "duplicate_ids_found": int, "duplicates": int,
            "output_duplicates": int, "source_duplicates": int,
            "id_column": str | None, "validation_passed": bool}``
        """
        storage_type = output_location.get("storage_type", "local")
        path = output_location.get("path", "")

        # Compute effective expected records accounting for transformation filters
        total_filtered = 0
        if filter_accounting is not None:
            if not isinstance(filter_accounting, dict):
                raise TypeError("filter_accounting must be a dictionary")
            for key, count in filter_accounting.items():
                if isinstance(count, bool) or not isinstance(count, (int, float)):
                    raise TypeError(f"filter_accounting value for '{key}' must be numeric, got {type(count).__name__}")
                if count < 0:
                    raise ValueError(f"filter_accounting value for '{key}' cannot be negative, got {count}")
                total_filtered += int(count)

            if expected_records is not None and total_filtered > expected_records:
                logger.warning(
                    "DataConsistencyValidator: total filtered records (%d) exceeds raw expected (%d); clamping effective expected to 0",
                    total_filtered,
                    expected_records,
                )
            effective_expected = (
                max(0, expected_records - total_filtered)
                if expected_records is not None
                else None
            )
        else:
            effective_expected = expected_records

        # ── Read actual output via storage plugin ──────────────────────────────
        try:
            df = self._read_output(storage_type, path)
            actual_records = len(df)
        except Exception as exc:
            logger.error(
                "DataConsistencyValidator: failed to read output at '%s': %s",
                path,
                exc,
            )
            return {
                "expected_records": effective_expected,
                "raw_input_records": expected_records if filter_accounting else None,
                "filtered_records": total_filtered if filter_accounting else 0,
                "filter_accounting": filter_accounting or {},
                "actual_records": 0,
                "data_loss": effective_expected,
                "unexpected_loss": effective_expected,
                "data_loss_percentage": 100.0 if effective_expected else 0.0,
                "duplicate_ids_found": 0,
                "duplicates": 0,
                "output_duplicates": 0,
                "source_duplicates": 0,
                "id_column": None,
                "validation_passed": False,
                "read_error": str(exc),
            }

        # ── Record count check ─────────────────────────────────────────────────
        if effective_expected is not None and actual_records is not None:
            data_loss = max(0, effective_expected - actual_records)
            data_loss_pct = (
                round(data_loss / effective_expected * 100.0, 4)
                if effective_expected > 0
                else 0.0
            )
        else:
            data_loss = None
            data_loss_pct = None

        # ── Duplicate ID detection (output vs source baseline) ─────────────────
        id_column, output_duplicates = self._find_duplicates(df)

        source_duplicates = None
        if source_location is not None:
            try:
                if isinstance(source_location, str):
                    s_type = "hdfs" if (source_location.startswith("hdfs://") or source_location.startswith("/data")) else "local"
                    s_path = source_location
                else:
                    s_type = source_location.get("storage_type", "local")
                    s_path = source_location.get("path", "")
                if s_path:
                    source_df = self._read_output(s_type, s_path)
                    _, source_duplicates = self._find_duplicates(source_df)
            except Exception as exc:
                logger.debug(
                    "DataConsistencyValidator: could not inspect source duplicates at '%s': %s",
                    source_location,
                    exc,
                )
                source_duplicates = None
        else:
            # When no source location is provided (e.g. basic unit test on output alone),
            # assume 0 pre-existing source duplicates so output_duplicates equals fault_induced_duplicates
            source_duplicates = 0

        if output_duplicates is not None and source_duplicates is not None:
            fault_induced_duplicates = max(0, output_duplicates - source_duplicates)
        else:
            fault_induced_duplicates = None

        if data_loss is not None and fault_induced_duplicates is not None:
            validation_passed = data_loss == 0 and fault_induced_duplicates == 0
        elif data_loss is not None:
            validation_passed = data_loss == 0
        else:
            validation_passed = False

        result = {
            "expected_records": effective_expected,
            "raw_input_records": expected_records if filter_accounting else None,
            "filtered_records": total_filtered if filter_accounting else 0,
            "filter_accounting": filter_accounting or {},
            "actual_records": actual_records,
            "data_loss": data_loss,
            "unexpected_loss": data_loss,
            "data_loss_percentage": data_loss_pct,
            "duplicate_ids_found": fault_induced_duplicates,
            "duplicates": fault_induced_duplicates,
            "fault_induced_duplicates": fault_induced_duplicates,
            "output_duplicates": output_duplicates,
            "source_duplicates": source_duplicates,
            "id_column": id_column,
            "validation_passed": validation_passed,
        }
        logger.info("DataConsistencyValidator: result = %s", result)
        return result

    def _read_output(self, storage_type: str, path: str):
        """
        Load the pipeline output as a pandas DataFrame using the plugin registry.

        Falls back to pandas.read_parquet for local files if the plugin fails.
        """
        import app.plugins  # noqa: F401 — ensure all plugins registered
        from app.plugins.registry import registry

        if storage_type == "hdfs" or path.startswith("hdfs://") or path.startswith("/data"):
            from app.storage.hdfs_client import HdfsStorageClient
            client = HdfsStorageClient()
            return client.read_dataframe(path)

        try:
            plugin = registry.get_storage(storage_type)
            if hasattr(plugin, "read_dataframe"):
                return plugin.read_dataframe(path)
        except Exception:
            pass

        # Fallback: local parquet read
        import glob
        import os
        import pandas as pd

        if os.path.isdir(path):
            # LocalStorage writes data.parquet; Spark writes part-*.parquet.
            parquet_files = [f for f in glob.glob(os.path.join(path, "*.parquet")) if os.path.getsize(f) > 0]
            if not parquet_files:
                raise FileNotFoundError(f"No valid Parquet output found in '{path}'.")
            if len(parquet_files) == 1:
                return pd.read_parquet(parquet_files[0])
            return pd.concat(
                (pd.read_parquet(parquet_file) for parquet_file in parquet_files),
                ignore_index=True,
            )
        else:
            if path.endswith(".csv"):
                return pd.read_csv(path)
            return pd.read_parquet(path)

    def _find_duplicates(self, df) -> tuple[str | None, int]:
        """
        Detect the first ``*_id`` column and count duplicate values.

        Returns
        -------
        tuple[str | None, int]
            ``(id_column_name, duplicate_count)``
            ``(None, 0)`` if no id column found.
        """
        if df is None or len(df) == 0:
            return None, 0

        id_columns = [col for col in df.columns if col.lower().endswith("_id")]
        if not id_columns:
            return None, 0

        id_col = id_columns[0]
        try:
            duplicate_count = int(df[id_col].duplicated().sum())
            if duplicate_count > 0:
                logger.warning(
                    "DataConsistencyValidator: found %d duplicate values in "
                    "column '%s'.",
                    duplicate_count,
                    id_col,
                )
            return id_col, duplicate_count
        except Exception as exc:
            logger.debug(
                "DataConsistencyValidator: duplicate check failed for '%s': %s",
                id_col,
                exc,
            )
            return id_col, 0
