"""
tests/reliability/test_data_consistency_validator.py
------------------------------------------------------
Unit tests for DataConsistencyValidator.

Uses temporary Parquet files as fixtures — no live HDFS or storage plugin
required.  Tests verify:
- Correct data_loss computation
- Correct data_loss_percentage
- Duplicate ID detection in *_id columns
- Correct result when actual == expected (no loss)
- Handles empty DataFrames gracefully
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from app.reliability.data_consistency_validator import DataConsistencyValidator


@pytest.fixture
def validator() -> DataConsistencyValidator:
    """Return a DataConsistencyValidator instance."""
    return DataConsistencyValidator()


@pytest.fixture
def parquet_fixture(tmp_path: Path):
    """
    Write a small Parquet file to tmp_path and return helper callables.
    """
    def _write(df: pd.DataFrame, filename: str = "data.parquet") -> str:
        path = str(tmp_path / filename)
        df.to_parquet(path, index=False)
        return path

    return _write


class TestValidateRecordCount:
    """Data loss computation tests."""

    def test_no_loss_when_counts_match(
        self, validator, parquet_fixture
    ) -> None:
        """validation_passed must be True when actual == expected."""
        df = pd.DataFrame({"transaction_id": [1, 2, 3], "amount": [10, 20, 30]})
        path = parquet_fixture(df)
        result = validator.validate(
            expected_records=3,
            output_location={"storage_type": "local", "path": path},
        )
        assert result["data_loss"] == 0
        assert result["data_loss_percentage"] == 0.0
        assert result["actual_records"] == 3
        assert result["validation_passed"] is True

    def test_detects_data_loss(self, validator, parquet_fixture) -> None:
        """data_loss must equal expected - actual when actual < expected."""
        df = pd.DataFrame({"transaction_id": [1, 2], "amount": [10, 20]})
        path = parquet_fixture(df)
        result = validator.validate(
            expected_records=5,
            output_location={"storage_type": "local", "path": path},
        )
        assert result["data_loss"] == 3
        assert result["data_loss_percentage"] == 60.0
        assert result["validation_passed"] is False

    def test_no_data_loss_when_actual_exceeds_expected(
        self, validator, parquet_fixture
    ) -> None:
        """data_loss must be 0 (not negative) when actual > expected."""
        df = pd.DataFrame({"col": range(10)})
        path = parquet_fixture(df)
        result = validator.validate(
            expected_records=5,
            output_location={"storage_type": "local", "path": path},
        )
        assert result["data_loss"] == 0

    def test_handles_file_not_found_gracefully(self, validator) -> None:
        """Missing output file must return 100% data loss, not raise."""
        result = validator.validate(
            expected_records=100,
            output_location={"storage_type": "local", "path": "/nonexistent/path.parquet"},
        )
        assert result["data_loss"] == 100
        assert result["data_loss_percentage"] == 100.0
        assert result["validation_passed"] is False
        assert "read_error" in result


class TestDuplicateIdDetection:
    """Duplicate ID detection in *_id columns."""

    def test_detects_no_duplicates(self, validator, parquet_fixture) -> None:
        """No duplicate IDs → duplicate_ids_found == 0."""
        df = pd.DataFrame({
            "transaction_id": ["t1", "t2", "t3"],
            "amount": [10, 20, 30],
        })
        path = parquet_fixture(df)
        result = validator.validate(
            expected_records=3,
            output_location={"storage_type": "local", "path": path},
        )
        assert result["duplicate_ids_found"] == 0
        assert result["id_column"] == "transaction_id"

    def test_detects_duplicates(self, validator, parquet_fixture) -> None:
        """Duplicate IDs must be counted and reported."""
        df = pd.DataFrame({
            "transaction_id": ["t1", "t1", "t2"],  # t1 appears twice
            "amount": [10, 20, 30],
        })
        path = parquet_fixture(df)
        result = validator.validate(
            expected_records=3,
            output_location={"storage_type": "local", "path": path},
        )
        assert result["duplicate_ids_found"] == 1
        assert result["id_column"] == "transaction_id"
        # Duplicates make validation fail
        assert result["validation_passed"] is False

    def test_no_id_column_returns_zero_duplicates(
        self, validator, parquet_fixture
    ) -> None:
        """When no *_id column exists, duplicate_ids_found must be 0."""
        df = pd.DataFrame({"name": ["alice", "bob"], "score": [10, 20]})
        path = parquet_fixture(df)
        result = validator.validate(
            expected_records=2,
            output_location={"storage_type": "local", "path": path},
        )
        assert result["duplicate_ids_found"] == 0
        assert result["id_column"] is None

    def test_empty_dataframe_handled(self, validator, parquet_fixture) -> None:
        """Empty DataFrame must return 0 duplicates without raising."""
        df = pd.DataFrame({"transaction_id": pd.Series([], dtype="str")})
        path = parquet_fixture(df)
        result = validator.validate(
            expected_records=0,
            output_location={"storage_type": "local", "path": path},
        )
        assert result["duplicate_ids_found"] == 0
        assert result["actual_records"] == 0

    def test_uses_directory_with_data_parquet(
        self, validator, tmp_path: Path
    ) -> None:
        """When path is a directory, reads data.parquet inside it."""
        df = pd.DataFrame({"order_id": ["o1", "o2"], "value": [5, 10]})
        (tmp_path / "data.parquet").parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(str(tmp_path / "data.parquet"), index=False)

        result = validator.validate(
            expected_records=2,
            output_location={"storage_type": "local", "path": str(tmp_path)},
        )
        assert result["actual_records"] == 2
        assert result["data_loss"] == 0

    def test_uses_spark_part_files_in_directory(
        self, validator, tmp_path: Path
    ) -> None:
        """Spark output directories contain part files rather than data.parquet."""
        pd.DataFrame({"order_id": ["o1", "o2"]}).to_parquet(
            tmp_path / "part-00000.parquet", index=False
        )
        pd.DataFrame({"order_id": ["o3"]}).to_parquet(
            tmp_path / "part-00001.parquet", index=False
        )

        result = validator.validate(
            expected_records=4,
            output_location={"storage_type": "local", "path": str(tmp_path)},
        )

        assert result["actual_records"] == 3
        assert result["data_loss"] == 1
        assert result["validation_passed"] is False


class TestStreamingDataConsistencyValidation:
    """Step 8.3: Data consistency validation for streaming runs."""

    def test_streaming_consistency_expected_equals_kafka_published_and_processed_equals_output_rows(
        self, validator, tmp_path: Path
    ) -> None:
        """
        Step 8.3 requirement:
        For streaming runs, expectedRecords equals kafka.published and
        processedRecords equals rows read back from the output directory.
        """
        kafka_published = 100_000
        # Simulate Spark Parquet output written to directory
        df1 = pd.DataFrame({"transaction_id": [f"T{i}" for i in range(50_000)], "amount": [10.0] * 50_000})
        df2 = pd.DataFrame({"transaction_id": [f"T{i}" for i in range(50_000, 100_000)], "amount": [20.0] * 50_000})
        df1.to_parquet(tmp_path / "part-00000.parquet", index=False)
        df2.to_parquet(tmp_path / "part-00001.parquet", index=False)

        result = validator.validate(
            expected_records=kafka_published,
            output_location={"storage_type": "local", "path": str(tmp_path)},
        )

        assert result["expected_records"] == kafka_published
        assert result["actual_records"] == 100_000
        assert result["expected_records"] == result["actual_records"]
        assert result["data_loss"] == 0
        assert result["data_loss_percentage"] == 0.0
        assert result["duplicate_ids_found"] == 0
        assert result["validation_passed"] is True

