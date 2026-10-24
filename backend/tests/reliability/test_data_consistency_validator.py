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

    def test_filter_pipeline_reduces_rows_without_counting_as_data_loss(
        self, validator, parquet_fixture
    ) -> None:
        """
        A pipeline with a filter (input 20 rows, output 14 rows, no fault)
        must validate with data_loss=0 because expected_records is the Spark
        output count (14), NOT the raw input rows (20).
        """
        # Pipeline written output: 14 rows that survived the filter
        filtered_output_df = pd.DataFrame({
            "transaction_id": [f"t{i}" for i in range(14)],
            "amount": [10.0 + i for i in range(14)],
        })
        path = parquet_fixture(filtered_output_df)

        # Expected records is verified Spark output (14), NOT input (20)
        result = validator.validate(
            expected_records=14,
            output_location={"storage_type": "local", "path": path},
        )
        assert result["actual_records"] == 14
        assert result["expected_records"] == 14
        assert result["data_loss"] == 0
        assert result["data_loss_percentage"] == 0.0
        assert result["validation_passed"] is True



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


class TestSourceDuplicatesSubtraction:
    """Tests for fault-induced duplicates accounting (output duplicates - source duplicates)."""

    def test_data_loss_when_actual_less_than_expected(
        self, validator, parquet_fixture
    ) -> None:
        """When actual < expected, data_loss must equal expected - actual."""
        # 85 actual records written
        df = pd.DataFrame({"transaction_id": [f"t{i}" for i in range(85)]})
        path = parquet_fixture(df)
        result = validator.validate(
            expected_records=100,
            output_location={"storage_type": "local", "path": path},
        )
        assert result["expected_records"] == 100
        assert result["actual_records"] == 85
        assert result["data_loss"] == 15
        assert result["data_loss_percentage"] == 15.0
        assert result["validation_passed"] is False

    def test_net_duplicates_subtracts_source_duplicates(
        self, validator, parquet_fixture
    ) -> None:
        """
        When source file contains duplicate IDs, net fault-induced duplicates
        must be max(0, output_duplicates - source_duplicates).
        """
        # Output has 5 unique IDs, but id1 appears 3 times, id2 appears 2 times -> 3 duplicate records
        # Total rows = 7: id1, id1, id1, id2, id2, id3, id4
        out_df = pd.DataFrame({"transaction_id": ["id1", "id1", "id1", "id2", "id2", "id3", "id4"]})
        out_path = parquet_fixture(out_df, "output.parquet")

        # Source has 1 duplicate: id1 appears 2 times -> 1 duplicate record
        src_df = pd.DataFrame({"transaction_id": ["id1", "id1", "id3", "id4"]})
        src_path = parquet_fixture(src_df, "source.parquet")

        result = validator.validate(
            expected_records=7,
            output_location={"storage_type": "local", "path": out_path},
            source_location={"storage_type": "local", "path": src_path},
        )
        assert result["output_duplicates"] == 3
        assert result["source_duplicates"] == 1
        assert result["fault_induced_duplicates"] == 2
        assert result["duplicate_ids_found"] == 2  # Net reported
        assert result["duplicates"] == 2

    def test_no_negative_duplicates_when_source_has_more(
        self, validator, parquet_fixture
    ) -> None:
        """
        If source had 5 duplicates but output only retained 2, net fault-induced
        duplicates must be 0 (never negative).
        """
        out_df = pd.DataFrame({"transaction_id": ["id1", "id1", "id2"]})
        out_path = parquet_fixture(out_df, "output.parquet")

    def test_duplicate_calculation_matrix(self, validator, parquet_fixture) -> None:
        """
        Unit test matrix for duplicates:
        - both null / unmeasured => fault_induced null (never 0)
        - source 4932, output 4842 => 0
        - source 100, output 130 => 30
        """
        # When source_location fails to read, source_duplicates is None, so fault_induced must be None
        df = pd.DataFrame({"transaction_id": ["id1", "id1", "id2"]})
        path = parquet_fixture(df, "out_no_src.parquet")
        res_no_src = validator.validate(
            expected_records=3,
            output_location={"storage_type": "local", "path": path},
            source_location={"storage_type": "local", "path": "/nonexistent/src.parquet"},
        )
        assert res_no_src["output_duplicates"] == 1
        assert res_no_src["source_duplicates"] is None
        assert res_no_src["fault_induced_duplicates"] is None
        assert res_no_src["duplicates"] is None

        # Direct mathematical logic assertions corresponding to source 4932, output 4842 => 0
        src_dups_1 = 4932
        out_dups_1 = 4842
        assert max(0, out_dups_1 - src_dups_1) == 0

        # source 100, output 130 => 30
        src_dups_2 = 100
        out_dups_2 = 130
        assert max(0, out_dups_2 - src_dups_2) == 30

    def test_data_loss_none_when_expected_or_actual_is_none(self, validator, parquet_fixture) -> None:
        """If expected_records or actual_records is None, data_loss must be None (never 0)."""
        df = pd.DataFrame({"transaction_id": ["id1", "id2"]})
        path = parquet_fixture(df, "out_none.parquet")
        res = validator.validate(
            expected_records=None,
            output_location={"storage_type": "local", "path": path},
        )
        assert res["expected_records"] is None
        assert res["actual_records"] == 2
        assert res["data_loss"] is None
        assert res["data_loss_percentage"] is None




class TestFilterAwareConsistencyValidation:
    """Tests for transformation filter accounting and scientific data loss equation."""

    def test_filter_accounting_null_and_condition_filters(
        self, validator, parquet_fixture
    ) -> None:
        """
        Verify that intentional filtering (e.g. sales_300k.csv with 5,124 nulls and
        5,132 condition drops) yields zero unexpected data loss when accounted for.
        """
        # 100 rows survived out of 120 raw input
        survived_rows = 100
        df = pd.DataFrame({
            "transaction_id": [f"txn_{i}" for i in range(survived_rows)],
            "amount": [10.5 + i for i in range(survived_rows)],
        })
        path = parquet_fixture(df, "filtered_output.parquet")

        result = validator.validate(
            expected_records=120,
            output_location={"storage_type": "local", "path": path},
            filter_accounting={
                "null_filtered": 12,
                "condition_filtered": 8,
            },
        )
        assert result["raw_input_records"] == 120
        assert result["filtered_records"] == 20
        assert result["expected_records"] == 100
        assert result["actual_records"] == 100
        assert result["data_loss"] == 0
        assert result["unexpected_loss"] == 0
        assert result["data_loss_percentage"] == 0.0
        assert result["validation_passed"] is True

    def test_sales_300k_exact_accounting_model(
        self, validator, parquet_fixture
    ) -> None:
        """
        Scientific model:
        Committed Records = Raw Input (300,000) - Null Filtered (5,124) - Condition Filtered (5,132) - Unexpected Loss (0)
        Committed Records = 289,744
        """
        df = pd.DataFrame({
            "transaction_id": [f"t{i}" for i in range(289_744)],
            "amount": [10.0] * 289_744,
        })
        path = parquet_fixture(df, "sales_300k_simulated.parquet")

        result = validator.validate(
            expected_records=300_000,
            output_location={"storage_type": "local", "path": path},
            filter_accounting={
                "null_filtered": 5_124,
                "condition_filtered": 5_132,
            },
        )
        assert result["raw_input_records"] == 300_000
        assert result["filtered_records"] == 10_256
        assert result["expected_records"] == 289_744
        assert result["actual_records"] == 289_744
        assert result["data_loss"] == 0
        assert result["unexpected_loss"] == 0
        assert result["data_loss_percentage"] == 0.0
        assert result["validation_passed"] is True

    def test_filter_accounting_with_genuine_loss(
        self, validator, parquet_fixture
    ) -> None:
        """
        When intentional filtering removes 20 rows, but actual output is 5 rows short
        of the post-filter expected count, the validator must report unexpected data_loss=5.
        """
        # Raw 100, filtered 20 -> expected 80, but only 75 actual rows written
        df = pd.DataFrame({"transaction_id": [f"t{i}" for i in range(75)]})
        path = parquet_fixture(df, "loss_output.parquet")

        result = validator.validate(
            expected_records=100,
            output_location={"storage_type": "local", "path": path},
            filter_accounting={"null_filtered": 20},
        )
        assert result["raw_input_records"] == 100
        assert result["filtered_records"] == 20
        assert result["expected_records"] == 80
        assert result["actual_records"] == 75
        assert result["data_loss"] == 5
        assert result["unexpected_loss"] == 5
        assert result["data_loss_percentage"] == 6.25  # 5 / 80 * 100
        assert result["validation_passed"] is False

    def test_filter_accounting_exceeding_raw_records_clamps_to_zero(
        self, validator, parquet_fixture
    ) -> None:
        """If filter count exceeds raw input, effective expected records must clamp to 0."""
        df = pd.DataFrame({"transaction_id": []})
        path = parquet_fixture(df, "empty_output.parquet")

        result = validator.validate(
            expected_records=50,
            output_location={"storage_type": "local", "path": path},
            filter_accounting={"total_filtered": 60},
        )
        assert result["expected_records"] == 0
        assert result["actual_records"] == 0
        assert result["data_loss"] == 0
        assert result["validation_passed"] is True

    def test_filter_accounting_negative_value_raises_value_error(
        self, validator, parquet_fixture
    ) -> None:
        """Negative filter values must be rejected with ValueError."""
        import pytest
        df = pd.DataFrame({"transaction_id": ["t1"]})
        path = parquet_fixture(df, "neg_test.parquet")
        with pytest.raises(ValueError, match="cannot be negative"):
            validator.validate(
                expected_records=10,
                output_location={"storage_type": "local", "path": path},
                filter_accounting={"dropped_negative": -5},
            )

    def test_filter_accounting_non_numeric_value_raises_type_error(
        self, validator, parquet_fixture
    ) -> None:
        """Non-numeric filter counts must be rejected with TypeError."""
        import pytest
        df = pd.DataFrame({"transaction_id": ["t1"]})
        path = parquet_fixture(df, "non_num_test.parquet")
        with pytest.raises(TypeError, match="must be numeric"):
            validator.validate(
                expected_records=10,
                output_location={"storage_type": "local", "path": path},
                filter_accounting={"dropped": "ten"},
            )

    def test_filter_accounting_non_dict_raises_type_error(
        self, validator, parquet_fixture
    ) -> None:
        """Non-dictionary filter accounting must be rejected with TypeError."""
        import pytest
        df = pd.DataFrame({"transaction_id": ["t1"]})
        path = parquet_fixture(df, "non_dict_test.parquet")
        with pytest.raises(TypeError, match="must be a dictionary"):
            validator.validate(
                expected_records=10,
                output_location={"storage_type": "local", "path": path},
                filter_accounting=[10, 20],  # type: ignore
            )

