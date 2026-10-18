import pytest
import pandas as pd
from app.reliability.data_consistency_validator import DataConsistencyValidator


def test_duplicate_extra_rows_definition():
    """
    Test ONE consistent duplicate definition across the platform:
    duplicate_extra_rows = total_rows - distinct_ids
    Example: ids [1, 1, 1, 2, 2, 3] => total_rows=6, distinct_ids=3 => duplicate_extra_rows=3
    Meanwhile, the count of distinct IDs that appear > 1 time is 2 (id 1 and id 2).
    """
    validator = DataConsistencyValidator()
    df = pd.DataFrame({
        "transaction_id": [1, 1, 1, 2, 2, 3],
        "value": ["a", "b", "c", "d", "e", "f"],
    })

    col, extra_rows = validator._find_duplicates(df)
    assert col == "transaction_id"
    # total_rows (6) - distinct_ids (3) == 3
    assert extra_rows == 3

    # Direct verification against formula
    assert len(df) - df["transaction_id"].nunique() == 3
    assert int(df["transaction_id"].duplicated().sum()) == 3

    # And verify the distinct duplicated IDs count is 2, not 3
    duplicated_ids_count = df.loc[
        df["transaction_id"].duplicated(keep=False), "transaction_id"
    ].nunique()
    assert duplicated_ids_count == 2
