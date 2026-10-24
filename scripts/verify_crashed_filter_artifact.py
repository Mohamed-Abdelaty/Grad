import csv
import pandas as pd
from pathlib import Path

csv_path = Path("backend/sample-data/sales_300k.csv")
print(f"Reading {csv_path}...")

# Read full CSV
df = pd.read_csv(csv_path)
print(f"Total rows in CSV: {len(df)}")

for N in [56963, 56968]:
    sub_df = df.iloc[:N]
    
    # Filter conditions in pipeline:
    # 1. remove_nulls on any column
    # 2. amount > 0
    null_mask = sub_df.isnull().any(axis=1)
    # also check if amount <= 0
    # non-null amount <= 0
    valid_amount_mask = sub_df["amount"] > 0
    
    # Rows that pass:
    surviving = sub_df[(~null_mask) & (valid_amount_mask)]
    surviving_count = len(surviving)
    
    # Rows that fail:
    failed_rows = N - surviving_count
    
    # Also break down:
    null_rows = null_mask.sum()
    non_positive_rows = ((~null_mask) & (sub_df["amount"] <= 0)).sum()
    
    print(f"\n--- First N = {N} rows ---")
    print(f"Input records: {N}")
    print(f"Surviving records (committed expected): {surviving_count}")
    print(f"Filtered out records: {failed_rows} (nulls={null_rows}, amount <= 0: {non_positive_rows})")
    if N == 56963:
        print(f"Comparison with datanode-stop-r2:")
        print(f"  records_out: {N}")
        print(f"  committed rows: 54,995")
        print(f"  delta = {N - 54995}")
        print(f"  exact match with filtered out records ({failed_rows})? {failed_rows == (N - 54995)}")
    if N == 56968:
        print(f"Comparison with datanode-stop-r5:")
        print(f"  records_out: {N}")
        print(f"  committed rows: 54,990")
        print(f"  delta = {N - 54990}")
        print(f"  exact match with filtered out records ({failed_rows})? {failed_rows == (N - 54990)}")
