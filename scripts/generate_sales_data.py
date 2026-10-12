#!/usr/bin/env python3
"""
scripts/generate_sales_data.py
------------------------------
Generates a synthetic sales dataset with controllable row count, random seed,
and dirty data ratio. Streams output using standard library csv module.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import os
import random
import shutil
import sys
from pathlib import Path


def generate_sales_data(
    rows: int,
    out_path: str,
    seed: int = 42,
    dirty_ratio: float = 0.05,
) -> None:
    random.seed(seed)
    target = Path(out_path)
    target.parent.mkdir(parents=True, exist_ok=True)

    now = datetime.datetime(2026, 10, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    seconds_in_30_days = 30 * 86400

    count_empty = 0
    count_negative = 0
    count_duplicate = 0
    prev_row: list[str] | None = None

    with open(target, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["transaction_id", "customer_id", "product_id", "amount", "timestamp"])

        for i in range(1, rows + 1):
            is_dirty = random.random() < dirty_ratio
            if is_dirty:
                dirty_kind = random.randint(0, 2)
                if dirty_kind == 0:
                    # Empty amount
                    cid = f"C{random.randint(1, 50000)}"
                    pid = f"P{random.randint(1, 2000)}"
                    amt = ""
                    ts = (now - datetime.timedelta(seconds=random.uniform(0, seconds_in_30_days))).isoformat()
                    tid = f"T{i:07d}"
                    row = [tid, cid, pid, amt, ts]
                    count_empty += 1
                elif dirty_kind == 1:
                    # Negative amount
                    cid = f"C{random.randint(1, 50000)}"
                    pid = f"P{random.randint(1, 2000)}"
                    amt = f"-{round(random.uniform(1, 500), 2):.2f}"
                    ts = (now - datetime.timedelta(seconds=random.uniform(0, seconds_in_30_days))).isoformat()
                    tid = f"T{i:07d}"
                    row = [tid, cid, pid, amt, ts]
                    count_negative += 1
                else:
                    # Exact duplicate of previous row
                    if prev_row is not None:
                        row = list(prev_row)
                        count_duplicate += 1
                    else:
                        cid = f"C{random.randint(1, 50000)}"
                        pid = f"P{random.randint(1, 2000)}"
                        amt = ""
                        ts = (now - datetime.timedelta(seconds=random.uniform(0, seconds_in_30_days))).isoformat()
                        tid = f"T{i:07d}"
                        row = [tid, cid, pid, amt, ts]
                        count_empty += 1
            else:
                cid = f"C{random.randint(1, 50000)}"
                pid = f"P{random.randint(1, 2000)}"
                amt = f"{round(random.uniform(1, 500), 2):.2f}"
                ts = (now - datetime.timedelta(seconds=random.uniform(0, seconds_in_30_days))).isoformat()
                tid = f"T{i:07d}"
                row = [tid, cid, pid, amt, ts]

            writer.writerow(row)
            prev_row = row

    file_size_bytes = os.path.getsize(target)
    file_size_mb = file_size_bytes / (1024 * 1024)

    print(f"Total rows written: {rows}")
    print(f"Dirty rows: empty_amount={count_empty}, negative_amount={count_negative}, duplicate={count_duplicate}")
    print(f"Output path: {target.resolve()}")
    print(f"File size: {file_size_bytes} bytes ({file_size_mb:.2f} MB)")

    # Also sync to backend/sample-data if target is sample-data/sales_large.csv so Docker container mount sees it
    backend_target = Path("backend/sample-data") / target.name
    if backend_target.parent.exists() and backend_target.resolve() != target.resolve():
        shutil.copy2(target, backend_target)
        print(f"Synced to container mount directory: {backend_target.resolve()}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate large synthetic sales dataset.")
    parser.add_argument("--rows", type=int, default=1_000_000, help="Total number of rows to generate")
    parser.add_argument("--out", type=str, default="sample-data/sales_large.csv", help="Output CSV path")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--dirty-ratio", type=float, default=0.05, help="Ratio of dirty records")

    args = parser.parse_args()
    generate_sales_data(
        rows=args.rows,
        out_path=args.out,
        seed=args.seed,
        dirty_ratio=args.dirty_ratio,
    )


if __name__ == "__main__":
    main()
