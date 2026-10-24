#!/usr/bin/env python3
"""
scripts/generate_large_dataset.py
---------------------------------
Generate synthetic sales CSV datasets for pipeline testing with multiple data shapes.
Includes both rich commercial fields (quantity, unit_price, total_amount, discount, category)
and platform schema fields (amount, timestamp) for complete end-to-end compatibility.
"""

from __future__ import annotations

import argparse
import csv
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

CATEGORIES = ["Electronics", "Clothing", "Books", "Home", "Sports", "Food"]
REGIONS = ["North", "South", "East", "West", "Central"]
PAYMENT_TYPES = ["Credit", "Debit", "Cash", "Online"]


def generate(rows: int, output: str, shape: str = "clean", seed: int = 42) -> None:
    random.seed(seed)
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    base_date = datetime(2024, 1, 1, 12, 0, 0)

    prev_row: list[str] | None = None

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "transaction_id",
            "customer_id",
            "product_id",
            "category",
            "quantity",
            "unit_price",
            "total_amount",
            "amount",
            "transaction_date",
            "timestamp",
            "region",
            "payment_type",
            "discount",
            "is_returned",
        ])

        for i in range(1, rows + 1):
            qty = random.randint(1, 100)
            price = round(random.uniform(1.0, 999.99), 2)
            disc = round(random.uniform(0.0, 0.3), 4)
            total = round(qty * price * (1.0 - disc), 2)

            tid = f"T{i:07d}"
            cid = f"CUST-{random.randint(1, 50000):05d}"
            pid = f"PROD-{random.randint(1, 5000):04d}"
            cat = random.choice(CATEGORIES)
            region = random.choice(REGIONS)
            pay = random.choice(PAYMENT_TYPES)
            ret = random.choice([True, False])

            delta_days = random.randint(0, 364)
            delta_seconds = random.randint(0, 86399)
            dt = base_date + timedelta(days=delta_days, seconds=delta_seconds)
            date_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            iso_str = dt.isoformat()

            amt_val = str(total)

            if shape == "with_nulls" and i % 50 == 0:
                cat = ""
                region = ""
                amt_val = ""

            if shape == "with_duplicates" and i % 200 == 0 and prev_row is not None:
                # duplicate row
                writer.writerow(prev_row)
                continue

            if shape == "edge_cases":
                if i % 1000 == 0:
                    qty = 0
                    price = 0.0
                    total = 0.0
                    amt_val = "0.0"
                elif i % 2000 == 0:
                    total = -abs(total)
                    amt_val = str(total)

            row = [
                tid,
                cid,
                pid,
                cat,
                str(qty),
                str(price),
                str(total),
                amt_val,
                date_str,
                iso_str,
                region,
                pay,
                str(disc),
                str(ret),
            ]
            writer.writerow(row)
            prev_row = row

    print(f"Generated {rows} rows ({shape}) -> {path.resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic sales CSV datasets.")
    parser.add_argument("--rows", type=int, default=1_000_000, help="Number of rows")
    parser.add_argument("--output", type=str, default="backend/sample-data/sales_large.csv", help="Output path")
    parser.add_argument(
        "--shape",
        type=str,
        default="clean",
        choices=["clean", "with_nulls", "with_duplicates", "edge_cases"],
        help="Data shape",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()
    generate(args.rows, args.output, args.shape, args.seed)
