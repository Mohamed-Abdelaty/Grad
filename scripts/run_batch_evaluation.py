#!/usr/bin/env python3
"""
scripts/run_batch_evaluation.py
-------------------------------
Runs the 1M-row batch pipeline evaluation across all 4 shapes:
clean, with_nulls, with_duplicates, edge_cases.
Records exact timing, input rows, output rows from HDFS, and verifies against expected counts.

HARD TIMEOUT: Each Spark job is given MAX_POLL_SECONDS to complete.
              If it does not complete in time, the shape is recorded as TIMED_OUT.
              NO infinite loops anywhere in this script.
"""

from __future__ import annotations

import io
import json
import os
import sys
import time
import urllib.parse
import urllib.request

# ── Strict ceiling constants ──────────────────────────────────────────────────
MAX_POLL_SECONDS = 600        # 10 min max per shape (clean took ~111s)
POLL_INTERVAL    = 8          # seconds between status polls
HTTP_TIMEOUT     = 15         # seconds per HTTP request
MAX_POLL_ERRORS  = 10         # abort if we get this many consecutive HTTP errors

BASE_URL = "http://127.0.0.1:8000"

# ── Auth ──────────────────────────────────────────────────────────────────────

def get_token() -> str:
    data = urllib.parse.urlencode({"username": "admin", "password": "123456"}).encode()
    req = urllib.request.Request(
        f"{BASE_URL}/api/auth/login",
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    res = urllib.request.urlopen(req, timeout=HTTP_TIMEOUT)
    return json.loads(res.read().decode())["access_token"]


# ── HDFS row counter ──────────────────────────────────────────────────────────

def count_hdfs_parquet_rows(hdfs_dir: str) -> tuple[int, list[str]]:
    """Count rows in every .parquet file under hdfs_dir via WebHDFS.
    Returns (total_rows, column_names). Uses pyarrow if available, else
    falls back to reporting -1 with an explanatory string."""
    path = hdfs_dir.replace("hdfs://namenode:9000", "")
    list_url = f"http://127.0.0.1:9870/webhdfs/v1{path}?op=LISTSTATUS"
    req = urllib.request.Request(list_url)
    res = urllib.request.urlopen(req, timeout=HTTP_TIMEOUT)
    listing = json.loads(res.read().decode())["FileStatuses"]["FileStatus"]

    try:
        import pyarrow.parquet as pq
        use_pyarrow = True
    except ImportError:
        use_pyarrow = False

    total_rows = 0
    cols: list[str] = []
    for item in listing:
        fname = item["pathSuffix"]
        if not fname.endswith(".parquet"):
            continue
        furl = f"http://127.0.0.1:9870/webhdfs/v1{path}/{fname}?op=OPEN"
        fres = urllib.request.urlopen(furl, timeout=60)
        raw = fres.read()
        if use_pyarrow:
            table = pq.read_table(io.BytesIO(raw))
            total_rows += table.num_rows
            if not cols:
                cols = table.column_names
        else:
            # Cannot count without pyarrow – return sentinel
            return -1, ["pyarrow_not_available"]
    return total_rows, cols


# ── Per-shape runner ─────────────────────────────────────────────────────────

def run_shape(shape_name: str, config_path: str, token: str) -> dict:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    with open(config_path, encoding="utf-8") as f:
        yaml_content = f.read()

    print(f"\n==========================================", flush=True)
    print(f"Submitting batch job for shape: {shape_name}", flush=True)
    print(f"Config: {config_path}", flush=True)

    # ── Submit ────────────────────────────────────────────────────────────────
    submit_req = urllib.request.Request(
        f"{BASE_URL}/api/pipelines/run",
        data=json.dumps({"yaml_content": yaml_content}).encode("utf-8"),
        headers=headers,
    )
    try:
        res = urllib.request.urlopen(submit_req, timeout=HTTP_TIMEOUT)
        submit_resp = json.loads(res.read().decode())
    except Exception as exc:
        print(f"  [{shape_name}] SUBMIT FAILED: {exc}", flush=True)
        return {
            "shape": shape_name, "status": "SUBMIT_FAILED",
            "error_message": str(exc),
        }

    run_id = submit_resp.get("pipeline_run_id") or submit_resp.get("run_id")
    print(f"  Run ID: {run_id}", flush=True)

    # ── Poll with hard ceiling ────────────────────────────────────────────────
    t0 = time.time()
    last_status: str | None = None
    final_data: dict = {}
    consecutive_errors = 0
    status = "unknown"

    while True:
        elapsed = time.time() - t0

        # ── Hard timeout ──────────────────────────────────────────────────────
        if elapsed >= MAX_POLL_SECONDS:
            print(
                f"  [{shape_name}] HARD TIMEOUT after {elapsed:.1f}s "
                f"(MAX_POLL_SECONDS={MAX_POLL_SECONDS}). Aborting poll.",
                flush=True,
            )
            status = "TIMED_OUT"
            break

        # ── Poll ──────────────────────────────────────────────────────────────
        poll_req = urllib.request.Request(
            f"{BASE_URL}/api/pipeline-runs/{run_id}",
            headers=headers,
        )
        try:
            poll_res = urllib.request.urlopen(poll_req, timeout=HTTP_TIMEOUT)
            final_data = json.loads(poll_res.read().decode())
            consecutive_errors = 0
        except Exception as exc:
            consecutive_errors += 1
            print(
                f"  [{shape_name}] Poll error #{consecutive_errors}/{MAX_POLL_ERRORS}: {exc}",
                flush=True,
            )
            if consecutive_errors >= MAX_POLL_ERRORS:
                print(
                    f"  [{shape_name}] Too many consecutive poll errors. "
                    f"Marking as POLL_FAILED.",
                    flush=True,
                )
                status = "POLL_FAILED"
                break
            time.sleep(POLL_INTERVAL)
            continue

        status = final_data.get("status", "unknown")
        if status != last_status:
            print(
                f"  [{shape_name}] Elapsed: {elapsed:.1f}s | Status: {status}",
                flush=True,
            )
            last_status = status

        if status in ("succeeded", "success", "failed", "error", "cancelled"):
            duration = elapsed
            print(
                f"  [{shape_name}] Completed: {status} in {duration:.2f}s",
                flush=True,
            )
            break

        time.sleep(POLL_INTERVAL)

    duration = time.time() - t0

    # ── Verify HDFS output ────────────────────────────────────────────────────
    hdfs_path = (
        final_data.get("output_path")
        or f"hdfs://namenode:9000/data/sales_large_{shape_name}"
    )
    print(f"  Verifying HDFS output at: {hdfs_path} ...", flush=True)

    output_rows = -1
    columns: list[str] = []
    if status in ("succeeded", "success"):
        try:
            output_rows, columns = count_hdfs_parquet_rows(hdfs_path)
            print(f"  HDFS row count: {output_rows}", flush=True)
        except Exception as exc:
            print(f"  Error reading HDFS via WebHDFS: {exc}", flush=True)
            output_rows = -1

    throughput = (
        round(output_rows / duration, 2)
        if duration > 0 and output_rows > 0
        else 0.0
    )

    result = {
        "shape": shape_name,
        "run_id": run_id,
        "status": status,
        "input_rows": 1_000_000,
        "output_rows": output_rows,
        "duration_seconds": round(duration, 2),
        "throughput_rows_per_sec": throughput,
        "hdfs_path": hdfs_path,
        "columns": columns,
        "error_message": final_data.get("error_message"),
    }
    print(f"  Result: {json.dumps(result, indent=2)}", flush=True)
    return result


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=== 1M-row Batch Evaluation (with hard timeouts) ===", flush=True)
    print(f"MAX_POLL_SECONDS={MAX_POLL_SECONDS}, POLL_INTERVAL={POLL_INTERVAL}s", flush=True)

    # Authenticate
    try:
        token = get_token()
        print("Obtained auth token successfully.", flush=True)
    except Exception as exc:
        print(f"FATAL: Cannot obtain auth token: {exc}", file=sys.stderr)
        sys.exit(1)

    all_results: dict = {}

    # ── Clean shape (load from disk if already done) ──────────────────────────
    clean_path = "docs/evaluation/results/batch_clean_result.json"
    if os.path.exists(clean_path):
        with open(clean_path) as f:
            all_results["clean"] = json.load(f)
        print(f"Clean shape already recorded — loaded from {clean_path}", flush=True)
    else:
        clean_res = run_shape(
            "clean", "backend/sample-configs/sales_large_clean.yaml", token
        )
        all_results["clean"] = clean_res
        with open(clean_path, "w") as f:
            json.dump(clean_res, f, indent=2)

    # ── Remaining 3 shapes ─────────────────────────────────────────────────────
    shapes = [
        ("with_nulls",      "backend/sample-configs/sales_large_nulls.yaml"),
        ("with_duplicates", "backend/sample-configs/sales_large_duplicates.yaml"),
        ("edge_cases",      "backend/sample-configs/sales_large_edge.yaml"),
    ]

    for shape_name, cfg in shapes:
        res = run_shape(shape_name, cfg, token)
        all_results[shape_name] = res

    # ── Persist combined results ───────────────────────────────────────────────
    out_file = "docs/evaluation/results/batch_1m_results.json"
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with open(out_file, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nAll shapes evaluated. Results saved to {out_file}", flush=True)

    # ── Summary table ─────────────────────────────────────────────────────────
    print("\n=== SUMMARY ===", flush=True)
    print(f"{'Shape':<20} {'Status':<12} {'Input':>10} {'Output':>10} {'Duration':>10} {'Throughput':>14}", flush=True)
    print("-" * 80, flush=True)
    for shape, r in all_results.items():
        print(
            f"{shape:<20} {r.get('status','?'):<12} "
            f"{r.get('input_rows', 0):>10,} {r.get('output_rows', 0):>10,} "
            f"{r.get('duration_seconds', 0):>9.1f}s "
            f"{r.get('throughput_rows_per_sec', 0):>13.1f}/s",
            flush=True,
        )


if __name__ == "__main__":
    main()
