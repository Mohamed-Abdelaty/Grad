"""
scripts/evaluation/export_raw_progress_control.py
-------------------------------------------------
Export raw spark_progress rows from PostgreSQL for all 5 control runs
into docs/evaluation/results/derived/raw_progress_control.csv.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

def main():
    repo_root = Path(__file__).resolve().parent.parent.parent
    derived_dir = repo_root / "docs" / "evaluation" / "results" / "derived"
    derived_dir.mkdir(parents=True, exist_ok=True)
    out_csv = derived_dir / "raw_progress_control.csv"

    sql_query = """
    COPY (
        SELECT 
            r.run_name,
            e.pipeline_run_id,
            e.seq,
            e.emitted_at,
            (e.payload->>'records_in')::numeric as records_in,
            (e.payload->>'records_out')::numeric as records_out,
            (e.payload->>'processedRowsPerSecond')::numeric as processed_rows_per_sec,
            (e.payload->>'recordsPerSecond')::numeric as records_per_sec,
            (e.payload->>'consumer_lag')::numeric as consumer_lag,
            (e.payload->>'etaSeconds')::numeric as eta_seconds
        FROM (
            VALUES 
                ('control-r1', 'd9e56a60-e3a2-4737-925b-865b979d7af2'::uuid),
                ('control-r2', '70873f6a-db61-42b3-95fa-62a24ccae081'::uuid),
                ('control-r3', '7ee6e289-386e-4818-91ee-e16cabf8dcec'::uuid),
                ('control-r4', 'ccf97759-fc25-4be6-8e19-3feeed60854d'::uuid),
                ('control-r5', '2c2a5076-276a-46ab-ae7e-b87b8ad63d84'::uuid)
        ) r(run_name, pid)
        JOIN pipeline_run_events e ON e.pipeline_run_id = r.pid
        WHERE e.event_type = 'spark_progress'
        ORDER BY r.run_name, e.seq ASC
    ) TO STDOUT WITH CSV HEADER;
    """

    cmd = ["docker", "exec", "-i", "platform-postgres", "psql", "-U", "platform_user", "-d", "platform_db", "-c", sql_query]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print("Error executing SQL:", res.stderr)
        raise SystemExit(1)
    
    out_csv.write_text(res.stdout, encoding="utf-8")
    print(f"Successfully exported {len(res.stdout.splitlines()) - 1} rows to {out_csv}")

if __name__ == "__main__":
    main()
