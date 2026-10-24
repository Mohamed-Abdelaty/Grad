import json
from pathlib import Path

master_path = Path("docs/evaluation/results/results_master.json")
master = json.loads(master_path.read_text(encoding="utf-8"))
runs = master["runs"]

# Sort runs by started_at if available
print(f"Total runs: {len(runs)}")
print("| # | Run Name | Experiment | Rep | Started At (UTC) | Finished At (UTC) | Status |")
print("|---|---|---|---|---|---|---|")

# Let's also check if started_at is directly in report.json
import build_master
spec_map = {spec["run_name"]: spec for spec in build_master.RUN_SPECIFICATIONS}

run_rows = []
for i, r in enumerate(runs, 1):
    run_name = r["run_name"]
    spec = spec_map.get(run_name, {})
    raw_dir = Path(spec.get("raw_dir", ""))
    started = r.get("started_at")
    finished = r.get("finished_at")
    if not started and raw_dir.exists():
        env_f = raw_dir / "environment.json"
        rep_f = raw_dir / "report.json"
        if env_f.exists():
            env_d = json.loads(env_f.read_text(encoding="utf-8"))
            started = started or env_d.get("started_at")
            finished = finished or env_d.get("finished_at")
        if rep_f.exists():
            rep_d = json.loads(rep_f.read_text(encoding="utf-8"))
            started = started or rep_d.get("started_at")
            finished = finished or rep_d.get("finished_at")
    run_rows.append((started or "", run_name, r["experiment"], r["repetition"], started, finished, r.get("status", "")))

# Sort chronologically by started_at
run_rows.sort(key=lambda x: x[0])

for idx, (_, rname, exp, rep, st, fin, status) in enumerate(run_rows, 1):
    print(f"| {idx} | {rname} | {exp} | r{rep} | {st} | {fin} | {status} |")
