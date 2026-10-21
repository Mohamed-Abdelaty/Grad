import json
import os
from pathlib import Path

raw_dir = Path("docs/evaluation/raw")

runs = []

# Control runs
for i in range(1, 6):
    r_dir = raw_dir / "control" / f"eval-control-r{i}"
    runs.append(("control", f"eval-control-r{i}", r_dir))

# Fault runs
fault_dirs = [
    ("kafka-stop", [
        "pilot-v3/eval-kafka-stop-r1",
        "pilot-v3/eval-kafka-stop-r2",
        "campaign/kafka-stop/eval-kafka-stop-r3",
        "campaign/kafka-stop/eval-kafka-stop-r4",
        "campaign/kafka-stop/eval-kafka-stop-r5"
    ]),
    ("kafka-restart", [f"campaign/kafka-restart/eval-kafka-restart-r{i}" for i in range(1, 6)]),
    ("spark-worker-stop", [f"campaign/spark-worker-stop/eval-spark-worker-stop-r{i}" for i in range(1, 6)]),
    ("spark-worker-restart", [f"campaign/spark-worker-restart/eval-spark-worker-restart-r{i}" for i in range(1, 6)]),
    ("datanode-stop", [f"campaign/datanode-stop/eval-datanode-stop-r{i}" for i in range(1, 6)]),
]

for exp, rps in fault_dirs:
    for rp in rps:
        r_dir = raw_dir / rp
        runs.append((exp, r_dir.name, r_dir))

print(f"Total runs found: {len(runs)}")

table = []
for exp, name, r_dir in runs:
    rep_f = r_dir / "report.json"
    phys_f = r_dir / "physical_metrics.json"
    
    with open(rep_f, "r", encoding="utf-8") as f:
        rep = json.load(f)
    with open(phys_f, "r", encoding="utf-8") as f:
        phys = json.load(f)
        
    pid = rep.get("pipelineRunId") or rep.get("pipeline_run_id")
    phys_rows = phys.get("physical_rows")
    ms = phys.get("multiset_comparison", {})
    missing = ms.get("missing_rows", phys.get("physical_data_loss", 0))
    extra = ms.get("extra_rows", 0)
    excess = ms.get("duplicate_beyond_baseline", phys.get("excess_duplicates", 0))
    
    pv = phys.get("platform_validator", {})
    pv_loss = pv.get("data_loss") if pv else rep.get("dataLoss")
    pv_proc = pv.get("processed_records") if pv else rep.get("processedRecords")
    pv_exp = pv.get("expected_records") if pv else rep.get("expectedRecords")
    
    # Terminal status of the pipeline
    status = rep.get("status") or rep.get("result")
    table.append({
        "exp": exp,
        "name": name,
        "pid": str(pid),
        "phys_rows": phys_rows,
        "pv_loss": pv_loss,
        "pv_proc": pv_proc,
        "pv_exp": pv_exp,
        "missing": missing,
        "extra": extra,
        "excess": excess,
        "status": status
    })

header = f"{'Exp':22} | {'Run Name':28} | {'Run ID':36} | {'Committed Rows':14} | {'PV Output':9} | {'PV Processed':12} | {'Missing':7} | {'Extra':5} | {'Excess':6} | {'Status'}"
print(header)
print("-" * len(header))
for t in table:
    print(f"{t['exp']:22} | {t['name']:28} | {t['pid']:36} | {str(t['phys_rows']):14} | {str(t['pv_loss']):9} | {str(t['pv_proc']):12} | {str(t['missing']):7} | {str(t['extra']):5} | {str(t['excess']):6} | {t['status']}")

# Check per-experiment means of PV output
print("\n=== Per-Experiment Validator Output Means ===")
from collections import defaultdict
pv_by_exp = defaultdict(list)
for t in table:
    if t["pv_loss"] is not None:
        pv_by_exp[t["exp"]].append(t["pv_loss"])

for exp, vals in pv_by_exp.items():
    print(f"{exp:22}: mean={sum(vals)/len(vals):.1f}, individual={vals}")
