import json
import psycopg2
from pathlib import Path
from datetime import datetime

raw_dir = Path("docs/evaluation/raw/campaign/datanode-stop")

conn = psycopg2.connect("postgresql://platform_user:platform_pass@localhost:5432/platform_db")
cur = conn.cursor()

results = []

for r in range(1, 6):
    run_dir = raw_dir / f"eval-datanode-stop-r{r}"
    with open(run_dir / "report.json", "r", encoding="utf-8") as f:
        rep = json.load(f)
        
    pipe_id = rep.get("pipelineRunId") or rep.get("pipeline_run_id")
    
    # 1. Pre-fault accounting snapshot
    pfa = rep.get("pre_fault_accounting", {})
    snap_ts = pfa.get("snapshot_timestamp")
    snap_recs = pfa.get("records_processed")
    
    # 2. Phase transitions
    phases = {p["name"]: p for p in rep.get("phases", [])}
    inj_phase = phases.get("injecting", {})
    docker_stop_issued = inj_phase.get("startedAt")
    docker_stop_finished = inj_phase.get("finishedAt")
    fault_injected_at = rep.get("faultInjectedAt")
    
    mon_phase = phases.get("monitoring", {})
    revert_issued = mon_phase.get("finishedAt")
    fault_reverted_at = rep.get("faultRevertedAt")
    recovered_at = rep.get("recoveredAt")
    
    # 3. Query all events from DB for this pipeline run
    cur.execute("""
        SELECT emitted_at, event_type, payload
        FROM pipeline_run_events
        WHERE pipeline_run_id = %s
        ORDER BY emitted_at ASC;
    """, (pipe_id,))
    events = cur.fetchall()
    
    # Trace records_out progression
    # Find records_out at snapshot, at docker stop issued, at docker stop finished (halt), at revert
    records_at_snap = snap_recs
    records_at_stop_issued = None
    records_at_halt = None
    records_at_revert = None
    
    # Also find any driver errors or exceptions
    storage_errors = []
    
    for em, et, pl in events:
        pl = pl or {}
        r_out = pl.get("records_out")
        em_iso = em.isoformat()
        
        # Check driver logs / stderr
        if et in ("driver_stderr_tail", "error", "pipeline_failed", "spark_error"):
            storage_errors.append((em, et, pl))
        elif "error" in str(pl).lower() or "exception" in str(pl).lower() or "refused" in str(pl).lower():
            storage_errors.append((em, et, pl))
            
        # Match records_out around key timestamps
        if docker_stop_issued and em_iso <= docker_stop_issued and r_out is not None:
            records_at_stop_issued = r_out
        if docker_stop_finished and em_iso <= docker_stop_finished and r_out is not None:
            records_at_halt = r_out
        if fault_reverted_at and em_iso <= fault_reverted_at and r_out is not None:
            records_at_revert = r_out
            
    # Also check if driver stderr tail was logged
    first_err_ts = None
    first_err_msg = None
    for em, et, pl in storage_errors:
        txt = str(pl)
        if "createBlockOutputStream" in txt or "minReplication" in txt or "ConnectException" in txt or "Connection refused" in txt:
            first_err_ts = em.isoformat()
            first_err_msg = txt[:150]
            break

    results.append({
        "run": f"r{r}",
        "pipe_id": pipe_id,
        "snapshot_ts": snap_ts,
        "snapshot_recs": snap_recs,
        "docker_stop_issued": docker_stop_issued,
        "docker_stop_finished": docker_stop_finished,
        "records_at_halt": records_at_halt,
        "revert_issued": revert_issued,
        "records_at_revert": records_at_revert,
        "first_err_ts": first_err_ts,
        "first_err_msg": first_err_msg,
        "total_errors": len(storage_errors)
    })

conn.close()

for r in results:
    print("=" * 80)
    print(f"Run: {r['run']} (Pipeline: {r['pipe_id']})")
    print(f"  Pre-fault Snapshot   : {r['snapshot_ts']} | records_out={r['snapshot_recs']}")
    print(f"  Docker stop issued   : {r['docker_stop_issued']}")
    print(f"  Container halt moment: {r['docker_stop_finished']} | records_out={r['records_at_halt']}")
    print(f"  Fault revert moment  : {r['revert_issued']} | records_out={r['records_at_revert']}")
    print(f"  First Storage Error  : {r['first_err_ts']}")
    if r['first_err_msg']:
        print(f"    Error detail: {r['first_err_msg']}")
