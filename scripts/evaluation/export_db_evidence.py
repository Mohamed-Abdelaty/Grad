"""
scripts/evaluation/export_db_evidence.py
----------------------------------------
Export raw pipeline_run_events from PostgreSQL for control and datanode-stop runs
into docs/evaluation/results/derived/ to provide committed derived evidence
of microsecond progress events, stream IDs, and driver stderr crash logs.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path


def main():
    repo_root = Path(__file__).resolve().parent.parent.parent
    derived_dir = repo_root / "docs" / "evaluation" / "results" / "derived"
    derived_dir.mkdir(parents=True, exist_ok=True)

    # 1. Map runs to pipelineRunIds
    runs = {}
    for i in range(1, 6):
        ctrl_path = repo_root / "docs" / "evaluation" / "raw" / "control" / f"eval-control-r{i}" / "report.json"
        if ctrl_path.exists():
            d = json.loads(ctrl_path.read_text(encoding="utf-8"))
            pid = d.get("pipelineRunId") or d.get("pipeline_run_id")
            runs[f"control-r{i}"] = pid

    for i in range(1, 6):
        dn_path = repo_root / "docs" / "evaluation" / "raw" / "campaign" / "datanode-stop" / f"eval-datanode-stop-r{i}" / "report.json"
        if dn_path.exists():
            d = json.loads(dn_path.read_text(encoding="utf-8"))
            pid = d.get("pipelineRunId") or d.get("pipeline_run_id")
            runs[f"datanode-stop-r{i}"] = pid

    print("Found run IDs:")
    for k, v in runs.items():
        print(f"  {k}: {v}")

    # 2. Extract control run events
    control_events = {}
    for i in range(1, 6):
        run_name = f"control-r{i}"
        pid = runs.get(run_name)
        if not pid:
            continue
        cmd = [
            "docker", "exec", "platform-postgres", "psql", "-U", "platform_user", "-d", "platform_db", "-t", "-A", "-c",
            f"SELECT json_agg(t) FROM (SELECT seq, event_type, payload, emitted_at FROM pipeline_run_events WHERE pipeline_run_id = '{pid}' ORDER BY seq ASC) t;"
        ]
        res = subprocess.run(cmd, capture_output=True, text=True)
        out = res.stdout.strip()
        if out and out != "":
            try:
                events = json.loads(out)
                control_events[run_name] = {
                    "pipeline_run_id": pid,
                    "event_count": len(events),
                    "events": events
                }
            except Exception as e:
                print(f"Failed to parse json for {run_name}: {e}")
        else:
            control_events[run_name] = {
                "pipeline_run_id": pid,
                "event_count": 0,
                "events": []
            }

    ctrl_out_path = derived_dir / "pipeline_run_events_control.json"
    ctrl_out_path.write_text(json.dumps(control_events, indent=2), encoding="utf-8")
    print(f"Wrote control events to {ctrl_out_path}")

    # 3. Extract datanode run events and crash logs
    datanode_events = {}
    datanode_crashes = {}
    for i in range(1, 6):
        run_name = f"datanode-stop-r{i}"
        pid = runs.get(run_name)
        if not pid:
            continue
        cmd = [
            "docker", "exec", "platform-postgres", "psql", "-U", "platform_user", "-d", "platform_db", "-t", "-A", "-c",
            f"SELECT json_agg(t) FROM (SELECT seq, event_type, payload, emitted_at FROM pipeline_run_events WHERE pipeline_run_id = '{pid}' ORDER BY seq ASC) t;"
        ]
        res = subprocess.run(cmd, capture_output=True, text=True)
        out = res.stdout.strip()
        if out and out != "":
            try:
                events = json.loads(out)
                datanode_events[run_name] = {
                    "pipeline_run_id": pid,
                    "event_count": len(events),
                    "events": events
                }
                # Check for driver_stderr_tail or error payloads
                for ev in events:
                    etype = ev.get("event_type")
                    payload = ev.get("payload") or {}
                    if "driver_stderr" in payload or "stderr" in payload or "error" in payload or etype in ("error", "driver_stderr_tail", "failed"):
                        datanode_crashes.setdefault(run_name, []).append({
                            "seq": ev.get("seq"),
                            "event_type": etype,
                            "emitted_at": ev.get("emitted_at"),
                            "payload": payload
                        })
            except Exception as e:
                print(f"Failed to parse json for {run_name}: {e}")
        else:
            datanode_events[run_name] = {
                "pipeline_run_id": pid,
                "event_count": 0,
                "events": []
            }

    dn_out_path = derived_dir / "pipeline_run_events_datanode.json"
    dn_out_path.write_text(json.dumps(datanode_events, indent=2), encoding="utf-8")
    print(f"Wrote datanode events to {dn_out_path}")

    crash_out_path = derived_dir / "datanode_crashes_evidence.json"
    crash_out_path.write_text(json.dumps(datanode_crashes, indent=2), encoding="utf-8")
    print(f"Wrote datanode crash evidence to {crash_out_path}")


if __name__ == "__main__":
    main()
