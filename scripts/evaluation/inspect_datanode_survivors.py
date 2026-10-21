import json
import psycopg2

conn = psycopg2.connect("postgresql://platform_user:platform_pass@localhost:5432/platform_db")
cur = conn.cursor()

survivor_runs = [
    ("r1", "6a8be505-922e-48b2-82a2-c0fdc66396f8", "2026-10-21T10:30:31.168290+00:00", "2026-10-21T10:30:51.187308+00:00", "2026-10-21T10:30:51.731343+00:00"),
    ("r3", "3e33732a-caaa-404b-8061-51c204f7919b", "2026-10-21T10:38:36.659556+00:00", "2026-10-21T10:38:56.686585+00:00", "2026-10-21T10:38:57.076870+00:00"),
    ("r4", "b24212d1-d1ff-4db0-b8a1-169b94270cee", "2026-10-21T10:45:36.510258+00:00", "2026-10-21T10:45:56.533952+00:00", "2026-10-21T10:45:57.011642+00:00"),
]

patterns = ["DataStreamer", "createBlockOutputStream", "Abandoning", "Excluding", "retry", "Slow", "Exception", "Connection refused"]

for name, pid, halt_ts, revert_ts, rec_ts in survivor_runs:
    print("=" * 80)
    print(f"SURVIVOR RUN {name}: Pipeline {pid}")
    print(f"  Container Halted : {halt_ts}")
    print(f"  Fault Reverted   : {revert_ts}")
    print(f"  Container Health : {rec_ts}")
    
    # Query all events in the outage window (halt_ts to rec_ts + 30s)
    cur.execute("""
        SELECT emitted_at, event_type, payload
        FROM pipeline_run_events
        WHERE pipeline_run_id = %s
        ORDER BY emitted_at ASC;
    """, (pid,))
    all_events = cur.fetchall()
    
    # Filter during outage
    outage_events = [e for e in all_events if halt_ts <= e[0].isoformat() <= rec_ts]
    print(f"  Total events during outage window ({halt_ts} to {rec_ts}): {len(outage_events)}")
    for em, et, pl in outage_events:
        print(f"    {em.isoformat()} | {et} | {str(pl)[:100]}")
        
    # Check for patterns across ALL events of the run
    matched = []
    for em, et, pl in all_events:
        txt = json.dumps(pl)
        for p in patterns:
            if p.lower() in txt.lower():
                matched.append((em.isoformat(), et, p, txt[:120]))
                break
    print(f"  Keywords matched across entire run: {len(matched)}")
    for m in matched[:10]:
        print(f"    {m[0]} | {m[1]} | matched '{m[2]}': {m[3]}")

conn.close()
