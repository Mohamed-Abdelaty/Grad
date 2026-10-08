import urllib.request
import json
import time
import subprocess
import threading
import sys

def main():
    print("--- 1. Login ---")
    req = urllib.request.Request("http://localhost:3000/api/auth/login", data=b"username=admin&password=REDACTED", headers={"Content-Type": "application/x-www-form-urlencoded"})
    res = urllib.request.urlopen(req)
    token = json.loads(res.read())["access_token"]

    with open("backend/sample-configs/sales_ui2b.yaml", "r") as f:
        yaml_str = f.read()

    print("\n--- 2. POST /run?wait=false ---")
    req2 = urllib.request.Request("http://localhost:3000/api/pipelines/run?wait=false", data=json.dumps({"yaml_content": yaml_str}).encode(), headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    t0 = time.time()
    res2 = urllib.request.urlopen(req2)
    run_data = json.loads(res2.read())
    run_id = run_data["pipeline_run_id"]
    print(f"POST returned in {time.time()-t0:.2f}s with status {res2.status}")
    print(f"Initial run data: {json.dumps(run_data)}")

    # Start a background thread for the SSE stream
    print("\n--- 3. Starting SSE request in background ---")
    
    def print_sse():
        sse_req = urllib.request.Request(f"http://localhost:3000/api/pipeline-runs/{run_id}/stream?token={token}")
        with open("scratch/stream.log", "w") as f:
            try:
                with urllib.request.urlopen(sse_req) as sse_res:
                    for line in sse_res:
                        line_str = line.decode()
                        sys.stdout.write(f"[SSE] {line_str}")
                        f.write(line_str)
            except Exception as e:
                print(f"[SSE Error] {e}")
    
    t = threading.Thread(target=print_sse)
    t.start()

    print("\n--- 4. Polling health & progress ---")
    for i in range(6):
        time.sleep(3)
        h_req = urllib.request.Request("http://localhost:3000/health")
        h_t0 = time.time()
        h_res = urllib.request.urlopen(h_req)
        h_ms = (time.time() - h_t0) * 1000
        print(f"[Health] Call {i+1} took {h_ms:.1f}ms: {json.loads(h_res.read())}")

        if i % 2 == 0:
            p_req = urllib.request.Request(f"http://localhost:3000/api/pipeline-runs/{run_id}/progress", headers={"Authorization": f"Bearer {token}"})
            p_res = urllib.request.urlopen(p_req)
            print(f"[Progress] Snapshot {i//2 + 1}: {json.dumps(json.loads(p_res.read()))}")

    print("\n--- 5. Waiting for SSE to finish ---")
    t.join()

if __name__ == "__main__":
    main()
