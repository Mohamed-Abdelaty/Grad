import urllib.request
import urllib.error
import json
import time

import os
import urllib.parse

def _get_credentials():
    u = os.environ.get("ADMIN_USERNAME") or os.environ.get("E2E_USERNAME")
    p = os.environ.get("ADMIN_PASSWORD") or os.environ.get("E2E_PASSWORD")
    if not u or not p:
        env_file = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
        if os.path.exists(env_file):
            with open(env_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("ADMIN_USERNAME=") and not u:
                        u = line.split("=", 1)[1].strip()
                    elif line.startswith("ADMIN_PASSWORD=") and not p:
                        p = line.split("=", 1)[1].strip()
    if not u:
        raise RuntimeError("Missing required environment variable: ADMIN_USERNAME or E2E_USERNAME")
    if not p:
        raise RuntimeError("Missing required environment variable: ADMIN_PASSWORD or E2E_PASSWORD")
    return u, p

def main():
    # Login
    u, p = _get_credentials()
    login_data = urllib.parse.urlencode({"username": u, "password": p}).encode("utf-8")
    req = urllib.request.Request("http://localhost:8000/api/auth/login", data=login_data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    res = urllib.request.urlopen(req)
    token = json.loads(res.read())["access_token"]
    
    with open("backend/sample-configs/sales_ui2b.yaml", "r") as f:
        yaml_str = f.read()
    
    # Run
    req2 = urllib.request.Request("http://localhost:8000/api/pipelines/run?wait=false", data=json.dumps({"yaml_content": yaml_str}).encode(), headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    res2 = urllib.request.urlopen(req2)
    run_id = json.loads(res2.read())["pipeline_run_id"]
    print(f"Run ID: {run_id}")
    
    # Progress
    for _ in range(30):
        req3 = urllib.request.Request(f"http://localhost:8000/api/pipeline-runs/{run_id}/progress", headers={"Authorization": f"Bearer {token}"})
        res3 = urllib.request.urlopen(req3)
        prog = json.loads(res3.read())
        print(f"Status: {prog['status']}")
        if prog["status"] != "running":
            print(json.dumps(prog, indent=2))
            break
        time.sleep(2)

if __name__ == "__main__":
    main()
