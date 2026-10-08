import urllib.request
import urllib.error
import json
import time

def main():
    # Login
    req = urllib.request.Request("http://localhost:8000/api/auth/login", data=b"username=admin&password=REDACTED", headers={"Content-Type": "application/x-www-form-urlencoded"})
    res = urllib.request.urlopen(req)
    token = json.loads(res.read())["access_token"]
    
    with open("backend/sample-configs/sales_ui2b.yaml", "r") as f:
        yaml_str = f.read()
    
    # Run
    req2 = urllib.request.Request("http://localhost:8000/api/pipelines/run?wait=false", data=json.dumps({"yaml_content": yaml_str}).encode(), headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    res2 = urllib.request.urlopen(req2)
    run_id = json.loads(res2.read())["pipeline_run_id"]
    print(f"Run ID: {run_id}")
    
    time.sleep(2) # let it start
    
    # Cancel
    print("Canceling...")
    req_cancel = urllib.request.Request(f"http://localhost:8000/api/pipeline-runs/{run_id}/cancel", method="POST", headers={"Authorization": f"Bearer {token}"})
    try:
        urllib.request.urlopen(req_cancel)
    except urllib.error.HTTPError as e:
        print(f"Cancel error: {e.code}")
    
    time.sleep(3)
    
    # Progress
    req3 = urllib.request.Request(f"http://localhost:8000/api/pipeline-runs/{run_id}/progress", headers={"Authorization": f"Bearer {token}"})
    res3 = urllib.request.urlopen(req3)
    prog = json.loads(res3.read())
    print(f"Final Status: {prog['status']}")

if __name__ == "__main__":
    main()
