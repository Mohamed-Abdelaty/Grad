import urllib.request
import json
import time
import subprocess

def main():
    print("--- 1. Login ---")
    req = urllib.request.Request("http://localhost:3000/api/auth/login", data=b"username=admin&password=REDACTED", headers={"Content-Type": "application/x-www-form-urlencoded"})
    res = urllib.request.urlopen(req)
    token = json.loads(res.read())["access_token"]

    with open("backend/sample-configs/sales_ui2b.yaml", "r") as f:
        yaml_str = f.read()

    print("\n--- 2. POST /run?wait=false ---")
    req2 = urllib.request.Request("http://localhost:3000/api/pipelines/run?wait=false", data=json.dumps({"yaml_content": yaml_str}).encode(), headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    res2 = urllib.request.urlopen(req2)
    run_id = json.loads(res2.read())["pipeline_run_id"]
    print(f"Run ID: {run_id}")

    time.sleep(5) # wait for spark to start

    print("\n--- 3. PS Before Cancel (Spark Active) ---")
    ps_before = subprocess.check_output(["docker", "exec", "platform-backend", "ps", "-ef"]).decode()
    for line in ps_before.splitlines():
        if "spark" in line or "java" in line:
            print(line)
            
    print("\n--- 4. HDFS /tmp Listing Before Cancel ---")
    hdfs_before = subprocess.check_output(["docker", "exec", "platform-namenode", "hdfs", "dfs", "-ls", "/tmp"]).decode()
    print(hdfs_before.strip())

    print("\n--- 5. POST /cancel ---")
    req3 = urllib.request.Request(f"http://localhost:3000/api/pipeline-runs/{run_id}/cancel", method="POST", headers={"Authorization": f"Bearer {token}"})
    try:
        urllib.request.urlopen(req3)
        print("Cancel triggered.")
    except Exception as e:
        print(f"Cancel failed: {e}")

    time.sleep(5)

    print("\n--- 6. Status After Cancel ---")
    req4 = urllib.request.Request(f"http://localhost:3000/api/pipeline-runs/{run_id}/progress", headers={"Authorization": f"Bearer {token}"})
    res4 = urllib.request.urlopen(req4)
    print(json.dumps(json.loads(res4.read())))

    print("\n--- 7. PS After Cancel ---")
    ps_after = subprocess.check_output(["docker", "exec", "platform-backend", "ps", "-ef"]).decode()
    spark_alive = False
    for line in ps_after.splitlines():
        if "spark" in line or "java" in line:
            if "spark-submit" in line:
                spark_alive = True
            print(line)
    if not spark_alive:
        print("No spark-submit processes found.")
        
    print("\n--- 8. Backend Liveness Check ---")
    try:
        h_req = urllib.request.Request("http://localhost:8000/api/health")
        h_res = urllib.request.urlopen(h_req)
        print(f"Backend is ALIVE. Health: {h_res.read().decode()}")
    except Exception as e:
        print(f"Backend is DEAD! {e}")

    print("\n--- 9. HDFS /tmp Listing After Cancel ---")
    try:
        hdfs_after = subprocess.check_output(["docker", "exec", "platform-namenode", "hdfs", "dfs", "-ls", "/tmp"]).decode()
        print(hdfs_after.strip())
    except subprocess.CalledProcessError:
        print("HDFS ls failed.")

if __name__ == "__main__":
    main()
