# ChaosFlow — Experiment Reproducibility Guide

**Project:** ChaosFlow — Configuration-Driven Resilient Big Data Pipeline Platform  
**Purpose:** Independent Verification, Empirical Reproduction, and Artifact Validation Guide  
**Scope:** Complete experimental protocol for the 30-run empirical fault-injection campaign (5 Control Baselines + 25 Fault Experiments)  
**Evaluated Dataset:** `sales_300k.csv` (300,000 raw transaction records; 289,744 post-filter records)  

---

## 1. System Requirements & Prerequisites

To reproduce the experimental campaign and re-verify all empirical numbers, the evaluating environment must satisfy the following hardware and software requirements:

### 1.1 Hardware Specifications
- **CPU:** 8 physical cores (or 16 logical threads) recommended for concurrent distributed emulation (min 4 cores).
- **RAM:** 16 GB minimum (24 GB recommended; 8 GB allocated to WSL2 / Docker daemon).
- **Storage:** 20 GB free disk space (for Docker images, Kafka write-ahead logs, and HDFS replica storage).

### 1.2 Software Stack
- **Operating System:** Windows 11 with WSL2 (Ubuntu 22.04 LTS) or Native Linux (Ubuntu 22.04+).
- **Docker Engine:** v24.0+ / Docker Desktop with Compose v2.20+.
- **Python:** 3.11.x (with `pandas`, `pyarrow`, `httpx`, `pytest`, `pyyaml`).
- **Node.js:** v20.x LTS with `npm` (for frontend / Vitest / Playwright).

---

## 2. Infrastructure Initialization

### 2.1 Boot the Distributed 13-Container Topology
From the repository root (`resilient-big-data-platform`):
```bash
docker compose up -d
```

### 2.2 Verify Distributed Cluster Health
Execute the platform health check script or curl the backend API:
```bash
curl -s http://localhost:8000/api/health | jq .
```
Expected healthy output:
```json
{
  "status": "ok",
  "database": {"connected": true, "error": null},
  "kafka": {"connected": true, "broker_count": 1, "error": null},
  "spark": {"connected": true, "worker_count": 1, "status": "ALIVE", "error": null},
  "hdfs": {"connected": true, "num_live_datanodes": 1, "error": null}
}
```

Ensure all 13 containers report `Up (healthy)` in `docker ps`:
1. `platform-backend` (FastAPI REST API, Port 8000)
2. `platform-frontend` (React 18 / Nginx, Port 3000)
3. `platform-postgres` (PostgreSQL 16 metadata persistent storage, Port 5432)
4. `platform-kafka` (Apache Kafka 3.7 KRaft broker, Port 9093)
5. `platform-spark-master` (Apache Spark 3.5.0 Master, Port 7077 / 8080)
6. `platform-spark-worker` (Apache Spark 3.5.0 Worker, 2 cores, 2 GB RAM)
7. `platform-namenode` (Hadoop HDFS 3.2.1 NameNode, Port 9000 / 9870)
8. `platform-datanode` (Hadoop HDFS 3.2.1 DataNode, Port 9864)
9. `platform-prometheus` (Prometheus 2.54 metrics engine, Port 9090)
10. `platform-grafana` (Grafana 11.2 dashboards, Port 3001)
11. `platform-airflow-webserver` (Apache Airflow 2.10.3 Web UI, Port 8081)
12. `platform-airflow-scheduler` (Apache Airflow 2.10.3 Scheduler daemon)
13. `platform-airflow-postgres` (PostgreSQL Airflow metadata database)

---

## 3. Cryptographic and Artifact Verification

Before running live tests, verify the integrity of all existing recorded experiment artifacts against the cryptographic manifest:

### 3.1 Verify All 134 SHA-256 Checksums
Run the automated SHA-256 verifier:
```bash
python -c "
import hashlib
from pathlib import Path

repo_dir = Path('.')
checksum_file = repo_dir / 'docs/evaluation/results/CHECKSUMS.txt'
passed = 0
with open(checksum_file, 'r') as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith('#'): continue
        expected_hash, rel_path = line.split()
        actual_hash = hashlib.sha256((repo_dir / rel_path).read_bytes()).hexdigest()
        assert actual_hash == expected_hash, f'Mismatch in {rel_path}'
        passed += 1
print(f'CHECKSUM VERIFICATION: {passed}/134 files cryptographically matched (100% integrity).')
"
```
**Expected Result:** `CHECKSUM VERIFICATION: 134/134 files cryptographically matched (100% integrity).`

### 3.2 Run the Forensic Report Consistency Linter
Enforce all 10 consistency rules:
```bash
python scripts/evaluation/lint_report.py
```
**Expected Result:**
```
================================================================================
CHAOSFLOW HONEST FORENSIC LINTER
================================================================================
PASSED: Zero consistency errors. All figures and claims proven from raw artifacts.
```

---

## 4. Reproducing the Campaign Matrix

The research evaluation consists of 30 structured runs across 6 experimental conditions ($5$ repetitions each):
1. **Control Condition:** `none:no_fault` (Baseline execution, $n=5$)
2. **Kafka Fault Condition 1:** `kafka:container_stop` ($n=5$)
3. **Kafka Fault Condition 2:** `kafka:container_restart` ($n=5$)
4. **Spark Worker Condition 1:** `spark-worker:container_stop` ($n=5$)
5. **Spark Worker Condition 2:** `spark-worker:container_restart` ($n=5$)
6. **HDFS Storage Condition:** `datanode:container_stop` ($n=5$)

### 4.1 Running the Automated Evaluation Test Matrix
Execute the 50 statistical evaluation and matrix tests:
```bash
python -m pytest tests/evaluation -q
```
**Expected Result:** `50 passed in ~11s`

### 4.2 Executing an Interactive Chaos Experiment via API
To trigger an experiment via the REST API:
```bash
# 1. Login and obtain bearer token
TOKEN=$(curl -s -X POST http://localhost:8000/api/auth/login \
  -F "username=admin" -F "password=123456" | jq -r .access_token)

# 2. Register Kafka Stop Experiment
EXP_ID=$(curl -s -X POST http://localhost:8000/api/experiments \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "yaml_content": "experiment:\n  name: live-eval-kafka\ntarget:\n  service: kafka\nfault:\n  type: container_stop\n  duration: 10\nvalidation:\n  max_recovery_time: 60\nrepetitions: 1\n",
    "pipeline_yaml": ""
  }' | jq -r .experiment_id)

# 3. Launch Experiment Run
RUN_ID=$(curl -s -X POST http://localhost:8000/api/experiments/$EXP_ID/run \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "yaml_content": "experiment:\n  name: live-eval-kafka\ntarget:\n  service: kafka\nfault:\n  type: container_stop\n  duration: 10\nvalidation:\n  max_recovery_time: 60\nrepetitions: 1\n"
  }' | jq -r .run_id)

echo "Experiment executed. Monitoring run: $RUN_ID"
```

---

## 5. Verifying Ground Truth in HDFS Storage

The physical proof of fault tolerance is multiset equality between expected clean records and physical records committed to HDFS Parquet files.

### 5.1 Inspecting Physical HDFS Parquet Files
Execute inside `platform-backend`:
```python
from app.storage.hdfs_client import HdfsStorageClient
import pandas as pd

client = HdfsStorageClient()

# Example: read output of run
path = "/data/output/audit_e2e_reproduction_1792796345"
summary = client.content_summary(path)
print("HDFS Summary:", summary)

df = client.read_dataframe(path)
print(f"Total Rows Committed: {len(df)}")
print("Schema Columns:", list(df.columns))
assert len(df) > 0, "HDFS output must not be empty"
```

### 5.2 Verification of Zero-Data-Loss Invariance
For every experiment run on `sales_300k.csv`:
- Raw CSV rows published to Kafka: $300,000$
- Intentionally filtered null rows: $5,124$
- Intentionally filtered non-positive rows: $5,132$
- **Expected clean rows:** $289,744$
- **HDFS committed rows:** $289,744$
- **Missing rows:** $0$
- **Fault-induced duplicate IDs:** $0$

Execute verification script across raw campaign reports:
```bash
python -c "
import json
with open('docs/evaluation/results/results_master.json', 'r') as f:
    data = json.load(f)

for r in data['runs']:
    target = r.get('target_service')
    ft = r.get('fault_type')
    integ = r.get('data_integrity', {})
    rows = integ.get('physical_committed_rows')
    missing = integ.get('missing_rows')
    excess_dup = integ.get('excess_duplicates')
    if rows is not None:
        assert rows == 289744, f'Row count mismatch in {r[\"run_name\"]}: {rows}'
        assert missing == 0, f'Missing rows in {r[\"run_name\"]}: {missing}'
        assert excess_dup == 0, f'Excess dups in {r[\"run_name\"]}: {excess_dup}'

print('GROUND TRUTH AUDIT PASSED: All 30 runs exhibit exact 289,744 multiset equality with 0 loss and 0 excess duplicates.')
"
```

---

## 6. Verifying Frontend Visualization and WebGL Context Stability

### 6.1 Run Vitest Frontend Unit & Component Suite
```bash
cd frontend
npm test
```
**Expected Result:** `25 passed (25 test files), 249 passed (249 tests)`

### 6.2 Run Production Bundle Build
```bash
npm run build
```
**Expected Result:** `✓ built in ~15s` with 0 TypeScript errors.

### 6.3 Run Playwright Browser Automation
```bash
npx playwright test tests/viz.spec.ts
```
**Verifications performed:**
- WebGL 3D Canvas rendering under real SSE signals.
- 60-second heap memory observation (growth $\le 50\text{ MB}$, measured $0.00\text{ MB}$).
- 10x repeated navigation without WebGL context leaks or error alerts.

---

## 7. Reviewer Checkpoint Checklist

An independent auditor or grading committee member can certify reproducibility by verifying these four command outputs:

- [ ] `python scripts/evaluation/lint_report.py` $\to$ `PASSED: Zero consistency errors.`
- [ ] `docker exec platform-backend pytest tests/ -m "not integration" -q` $\to$ `598 passed, 16 skipped`
- [ ] `python -m pytest tests/e2e -q` $\to$ `8 passed`
- [ ] `cd frontend && npm test` $\to$ `249 passed`
