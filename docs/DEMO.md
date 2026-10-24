# ChaosFlow Defense Demonstration Script

This document provides a concise, structured 10-step script for defending the **Configuration-Driven Resilient Big Data Pipeline Platform (ChaosFlow)** during graduation / oral examination.

---

## Prerequisites

Before starting the demonstration, verify that all cluster components and prerequisites are ready:

1. **Host Environment & Containers**:
   All 13 containers must be up and healthy:
   ```powershell
   docker compose ps
   ```
   *Required Containers*:
   - `platform-backend` (FastAPI API server on port 8000)
   - `platform-frontend` (Nginx + React SPA on port 3000)
   - `platform-postgres` (PostgreSQL 16 on port 5432)
   - `platform-kafka` (Apache Kafka KRaft on port 9092)
   - `platform-spark-master` (Spark Master on port 7077, Web UI 8080)
   - `platform-spark-worker` (Spark Worker, 4 cores, 2GB memory)
   - `platform-namenode` (Hadoop HDFS NameNode on port 9870 / 9000)
   - `platform-datanode` (Hadoop HDFS DataNode on port 9864)
   - `platform-prometheus` (Prometheus on port 9090)
   - `platform-grafana` (Grafana on port 3001)
   - `platform-airflow-webserver`
   - `platform-airflow-scheduler`
   - `platform-airflow-postgres`

2. **Required Data Files**:
   Ensure sample datasets are mounted inside the containers:
   - `backend/sample-data/sales_sample.csv` (Baseline small batch)
   - `backend/sample-data/sensor_readings.json` (Generic IoT batch)
   - `backend/sample-data/sales_300k.csv` (Campaign streaming dataset)

3. **Authentication Credentials**:
   Credentials are auto-generated and stored in `.env` in the project root:
   ```powershell
   Get-Content .env | Select-String "ADMIN_"
   ```
   Default development fallback credentials:
   - **Username**: `admin`
   - **Password**: `admin` (or generated password from `.env`)

---

## 10-Step Defense Script

### Step 1: Cluster Health & Container Status Verification
- **Command**:
  ```powershell
  curl -s http://localhost:8000/api/system/status
  ```
- **Expected Output**:
  ```json
  {"status":"healthy","database":{"connected":true},"kafka":{"connected":true},"spark":{"connected":true},"hdfs":{"connected":true}}
  ```
- **Fallback**: If any service shows `false`, restart the container:
  ```powershell
  docker compose restart <service-name>
  ```

### Step 2: Access & Secure Authentication
- **Action**: Open `http://localhost:3000/login` in the browser. Enter credentials and click **Login**.
- **Expected Output**: HTTP 200 with JWT access token; browser redirects to `/` displaying the main Dashboard with active system cards.
- **Fallback**: If login fails with 401, check `.env` for the exact `ADMIN_USERNAME` and `ADMIN_PASSWORD`.

### Step 3: Platform KPI and Real-Time Dashboard Audit
- **Action**: Inspect the 4 health cards and KPI row on the Dashboard.
- **Expected Output**: 4 green badges (Database, Kafka, Spark, HDFS). Recent pipeline run success rate and MTTR metrics visible.

### Step 4: Declarative Pipeline Authoring & Schema Validation
- **Action**: Navigate to `/pipelines`. Click **Templates** -> select `sales-csv`. Click **Validate**.
- **Expected Output**: Syntax-highlighted YAML loads. Validation panel indicates `Valid` with green checkmark and 0 schema violations.
- **Fallback**: If validation errors appear, click **Templates** -> `sales-csv` again to reset to pristine YAML.

### Step 5: Batch Pipeline Submission & Real-Time Telemetry
- **Action**: Click **Run Pipeline**.
- **Expected Output**: Execution status transitions from `pending` -> `running`. Interactive 3D scene (or 2D canvas fallback) displays flowing data blocks across stages (Source -> Kafka -> Spark -> HDFS).

### Step 6: End-to-End Batch Pipeline Completion & Parity Check
- **Action**: Wait ~15 seconds for run completion. Click **Output** tab.
- **Expected Output**: Pipeline status turns `succeeded`. Output row count matches processed input count. Tabular preview renders first 500 rows.

### Step 7: ChaosLab Fault Injection Configuration
- **Action**: Navigate to `/reliability`. Click **New Experiment**. Configure:
  - **Target**: `kafka`
  - **Fault Type**: `container_stop`
  - **Duration**: `20` seconds
  - **Max Allowed Recovery**: `60` seconds
- **Expected Output**: Safety validator approves parameters. Wizard proceeds to summary review.

### Step 8: Fault Injection Execution & Live Outage Observation
- **Action**: Click **Confirm & Start Experiment**.
- **Expected Output**:
  - Live monitor shows status `injecting`.
  - Kafka container stops (`docker inspect platform-kafka --format '{{.State.Status}}'` returns `exited`).
  - Active countdown timer displays fault window.

### Step 9: Automatic Cluster Self-Healing & Recovery Verification
- **Action**: Observe the live monitor after 20 seconds.
- **Expected Output**:
  - Platform automatically issues container restart / start.
  - Kafka container returns to `running` state.
  - Status updates to `waiting_for_recovery` then `succeeded`.
  - MTTR is calculated (typically 21–35s, well under 60s SLA).

### Step 10: Resilience Audit Report & Metric Drilldown
- **Action**: Click **View Report** on the completed experiment.
- **Expected Output**: Comprehensive resilience audit report displays PASS badge, recovery timeline, availability degradation graph, and zero data loss confirmation.

---

## Emergency Fallback Matrix

| Issue | Quick Diagnostic | One-Line Remediation |
|---|---|---|
| Kafka container fails to start | `docker logs platform-kafka` | `docker compose restart platform-kafka` |
| Spark worker disconnected | `docker logs platform-spark-worker` | `docker compose restart platform-spark-worker` |
| HDFS SafeMode active | `docker exec platform-namenode hdfs dfsadmin -safemode get` | `docker exec platform-namenode hdfs dfsadmin -safemode leave` |
| WebGL Canvas disabled | Browser GPU acceleration | Platform automatically renders 2D SVG fallback (`VizErrorBoundary`) |
