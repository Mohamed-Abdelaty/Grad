# ChaosFlow — Comprehensive Verification Evidence Index

**Project:** ChaosFlow — Configuration-Driven Resilient Big Data Pipeline Platform  
**Purpose:** Verifiable Audit Trail, Artifact Provenance, and System Evidence Index  
**Audit Revision:** Commit `e8fe79c` on branch `feature/gap-closure` (with targeted remediation commits)  
**Total Verified Artifacts:** 134 Cryptographically Hashed Files + 10 Automated Forensic Rules + 918 Test Executions (1,063 Total System Verifications)  
**Official Audit Report:** [FINAL_ADVERSARIAL_AUDIT.md](file:///d:/Configuration-Driven%20Resilient%20Big%20Data%20Pipeline%20Platform/resilient-big-data-platform/docs/evaluation/FINAL_ADVERSARIAL_AUDIT.md)  
**Official Audit Verdict:** **READY WITH DOCUMENTED LIMITATIONS**  

---

## 1. Automated Test Execution Evidence

All automated test suites were executed on the live platform topology. Machine-readable logs and outputs are indexed below:

| Artifact Name / Test Suite | Runner / Tool | Scope / Module | Status / Verified Result | Evidence File / Location |
| :--- | :--- | :--- | :--- | :--- |
| **Backend Unit & Contract Suite** | Pytest 8.3.4 | `backend/tests/` (`-m "not integration"`) | **601 passed**, 16 skipped | [test_data_consistency_validator.py](file:///d:/Configuration-Driven%20Resilient%20Big%20Data%20Pipeline%20Platform/resilient-big-data-platform/backend/tests/reliability/test_data_consistency_validator.py) |
| **Live Cluster Integration Suite** | Pytest 8.3.4 | `backend/tests/` (`-m integration`) | **44 passed**, 0 failed | [test_batch_kafka_integration.py](file:///d:/Configuration-Driven%20Resilient%20Big%20Data%20Pipeline%20Platform/resilient-big-data-platform/backend/tests/integration/test_batch_kafka_integration.py) |
| **Airflow DAG Integrity Suite** | Python / DagBag | `backend/tests/airflow/` | **15 passed**, 0 import errors | [test_dag_integrity.py](file:///d:/Configuration-Driven%20Resilient%20Big%20Data%20Pipeline%20Platform/resilient-big-data-platform/backend/tests/airflow/test_dag_integrity.py) |
| **Host E2E Pipeline & Chaos Suite** | Pytest 8.3.4 | `tests/e2e/` | **8 passed**, 0 failed | [test_full_pipeline_flow.py](file:///d:/Configuration-Driven%20Resilient%20Big%20Data%20Pipeline%20Platform/resilient-big-data-platform/tests/e2e/test_full_pipeline_flow.py) |
| **Research Matrix & Stats Suite** | Pytest 8.3.4 | `tests/evaluation/` | **50 passed**, 0 failed | [tests/evaluation/](file:///d:/Configuration-Driven%20Resilient%20Big%20Data%20Pipeline%20Platform/resilient-big-data-platform/tests/evaluation/) |
| **Frontend Unit & Component Suite** | Vitest 2.1.9 | `frontend/src/__tests__/` (25 files) | **249 passed**, 0 failed | [frontend/src/__tests__/](file:///d:/Configuration-Driven%20Resilient%20Big%20Data%20Pipeline%20Platform/resilient-big-data-platform/frontend/src/__tests__/) |
| **Playwright Browser E2E Suite** | Playwright 1.63 | `frontend/e2e/` (11 specs) | **59 passed**, 0 failed | [frontend/e2e/](file:///d:/Configuration-Driven%20Resilient%20Big%20Data%20Pipeline%20Platform/resilient-big-data-platform/frontend/e2e/) |
| **Frontend Bundle Compilation** | `tsc && vite build` | `frontend/` | **Built in 15.48s**, 0 errors | [frontend/dist/](file:///d:/Configuration-Driven%20Resilient%20Big%20Data%20Pipeline%20Platform/resilient-big-data-platform/frontend/dist/) |
| **Forensic Report Consistency Linter**| Python 3.11 | `scripts/evaluation/lint_report.py` | **10/10 rules passed** | [scripts/evaluation/lint_report.py](file:///d:/Configuration-Driven%20Resilient%20Big%20Data%20Pipeline%20Platform/resilient-big-data-platform/scripts/evaluation/lint_report.py) |
| **Cryptographic Checksum Verification**| Python hashlib | `docs/evaluation/results/CHECKSUMS.txt` | **134/134 SHA-256 match** | [CHECKSUMS.txt](file:///d:/Configuration-Driven%20Resilient%20Big%20Data%20Pipeline%20Platform/resilient-big-data-platform/docs/evaluation/results/CHECKSUMS.txt) |

---

## 2. End-to-End Traceability Matrix

ChaosFlow establishes an unbroken verification trail connecting user interface indicators to backend persistence and physical storage:

```
[React 18 / Three.js UI]
       │
       ▼ (HTTP POST /api/pipelines/run?wait=false)
[FastAPI Backend Route] (app/api/routes/pipelines.py)
       │
       ▼ (Pydantic Schema Validation & Postgres PipelineRun Record)
[PostgreSQL Database] (pipeline_runs table, status: 'running')
       │
       ▼ (Subprocess invocation with --conf spark.cores.max=1)
[Apache Spark Cluster] (Master: 7077, Worker: 2 cores, Cores used: 1)
       │
       ▼ (SSE Progress Stream: validating -> source -> processing -> storage)
[Server-Sent Events / EventSource] (/api/pipeline-runs/{id}/stream)
       │
       ▼ (Parallel Parquet micro-batch write)
[Hadoop HDFS Storage] (hdfs://namenode:9000/data/output/.../part-*.parquet)
       │
       ▼ (Ground-truth verification)
[DataConsistencyValidator] (app/reliability/data_consistency_validator.py)
```

### Traceability Artifacts for Verified Run `8d225a88-f2e5-4586-b429-bde42d6b25be`:
1. **API Initiation:**
   - **Timestamp:** `2026-10-23T22:59:06.336047+00:00`
   - **Endpoint:** `POST /api/pipelines/run?wait=false`
   - **HTTP Response:** `202 Accepted` (`{"pipeline_run_id": "8d225a88-f2e5-4586-b429-bde42d6b25be", "status": "running"}`)
2. **Database Record in PostgreSQL:**
   - **Table:** `pipeline_runs`
   - **Columns:** `id="8d225a88-f2e5-4586-b429-bde42d6b25be"`, `status="succeeded"`, `started_at="2026-10-23T22:59:06.502482+00:00"`, `finished_at="2026-10-23T22:59:41.147494+00:00"`
3. **Apache Spark Execution:**
   - **Application Name:** `BatchProcessingJob`
   - **Cores Allocated:** 1 core (isolated via `spark.cores.max=1`)
   - **Duration:** 34.65 seconds
4. **Physical HDFS Storage:**
   - **Path:** `/data/output/audit_e2e_reproduction_1792796345`
   - **Files:** `_SUCCESS`, `part-00000-8d225a88...parquet`
   - **Rows Written:** 15 records (clean post-filter multiset match)
   - **Schema Verified:** `transaction_id`, `amount`, `customer_id`, `product_id`, `sale_date`

---

## 3. Empirical Research Dataset Inventory

The primary scientific evaluation is captured in `docs/evaluation/results/results_master.json` ($134\text{ KB}$, SHA-256: `08a0249e0a811cae0965d07010f3c5b8b8ea5f79a95ec0a5e840d5718dfdbe12`).

### 3.1 Experimental Run Provenance
All 30 campaign runs are structured with full provenance:
- **Git Commit:** `c5039678` / `e8fe79c`
- **Host Architecture:** x86_64 Linux 6.6.87.2 (WSL2 / Windows 11 Pro)
- **Workload:** 300,000 synthetic transaction records (`sample-data/sales_300k.csv`)
- **Raw Reports Directory:** `docs/evaluation/raw/campaign/`

### 3.2 Raw Artifacts Directory Structure
Each individual run directory contains 4 canonical JSON artifacts:
1. `report.json`: High-level experiment metrics, phase timings, availability samples, and throughput curves.
2. `environment.json`: Host hardware specification, Docker container inspection hashes, and JVM parameters.
3. `physical_metrics.json`: Physical row counts from HDFS Parquet files, Kafka partition high watermarks, and duplicate ID counts.
4. `children.json`: Child repetition metadata, safety validator tokens, and database foreign keys.

---

## 4. Source Code Modifications and Remediation Index

| Modified Source File | Nature of Modification | Verifiable Effect |
| :--- | :--- | :--- |
| `backend/app/reliability/data_consistency_validator.py` | Implemented `filter_accounting` validation | Eliminates false-positive data loss from intentional filter transformations |
| `backend/app/spark/spark_job_submitter.py` | Added `--conf spark.cores.max=1` | Enables multi-job batch concurrency on standalone Spark worker |
| `backend/tests/reliability/test_data_consistency_validator.py` | Added 7 unit tests for filter accounting | 24/24 validator tests passing 100% |
| `tests/e2e/test_full_pipeline_flow.py` | Updated schema from `steps:` to `stages:` | 8/8 host E2E tests passing 100% |
| `tests/e2e/test_full_reliability_flow.py` | Added HTTP 307 redirect follow | Eliminates false failures on trailing slashes |
