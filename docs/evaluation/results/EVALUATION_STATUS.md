# Batch Evaluation Status — feature/gap-closure

**HEAD**: af8ecc0  
**Branch**: feature/gap-closure  
**Last Updated**: 2026-10-24

---

## Completed ✅

| Task | Evidence |
|------|----------|
| Clean shape (1M rows) | `docs/evaluation/results/batch_clean_result.json` — run_id `de771bd9`, status `succeeded`, 1,000,000 HDFS output rows, 110.86s |
| Dataset generation (4 shapes) | `backend/sample-data/` — all four ~124 MB CSV files present |
| Dataset validation (Python CSV scan) | `with_nulls`: 20,000 null rows → expect 980,000 output; `with_duplicates`: 5,000 dup IDs → expect 995,000; `edge_cases`: 1,000 zero-amount → expect 999,000 |
| Pipeline YAML configs | `backend/sample-configs/sales_large_{clean,nulls,duplicates,edge}.yaml` |
| Timeout-bounded eval script | `scripts/run_batch_evaluation.py` — MAX_POLL_SECONDS=600, MAX_POLL_ERRORS=10, HTTP_TIMEOUT=15 |
| Test runner with timeouts | `scripts/run_all_tests_with_timeout.py` — SUITE_TIMEOUT_SECONDS=300 |
| Bug fix: uvicorn --reload | `docker-compose.yml` now uses `--workers 1` (prevents WatchfilesRustInternalError OOM during Spark) |
| Bug fix: data_consistency_validator | Added `filter_accounting` param + `expected_records: int | None` |
| Bug fix: spark_job_submitter | `spark.cores.max` config now applies to all jobs (not just Kafka) |
| Baseline test counts | Backend: 601 passed, 16 skipped; Eval: 50 passed; Frontend: 248/249 |
| Git commit | af8ecc0 — all changes committed to feature/gap-closure |

---

## Pending — REQUIRES DOCKER RUNNING ⚠️

The following steps require Docker Desktop to be open in the user's Windows session:

| Step | Command |
|------|---------|
| Run with_nulls batch | `python scripts/run_batch_evaluation.py` (loads clean from disk, runs 3 remaining shapes) |
| Run with_duplicates batch | (same script) |
| Run edge_cases batch | (same script) |
| Verify HDFS row counts | Script auto-verifies via WebHDFS at port 9870 |
| Run backend unit tests | `docker exec platform-backend pytest tests/ -q -m "not integration and not e2e"` |
| Run evaluation tests | `docker exec platform-backend pytest tests/evaluation/ -q` |
| Final commit (docs) | After batch results are recorded |

---

## Bug Fixed This Session: Infinite Polling Loop

**Root Cause**: `scripts/run_batch_evaluation.py` line 76 had `while True:` with no ceiling.  
**Fix applied**: Added `MAX_POLL_SECONDS = 600`, `MAX_POLL_ERRORS = 10`, `HTTP_TIMEOUT = 15`.  
**Location**: [`scripts/run_batch_evaluation.py`](../../../scripts/run_batch_evaluation.py)  
**Committed**: af8ecc0

---

## Docker Desktop Startup Issue

Docker Desktop must be started manually by the user **before** running any Docker-dependent commands.  
**Cause**: Docker Desktop uses a named pipe `\\.\pipe\dockerDesktopLinuxEngine` that only exists in the user's interactive Windows GUI session. Automated shell sessions cannot inherit it.

**Start command**:
```powershell
Start-Process "C:\Users\user\AppData\Local\Programs\DockerDesktop\Docker Desktop.exe"
# Wait ~60 seconds for all containers to become healthy
docker ps
```
