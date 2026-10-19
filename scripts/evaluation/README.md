# ChaosFlow Evaluation Harness — Graduation Research

This directory contains the automated, reproducible evaluation harness for the ChaosFlow platform's graduation research evaluation phase.

## Experimental Design Note

> **Baseline Design Statement**: There is no standalone "fault-free control" experiment. The experimental design evaluates resilience through within-run, phase-by-phase comparison: a calibrated baseline window (`[run_start, fault_injected_at]`) measures unperturbed pipeline throughput and component availability, against which during-fault degradation (`[fault_injected_at, fault_reverted_at]`) and post-fault recovery time (`[fault_reverted_at, recovered_at]`) are directly benchmarked.

---

## Architecture & Guarantees

1. **REST API Exclusivity**: All interactions (experiment creation, pipeline registration, execution triggering, metrics retrieval) run strictly through the platform's public REST API (`/api/experiments`, `/api/pipelines`, `/api/reports`). No backend backdoors or raw database mutations are performed during evaluation.
2. **100% Kafka Topic Isolation**: To prevent cross-repetition contamination (e.g. repetition 2 draining leftover records from repetition 1), every repetition is submitted as an independent experiment (`eval-<experiment_id>-r<rep_idx>`) with a fresh, dedicated Kafka topic (`eval-stream-<experiment_id>-r<rep_idx>`) and independent HDFS sink directory.
3. **Strict Quiescence & Cool-Down Verification**:
   - Asserts `/health` status is 200 OK across all platform components.
   - Asserts active pipeline runs == 0.
   - Asserts pending/running experiment runs == 0.
   - Asserts Spark worker cores are completely free (`coresUsed == 0` via Spark Master JSON API).
   - Asserts consumer lag on previous evaluation topic == 0.
   - Asserts free host RAM >= 1500 MB.
   - If any check fails to settle after 3 minutes, the harness immediately **ABORTS** the matrix run.
4. **Resumable & Non-Destructive**: Checks for existing `report.json` in `docs/evaluation/raw/<name>/` before running. Existing successful runs are never overwritten, re-executed, or deleted.
5. **Verbatim Provenance Artifacts**: Each repetition generates:
   - `report.json`: Full API response from `/api/reports/{id}` including comparison table.
   - `children.json`: Raw child run records with measured availability, throughput, data loss, and recovery time.
   - `environment.json`: Git commit hash, timestamps, target service, fault parameters, host free RAM before/after, Spark cores free, Kafka data size, topic count, total message count, and full docker stats snapshot.

---

## Directory Structure

```
scripts/evaluation/
├── matrix.yaml          # Evaluation matrix: 5 fault scenarios, baseline, fault durations
├── run_evaluation.py    # Python CLI evaluation runner
├── aggregate.py         # Deterministic statistical aggregator (computes n, mean, sample std N-1, min, max)
└── README.md            # Usage and architectural documentation
```

---

## Prerequisites

1. **Active Stack**: Ensure all 13 Docker Compose services are healthy (`docker compose ps`).
2. **Host RAM**: A minimum of 1500 MB free physical memory is enforced by the harness. Close non-essential desktop applications (e.g. Chrome, Claude) during test runs to ensure stability.
3. **Environment Credentials**: Set credentials in your environment or use the defaults:
   ```bash
   export ADMIN_USERNAME="admin"
   export ADMIN_PASSWORD="your_password"
   ```

---

## CLI Usage

### 1. Run the Evaluation Matrix Tests
Before running live experiments, execute the harness and aggregator unit test suite:
```powershell
py -3.11 -m pytest tests/evaluation/ -v
```

### 2. Execute Phase 2 Pilot (1 Experiment, 2 Repetitions)
To run the pilot scenario (`kafka-stop`, 2 repetitions) to verify end-to-end orchestration and provenance storage:
```powershell
py -3.11 scripts/evaluation/run_evaluation.py --pilot
```

### 3. Execute the Full Evaluation Matrix (5 Experiments, 5 Repetitions)
To execute all 25 runs defined in `matrix.yaml`:
```powershell
py -3.11 scripts/evaluation/run_evaluation.py
```

### 4. Aggregate Results into Deterministic Summary CSV
To aggregate all completed runs across scenarios:
```powershell
py -3.11 scripts/evaluation/aggregate.py --raw-dir docs/evaluation/raw --output docs/evaluation/summary_table.csv
```

---

## Matrix Experiment Configurations

| Experiment ID | Target Service | Fault Type | Injected Duration | Baseline Window | Description |
|---|---|---|---|---|---|
| `kafka-stop` | `kafka` | `container_stop` | 20s | 10s | Broker stoppage; measures backpressure & consumer lag. |
| `kafka-restart` | `kafka` | `container_restart` | 20s | 10s | Transient broker restart dynamics & KRaft recovery. |
| `spark-worker-stop` | `spark-worker` | `container_stop` | 20s | 10s | Compute worker loss; measures stream task rescheduling. |
| `spark-worker-restart` | `spark-worker` | `container_restart` | 20s | 10s | Worker restart & executor reconnection resilience. |
| `datanode-stop` | `datanode` | `container_stop` | 20s | 10s | Storage block host halt; sink write failure semantics. |

> **Note on Network Emulation**: `platform-kafka` uses the `confluentinc/cp-kafka:7.6.0` image which does not package Linux `iproute2` (`tc`). Therefore, `service_delay` is not scheduled against Kafka and container-level faults are used.
