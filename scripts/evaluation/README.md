# ChaosFlow Evaluation Harness — Graduation Research

This directory contains the automated, reproducible evaluation harness for the ChaosFlow platform's graduation research evaluation phase.

## Architecture & Guarantees

1. **REST API Exclusivity**: All interactions (experiment creation, pipeline registration, execution triggering, metrics retrieval) run strictly through the platform's public REST API (`/api/experiments`, `/api/pipelines`, `/api/reports`). No backend backdoors or raw database mutations are performed during evaluation.
2. **100% Kafka Topic Isolation**: To prevent cross-repetition contamination (e.g. repetition 2 draining leftover records from repetition 1), every repetition is submitted as an independent experiment (`eval-<experiment_id>-r<rep_idx>`) with a fresh, dedicated Kafka topic (`eval-stream-<experiment_id>-r<rep_idx>`) and independent HDFS sink directory.
3. **Quiescence & Cool-Down Verification**:
   - Asserts `/health` status is 200 OK across all platform components.
   - Asserts active pipeline runs == 0 and running experiment runs == 0.
   - Enforces a pre-flight free RAM safety check on the host OS.
   - Sleeps for the configured cool-down duration (default: 15s) between repetitions.
4. **Resumable & Non-Destructive**: Checks for existing `report.json` in `docs/evaluation/raw/<name>/` before running. Existing successful runs are never overwritten, re-executed, or deleted.
5. **Verbatim Provenance Artifacts**: Each repetition generates:
   - `report.json`: Full API response from `/api/reports/{id}` including comparison table.
   - `children.json`: Raw child run records with measured availability, throughput, data loss, and recovery time.
   - `environment.json`: Git commit hash, timestamps, target service, fault parameters, and memory state.

---

## Directory Structure

```
scripts/evaluation/
├── matrix.yaml          # Evaluation matrix: 5 fault scenarios, baseline, fault durations
├── run_evaluation.py    # Python CLI evaluation runner
└── README.md            # Usage and architectural documentation
```

---

## Prerequisites

1. **Active Stack**: Ensure all 13 Docker Compose services are healthy (`docker compose ps`).
2. **Host RAM**: A minimum of 500 MB (default threshold in `matrix.yaml`) free physical memory is recommended. For maximum stability on 16 GB machines, close non-essential desktop applications (e.g. Chrome, Claude) during test runs.
3. **Environment Credentials**: Set credentials in your environment or use the defaults:
   ```bash
   export ADMIN_USERNAME="admin"
   export ADMIN_PASSWORD="your_password"
   ```

---

## CLI Usage

### 1. Run the Evaluation Matrix Tests
Before running live experiments, execute the harness unit and contract test suite:
```powershell
py -3.11 -m pytest backend/tests/evaluation/test_evaluation_harness.py -v
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

### CLI Arguments
- `--matrix <path>`: Path to custom matrix configuration YAML (default: `scripts/evaluation/matrix.yaml`).
- `--pilot`: Execute only the first experiment for 2 repetitions (pilot mode).
- `--repetitions <N>`: Override the repetition count configured in `matrix.yaml`.
- `--base-url <url>`: Override the platform API base URL (default: `http://localhost:8000`).

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
