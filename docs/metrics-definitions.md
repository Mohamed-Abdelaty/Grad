# Metric Definitions and Provenance

This document provides a formal definition, mathematical formula, source counters, measurement windows, sampling intervals, direct/derived nature, null conditions, and known limitations for every resilience metric reported by the ChaosLab platform.

---

## 1. Availability

### Definition & Purpose
Measures the operational health and reachability of the targeted service during the active fault window (and during the pre-fault baseline window).

### Measurement Window & Sampling
- **Baseline Window**: Captured during the baseline phase prior to fault injection over `[baseline_start, baseline_end]` (duration controlled by `baseline_seconds`, default: 3.0s, bounded [1, 60]). Probed at 1.0s target intervals.
- **Fault Window**: Captured during the fault execution window over `[fault_injected_at, fault_reverted_at]` (duration set by experiment `fault.duration`). Probed at 1.0s target intervals.
- **Probe Interval & Timeout**: Probing occurs every 1.0s. When a service is unresponsive or terminating, TCP/HTTP deep health checks employ a per-probe timeout of up to 5.0s before recording an unoperational status.
- **Repetitions Dynamic Aggregation**: For multi-repetition experiments ($R > 1$), total observed probes across all child repetitions are accumulated dynamically in the definition provenance.

### Formula
$$\text{Availability (\%)} = \left( \frac{N_{\text{successful\_probes}}}{N_{\text{total\_probes}}} \right) \times 100$$

### Direct or Derived
- **Direct**: Count of boolean probe outcomes against service health endpoints.

### Null Conditions & Reasons
- `normal`: `None` if availability probing failed or was bypassed prior to injection.
- `during_fault`: `None` if no availability probes were captured during the fault window.

### Known Limitations
- **`container_restart` Behavior**: Under `container_restart`, the Docker engine restart API blocks synchronously until the daemon restarts the container into an operational state. Consequently, availability probes dispatched immediately after the call returns will observe 100% availability during the subsequent monitoring window.

---

## 2. Recovery Time (MTTR)

### Definition & Purpose
The elapsed duration in seconds between the moment fault remediation/rollback begins and the moment all health verification checks confirm the target service has returned to fully operational status.

### Measurement Window
- Begins at `revert_started_at` (the initiation of container unpause/start, traffic control teardown, or process cleanup) and ends at `revert_completed_at` upon operational health gate passage.

### Formula
$$\text{Recovery Time (s)} = T_{\text{operational}} - T_{\text{revert\_initiated}}$$

### Direct or Derived
- **Direct**: High-resolution monotonic timer delta recorded by the runner lifecycle.

### Baseline Applicability & Null Conditions
- **Baseline Counterpart**: **None** (`baseline_applicable = False`). Recovery time has no fault-free baseline counterpart by mathematical definition.
- `normal`: `None` with reason `"n/a (by definition)"`.
- `normal_n_samples`: `None`.
- `delta` and `pct_change`: `None`.
- `during_fault`: Real measured elapsed seconds. `None` only if the experiment timed out or failed before recovery could be verified.

---

## 3. Throughput (Processing / Ingestion Rate)

### Definition & Purpose
The rate of record processing or ingestion in records per second across the data pipeline during the baseline and during the fault injection window.

### Source Counters & Provenance
- **Batch Pipelines with Spark**: `spark.recordsOut` (records written by the active Spark driver / stages).
- **Streaming Pipelines with Kafka**: `kafka.published` or `kafka.consumer_records`.
- **Destination Storage**: `hdfs.rowsWritten`.
- **Field Exposing Counter**: Each throughput row in the report explicitly exposes `counter_used` (e.g. `"spark.recordsOut"`) and `pipeline_active_at_injection` (`bool`).

### Measurement Windows & Calculation
- **Normal (Baseline)**: Measured over the pre-fault baseline window `[t_0, t_0 + baseline_seconds]` using delta:
  $$\text{Throughput}_{\text{normal}} = \frac{\Delta \text{Counter}_{\text{baseline}}}{\Delta T_{\text{baseline}}}$$
  Excluded when pipeline is idle prior to injection (`records_processed == 0`).
- **During Fault**: Measured strictly over the active fault window `[fault_injected_at, fault_reverted_at]`:
  $$\text{Throughput}_{\text{during\_fault}} = \frac{\Delta \text{Counter}_{[\text{fault\_injected\_at}, \text{fault\_reverted\_at}]}}{T_{\text{fault\_reverted\_at}} - T_{\text{fault\_injected\_at}}}$$

### Direct or Derived
- **Derived**: Quotient of physical record counter deltas divided by elapsed wall-clock seconds.

### In-Flight Validation & Null Conditions
- **Pipeline Active At Injection**: If the pipeline was not actively processing records when the fault was injected (`pipeline_active_at_injection == False`):
  - `during_fault`: Set to `None` with reason `"pipeline not active at injection"`.
  - A stalled or pre-execution pipeline is never recorded as 0.0 records/sec, preventing false attribution of zero throughput to fault impact.
- **No Pipeline Linked**: When an experiment runs standalone without a linked pipeline:
  - `normal`: `None` with reason `"no pipeline linked"`.
  - `during_fault`: `None` with reason `"no pipeline linked"`.
- **Pipeline Idle in Baseline**:
  - `normal`: `None` with reason `"pipeline idle / no progress in baseline window"`.

---

## 4. Data Loss

### Definition & Purpose
The quantity of records lost, dropped, or corrupted as a result of the injected infrastructure fault, evaluated by comparing verified pre-fault/engine expected counts against actual persistent destination storage counts after recovery.

### Independence of Expected vs Actual
- **Expected Records ($R_{\text{expected}}$)**: Measured from an independent source: verified source records or Spark driver output records (`spark.recordsOut` before post-fault storage sink write).
- **Actual Records ($R_{\text{actual}}$)**: Physically counted from the destination storage system (e.g., counting rows in HDFS Parquet partition files via WebHDFS) **after** full service recovery.

### Formula
$$\text{Data Loss} = \max\left(0, R_{\text{expected}} - R_{\text{actual}}\right)$$

### Direct or Derived
- **Derived**: Non-negative difference between independent expected count and physical post-recovery count.

### Baseline Applicability & Null Conditions
- **Baseline Counterpart**: **None** (`baseline_applicable = False`). Data loss is a fault-induced delta; there is no baseline data loss in normal fault-free execution by definition.
- `normal`: `None` with reason `"n/a (by definition)"`.
- `normal_n_samples`: `None`.
- `delta` and `pct_change`: `None`.
- `during_fault`: Real integer record count. `None` with reason `"no pipeline linked"` or `"not measured"` if destination verification could not execute.

---

## 5. Duplicate Records (Fault-Induced Duplicates)

### Definition & Purpose
The count of duplicate records introduced strictly by fault injection, network retries, or at-least-once message reprocessing, excluding duplicates inherently present in the source input dataset.

### Source vs Destination Provenance
- **Source Duplicates ($D_{\text{source}}$)**: Directly calculated by scanning the pipeline's raw source file or input topic for duplicate unique identification keys (`id_column`, e.g., `transaction_id`).
- **Output Duplicates ($D_{\text{output}}$)**: Directly calculated by scanning physical destination files (e.g., HDFS Parquet files) after pipeline completion for duplicate identification keys.
- **Reporting Columns**: The report row exposes `source_duplicates`, `output_duplicates`, and `fault_induced_duplicates`.

### Formula
$$\text{Fault Induced Duplicates} = \max\left(0, D_{\text{output}} - D_{\text{source}}\right)$$
*Condition: Calculated ONLY when both $D_{\text{output}}$ and $D_{\text{source}}$ are non-null real numbers.*

### Direct or Derived
- **Derived**: Delta between physical output duplicate keys and source input duplicate keys.

### Baseline Applicability & Null Conditions
- **Baseline Counterpart**: Not measured during the baseline phase (`baseline_applicable = False`).
- `normal`: `None` with reason `"not measured during baseline"`.
- `normal_n_samples`: `None`.
- `delta` and `pct_change`: `None`.
- `during_fault`: Set to `fault_induced_duplicates` when measured; `None` with reason `"source or output duplicates unmeasured"` if either scan is unavailable.

---

## Summary Comparison Matrix

| Metric | Normal (Baseline) | Normal Samples ($N$) | During Fault | Delta / Pct Change | Counter / Source |
|---|---|---|---|---|---|
| **Availability** | Real \% (or `None`) | $N \ge 1$ (or `None`) | Real \% | Real (if normal $\ne 0$) | Probing engine (1s interval) |
| **Throughput** | Real rate (or `None`) | $N \ge 1$ (or `None`) | Real rate (or `None` if inactive) | Real (if normal $\ne 0$) | `spark.recordsOut` / `kafka.published` |
| **Data Loss** | `None` ("n/a (by definition)") | `None` | Real record count (or `None`) | `None` | Spark driver vs HDFS physical scan |
| **Duplicate Records**| `None` ("not measured") | `None` | Real count (or `None`) | `None` | HDFS output ID scan minus source ID scan |
| **Recovery Time** | `None` ("n/a (by definition)") | `None` | Real elapsed seconds | `None` | Monotonic recovery stopwatch |
