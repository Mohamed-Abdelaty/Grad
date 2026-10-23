# Evaluation Section Notes: Author Guidance & Threats to Validity

**Document Purpose**: Concise, forensic reference for authors drafting the ChaosFlow resilience evaluation paper section. All statements, figures, and limitations are strictly grounded in committed raw artifacts and recomputed master results.

---

## 1. Summary of Proven Findings vs. Hypotheses vs. Unverified Claims

| Topic / Metric | Status | Evidence / Repository Artifact | Key Note for Paper |
| :--- | :--- | :--- | :--- |
| **Broker Outage Zero Physical Loss** (`kafka-stop`, `kafka-restart`) | **PROVEN** | `docs/evaluation/results/five_column_multiset_results.json` | 100% zero data loss across 10 repetitions ($N=10$). Exactly 289,744 committed rows; 0 missing, 0 extra, 0 excess duplicate records across 5-column multiset. |
| **Worker Failure Zero Physical Loss** (`spark-worker-stop`, `spark-worker-restart`) | **PROVEN** | `docs/evaluation/results/five_column_multiset_results.json` | 100% zero data loss across 10 repetitions ($N=10$). Exactly 289,744 committed rows; 0 missing, 0 extra, 0 excess duplicate records. |
| **Single-Replica Storage Failure Mode** (`datanode-stop`) | **PROVEN** | `docs/evaluation/results/derived/datanode_crashes_evidence.json` | 2 of 5 runs crashed (r2, r5) terminating at ~55k rows (~234.7k rows missing). Query terminated with `org.apache.hadoop.ipc.RemoteException: File ... could only be written to 0 of the 1 minReplication nodes`. |
| **Multi-Node Failover Behavior** (`dfs.replication=3`) | **NOT EVALUATED** | Hadoop HDFS Specification | Single-node deployment (`dfs.replication=1`) tested. Multi-node block replica failover without query termination is expected per Hadoop architecture but was NOT evaluated in this testbed. |
| **Dual Recovery Gap (Container vs Application)** | **PROVEN** | `docs/evaluation/results/results_master.json` | Container health checks pass rapidly (~0.48–1.47s), but application-level 90% throughput restoration requires ~17.56s (worker stop) and ~22.89s (kafka stop). |
| **Platform Validator Bias (10,256 constant loss)** | **PROVEN** | `backend/app/reliability/data_consistency_validator.py:71-95` | Built-in validator compares 300,000 raw input rows directly against HDFS output rows without filter awareness (`remove_nulls` and `amount > 0` remove 10,256 rows). |
| **Control Run Validator Execution** | **PROVEN** | `docs/evaluation/raw/control/eval-control-r*/report.json:dataLoss` | Control runs did NOT execute the validator (`dataLoss: null`). Table verdict must state "not run". |
| **10s Baseline Window Quantization** | **PROVEN** | `docs/evaluation/results/baseline_quantization_comparison.json` | 10s baseline captures 4, 5, or 6 discrete micro-batches (2s trigger interval), quantizing into step functions ($\approx 1,199$, $1,499$, $1,799$ rec/s). |
| **True Halted Throughput = 0.00 rec/s** | **PROVEN** | `docs/evaluation/results/derived/pipeline_run_events_control.json` | During container downtime, zero micro-batches commit. Reported non-zero window throughput (~107–571 rec/s) is caused by pre-halt and in-flight drain. |
| **Spark Worker Restart r4 Validator Output (9,329)** | **UNRESOLVED** | `docs/evaluation/raw/campaign/spark-worker-restart/eval-spark-worker-restart-r4/report.json` | Validator reported 9,329 ($300,000 - 290,671$). Physical output is 289,744 committed rows. The source of the in-memory tracker counter 290,671 cannot be proven from raw logs. |

---

## 2. Text for "Threats to Validity" Section

Authors may adapt the following text directly into the manuscript:

### 2.1 Single-Replica HDFS Topology (`dfs.replication=1`)
> *"In our testbed, the HDFS storage layer was deployed with a replication factor of 1 (`dfs.replication=1`). Under this configuration, stopping the sole DataNode container caused 2 of 5 runs to terminate prematurely with an unhandled `RemoteException` during checkpoint offset file commit, resulting in physical data truncation at approximately 55,000 rows. This outcome reflects single-replica topology constraints rather than an architectural flaw in Spark Structured Streaming. In production multi-node deployments with standard replication (`dfs.replication >= 3`), DataStreamer excludes the failed DataNode and pipelines block writes to surviving nodes without query failure. Evaluating multi-node replication failover remains future work."*

### 2.2 Micro-Batch Boundary Quantization in Baseline Measurement
> *"The platform's operational baseline is measured over a 10-second sampling window immediately preceding fault injection. Because the streaming pipeline processes data with a 2.0-second trigger interval and approximately 3,000 records per micro-batch, a 10-second window captures either 4, 5, or 6 batch boundaries, quantizing baseline throughput into discrete values ($\approx 1,199.2$, $1,499.0$, or $1,799.0\text{ rec/s}$). To address this discrete quantization artifact, we compute an unquantized operational baseline over all post-warmup pre-injection micro-batches, which yields continuous, steady-state throughput estimates ($1,446.0 - 1,784.6\text{ rec/s}$)."*

### 2.3 Built-in Consistency Validator Filter-Unawareness
> *"Evaluation revealed a systematic reporting artifact in the platform's default `DataConsistencyValidator`. The validator computes data loss by subtracting committed sink rows from raw source CSV lines ($300,000 - \text{output}$). Because the pipeline definition includes deterministic filter operations (`remove_nulls` removing 5,124 rows, and `amount > 0` removing 5,132 rows), exactly 10,256 rows are intentionally filtered out. Consequently, the platform validator reports a false loss of 10,256 records even when physical data completeness is 100% intact. We audited physical data preservation directly on HDFS using PyArrow multiset comparisons over all five columns, proving zero loss."*

### 2.4 Tracker Counter Discrepancy in `spark-worker-restart-r4`
> *"In repetition 4 of the Spark worker restart experiment, the platform validator recorded 290,671 processed records, reporting 9,329 data loss ($300,000 - 290,671$). However, independent physical audit of HDFS storage confirmed that exactly 289,744 post-filter records were committed with zero physical row loss. The origin of the 290,671 in-memory counter discrepancy could not be proven from committed raw driver logs and is classified as unresolved."*

---

## 3. Recommended Figures and Visualizations

Authors should generate the following figures from `docs/evaluation/results/per_run_metrics.csv` and `docs/evaluation/results/paper_tables.csv`:

1. **Figure 1: Dual Recovery Dynamics (Box Plot)**:
   - X-axis: Fault Scenario (`kafka-stop`, `kafka-restart`, `spark-worker-stop`, `spark-worker-restart`, `datanode-stop`).
   - Y-axis: Recovery Time in Seconds (log scale).
   - Series: Container Recovery Time vs. 90% Throughput Restoration Time. Demonstrates that application recovery latency is up to $36\times$ longer than infrastructure recovery.
2. **Figure 2: Degradation vs. Control Baseline (Grouped Bar Chart)**:
   - Compares during-fault window throughput vs. empirical control steady-state (1,498.1 rec/s).
   - Highlights that transient restarts (`kafka-restart`: 17.4% drop; `spark-worker-restart`: 0.6% drop vs control) maintain high availability compared to complete container halts (>60% drop).
3. **Figure 3: DataNode Survival Timeline (Gantt / Event Timeline)**:
   - Plots the progression of events for runs r1 through r5: Snapshot $\to$ Docker Stop $\to$ Container Exited $\to$ First Error $\to$ Revert $\to$ Resume/Terminate.
   - Contrasts the 3 surviving runs (~37-39s stall before resumption) with the 2 crashed runs (failure at 10.4s and 10.5s post-stop).

---

## 4. Explicitly Unevaluated Architectures & Scenarios

The paper MUST NOT claim coverage of the following untested configurations:
1. **Multi-Node HDFS Replication (`dfs.replication >= 3`)**: Only single-replica storage was evaluated.
2. **Dynamic Resource Allocation**: Spark executors were statically assigned; dynamic allocation and executor decommissioning were not evaluated.
3. **Multiple Kafka Consumer Groups**: Ingestion was evaluated with a single consumer group on a 3-partition topic.
4. **Network Partition Faults (`iptables` packet drop / partition)**: Faults were limited to container stop and restart; network partitions and latency injection were not evaluated.
