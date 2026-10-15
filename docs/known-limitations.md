# ChaosFlow Known Limitations & Design Boundaries

This document provides an honest, transparent breakdown of the known boundaries, trade-offs, and operational limitations of the ChaosFlow platform.

---

## 1. Batch Mode Has No Kafka Consumer Lag (`kafka.consumerLag` is `null`)

In batch execution mode, Apache Spark queries Kafka using bounded partition offsets (`startingOffsets` to `endingOffsets`) instead of running as a persistent streaming consumer group committing offset heartbeats to the Kafka coordinator.
- **Why it matters**: There is no long-running consumer group committing periodic offset checkpoints to Kafka's consumer group coordinator during batch runs.
- **Contract**: `kafka.consumerLag` is strictly returned as `null` (rendered on the UI as an em dash `—`) rather than displaying a false zero.

---

## 2. Single-Node Virtual Cluster Topology

The platform orchestrates 13 containerized services (Kafka, Spark Master, Spark Worker, HDFS NameNode, HDFS DataNode, PostgreSQL, Airflow, Prometheus, Grafana, Nginx, FastAPI) on a single Docker host.
- **Why it matters**: All network traffic between services is routed over a single Docker bridge network (`chaosflow_network`). Network faults (such as `network_delay` or `network_loss`) injected via `tc` or `iptables` affect container-level virtual interfaces rather than physical switches or fiber optic interconnects.
- **Trade-off**: Provides reproducible, zero-cost chaos testing on commodity developer hardware without requiring expensive multi-rack cloud infrastructure.

---

## 3. Container-Level Fault Injection vs OS Kernel Panic

ChaosLab fault injectors operate via the Docker engine API and Linux container namespaces (`cgroups`, `tc`, `kill -STOP`, `fallocate`).
- **Why it matters**: Faults like `container_stop`, `container_restart`, `cpu_stress`, `memory_pressure`, and `disk_pressure` target specific container namespaces. They do not simulate hardware power loss, physical disk head crashes, or host kernel panics.
- **Safety guarantee**: By constraining fault injection to container namespaces via `safety.py`, host system stability and non-platform containers are 100% protected against corruption.

---

## 4. Spark Task Totals are Cumulative and Dynamic

Apache Spark lazily constructs its DAG (Directed Acyclic Graph) and submits execution stages dynamically as upstream shuffles complete.
- **Why it matters**: `spark.tasksTotal` represents the cumulative task count across currently submitted and completed stages. It can increase during runtime (e.g. from 10 tasks to 200 tasks when wide shuffle partitions execute).
- **Design decision**: UI progress bars calculate percentage completion dynamically against known tasks rather than relying on a static up-front estimate.

---

## 5. AI Diagnostics & Anomaly Scoring Boundaries

The reliability analysis service (`POST /api/ai/reliability-analysis`) provides heuristic anomaly detection across MTTR, data loss, and throughput metrics.
- **Why it matters**: When optional machine learning libraries (such as `scikit-learn` Isolation Forest) are present, multivariate outlier analysis is performed. When absent, the system seamlessly falls back to a deterministic robust Z-score calculation (median absolute deviation).
- **Limitation**: Recommendations are rule-informed diagnostic heuristics designed to assist SREs; they should be treated as guidance rather than autonomous cluster remediation.

---

## 6. Headless WebGL vs Hardware Acceleration

The 3D cluster topology visualizer (`Scene3D.tsx`) is built with Three.js and WebGL.
- **Why it matters**: In modern GPU-equipped browsers, the scene renders at a smooth 60 FPS. In virtualized environments, CI runners, or headless Chromium sessions without hardware acceleration (such as GitHub Actions or headless Linux containers), WebGL defaults to software rasterization (SwiftShader / Mesa).
- **Fallback**: The platform includes automatic WebGL feature detection and gracefully provides an accessible, lightweight 2D canvas / text-equivalent representation (`data-testid="viz-text-equivalent"`).
