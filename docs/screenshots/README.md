# ChaosFlow Visual Verification & Screenshot Index

This directory archives automated screenshots and recordings captured during Playwright regression passes across all development phases.

---

## Phase UI-4: Pipeline Management & YAML Authoring (`ui-4/`)

Captured via `frontend/e2e/pipelines.spec.ts`.

| File | Resolution | Description |
|---|---|---|
| `empty_state.png` | 1440x900 | Pipeline management page showing empty runs history table and editor ready state. |
| `valid_state.png` | 1440x900 | Editor populated with `sales-csv` configuration displaying green `Valid` status pill and 0 errors. |
| `invalid_state.png` | 1440x900 | Syntax error diagnostics panel highlighting line-level validation issues. |
| `runs_table_filters.png` | 1440x900 | Filtered runs history table demonstrating status, date, and search facet filters. |
| `mobile_valid_state.png` | 390x844 | Mobile layout showing stacked editor controls and responsive action buttons. |

---

## Phase UI-5: Live Run Tracking & Operations (`ui-5/`)

Captured via `frontend/e2e/run-details.spec.ts`.

| File | Resolution | Description |
|---|---|---|
| `desktop-running.png` | 1440x900 | Live pipeline execution showing active Spark stage, animated particle telemetry, and connection badge. |
| `desktop-succeeded.png` | 1440x900 | Succeeded pipeline execution with verified row counts, throughput sparkline, and stage timeline. |
| `desktop-failed.png` | 1440x900 | Failed execution view with error banner, stage failure diagnostics, and re-run action. |
| `mobile-running.png` | 390x844 | Mobile viewport of active pipeline run with stacked metrics cards. |
| `long-run.webm` | Video | Recording of a 1M-row execution from inception to completion. |

---

## Phase UI-6: Output Preview, Verification & Downloads (`ui-6/`)

Captured via `frontend/e2e/output.spec.ts`.

| File | Resolution | Description |
|---|---|---|
| `desktop-output-preview.png` | 1440x900 | In-browser preview of first 500 rows with schema-inferred column types. |
| `desktop-output-metadata.png` | 1440x900 | Parity verification card showing HDFS file size, total row count, and checksum status. |
| `desktop-output-failed.png` | 1440x900 | Output tab state for a failed run displaying unavailable output message. |
| `mobile-output.png` | 390x844 | Mobile responsive preview with horizontal scrolling table and download button. |

---

## Phase UI-7: 3D Cluster Visualizer & Topology Telemetry (`ui-7/`)

Captured via `frontend/e2e/viz.spec.ts`.

| File | View | Description |
|---|---|---|
| `pending-3d.png` / `pending-2d.png` | 3D / 2D | Idle cluster conveyor plane with all stage nodes ready. |
| `kafka-40-3d.png` / `kafka-40-2d.png` | 3D / 2D | Active ingestion phase with particles flowing along the Source-Kafka bezier curve. |
| `spark-active-3d.png` / `spark-active-2d.png` | 3D / 2D | Active computation phase with purple glowing Spark executor nodes. |
| `hdfs-writing-3d.png` / `hdfs-writing-2d.png` | 3D / 2D | Storage commit phase with green data stream writing to HDFS DataNode. |
| `succeeded-3d.png` / `succeeded-2d.png` | 3D / 2D | Succeeded run state with all nodes illuminated green. |
| `failed-spark-3d.png` / `failed-spark-2d.png` | 3D / 2D | Failed run state highlighting failing Spark node in red. |
| `cancelled-3d.png` / `cancelled-2d.png` | 3D / 2D | Cancelled run state showing halted particle streams and amber status. |
| `fault-injected-3d.png` / `fault-injected-2d.png` | 3D / 2D | ChaosLab fault injection in progress with pulsating red aura. |
| `fault-recovering-3d.png` / `fault-recovering-2d.png`| 3D / 2D | Recovery phase with node transitioning back to operational state. |
| `fault-recovered-3d.png` / `fault-recovered-2d.png` | 3D / 2D | Fully recovered node resuming steady particle telemetry. |
| `streaming-lag-3d.png` / `streaming-lag-2d.png` | 3D / 2D | Streaming mode showing consumer lag accumulator visualization. |
| `run-running-1440x900.png` | Desktop | Full view of live operations page with 3D scene embedded. |
| `run-succeeded-1440x900.png` | Desktop | Full view of succeeded run with 3D scene and completion summary. |
| `run-mobile-390x844.png` | Mobile | Responsive 3D scene scaling and fallback controls on mobile viewport. |

---

## Phase UI-8: ChaosLab Reliability Engine & Resilience Reports (`ui-8/`)

Captured via `frontend/e2e/reliability.spec.ts`.

| File | Resolution | Description |
|---|---|---|
| `wizard-1440x900.png` / `wizard-390x844.png` | Desktop / Mobile | 4-step experiment configuration wizard (target, fault, duration, SLA thresholds). |
| `monitor-1440x900.png` / `monitor-390x844.png` | Desktop / Mobile | Live experiment monitor showing fault countdown, probe status, and recovery timeline. |
| `result-summary-1440x900.png` / `result-summary-390x844.png` | Desktop / Mobile | Post-experiment modal displaying automated PASS/FAIL verdict and measured MTTR. |
| `reports-1440x900.png` / `reports-390x844.png` | Desktop / Mobile | Historical reports overview table with multi-select comparison checkboxes. |
| `report-details-1440x900.png` / `report-details-390x844.png` | Desktop / Mobile | Deep-dive report audit showing criteria breakdown, availability samples, and AI diagnostics. |
| `compare-modal-1440x900.png` / `compare-modal-390x844.png` | Desktop / Mobile | Side-by-side comparative analysis of two chaos experiment runs. |
| `run-kafka-faulted-1440x900.png` / `run-kafka-faulted-390x844.png` | Desktop / Mobile | Live pipeline run view during active Kafka fault injection showing pipeline buffering. |
