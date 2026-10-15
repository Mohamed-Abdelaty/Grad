# ChaosFlow 16-Step Demonstration Script

This script outlines the official end-to-end live demonstration scenario for reviewers and stakeholders. It showcases pipeline authoring, real-time 3D telemetry, output data verification, controlled chaos fault injection, resilience evaluation, and historical reporting.

---

## Preparation (Run Before Presenting)

1. **Verify All Containers are Running**:
   ```powershell
   docker ps --format "table {{.Names}}\t{{.Status}}"
   ```
   *Expected: All 13 platform containers healthy.*

2. **Generate Baseline Dataset (if needed)**:
   ```powershell
   python scripts/generate_sales_data.py --rows 50000 --output backend/sample-configs/sales_50k.csv
   ```

3. **Warm-Up Run (prevents initial JIT/Spark delay)**:
   Execute a quick dry-run of the `sales-csv` pipeline via the UI or API to ensure JVM classes and executors are warm.
   *Expected execution time: 10–18 seconds.*

4. **Timing Benchmarks (Local Machine)**:
   - Login & Dashboard render: ~1 second.
   - Pipeline YAML validation: ~300 ms.
   - 50k-row batch execution (Source -> Kafka -> Spark -> HDFS): 12–20 seconds.
   - ChaosLab container fault injection & recovery: ~25 seconds.
   - Backup plan if Spark is slower on low-memory VMs: Use the default 10,000-row `sales-csv` template which finishes in under 8 seconds.

---

## The 16-Step Live Demonstration Walkthrough

| # | Step Name | Exact Action / Click | What to Say (One Sentence) | Expected On-Screen Result |
|---|---|---|---|---|
| 1 | **System Access** | Open `http://localhost:3000/login` | "ChaosFlow secures platform access with an in-memory JWT authentication model and sliding-window rate limiting." | Login view loads with empty credentials and document title `Login · ChaosFlow`. |
| 2 | **Authentication** | Enter credentials from `.env` and click **Login** | "Logging in authenticates against FastAPI and redirects directly to the operational dashboard." | Navigates to `/`; `Dashboard · ChaosFlow` title displayed. |
| 3 | **Health & KPI Audit** | Inspect top **System Health** cards and **KPI Row** | "The dashboard provides an instant overview of infrastructure health and aggregated resilience metrics." | 4 health cards show Database, Kafka, Spark, and HDFS connected. |
| 4 | **Pipeline Authoring** | Click **Pipelines** in the sidebar navigation | "PipeForge allows engineers to define complex big data pipelines declaratively using validated YAML configurations." | Editor opens with syntax highlighting and schema validation panel. |
| 5 | **Template Selection** | Click **Templates** dropdown and select `sales-csv` | "We start by loading a production-ready sales transaction batch processing pipeline." | YAML content populates the CodeMirror editor immediately. |
| 6 | **Schema Validation** | Click **Validate** button | "Every pipeline configuration is validated against schema definitions and service connectivity before submission." | Validation state turns green with `Valid` status pill and 0 errors. |
| 7 | **Pipeline Launch** | Click **Run Pipeline** button | "Submitting the pipeline triggers distributed ingestion and opens real-time operations tracking." | Navigates to `/pipelines/:id`; stage stepper enters `active` state. |
| 8 | **Live 3D Telemetry** | Observe **Interactive 3D Visualizer** and counters | "Real-time telemetry streams stage transitions, active throughput, and dynamic particle animations." | 3D conveyor scene renders with data packets flowing from Source to Kafka. |
| 9 | **Spark Execution** | Watch Spark stage turn purple and reach completion | "Apache Spark processes batch transformations and commits partitioned data into HDFS." | Stage stepper transitions: Source (done) -> Kafka (done) -> Spark (done) -> HDFS (done). |
| 10 | **Data Parity & Preview**| Click **Output** tab | "The platform verifies cryptographic HDFS row count parity and renders an immediate sample preview." | First 500 rows render in tabular preview matching verified count. |
| 11 | **Export Output** | Click **Download Full CSV** | "Processed outputs can be downloaded directly through a secure, time-limited token." | CSV file downloads with filename matching the run ID. |
| 12 | **ChaosLab Wizard** | Click **Reliability** in sidebar | "ChaosLab enables controlled failure injection against live cluster nodes to test recovery SLAs." | Experiment wizard displays target services and fault options. |
| 13 | **Configure Fault** | Select `kafka`, `container_stop`, duration `20s`, thresholds `0` loss, `30s` MTTR | "We configure a severe infrastructure fault: stopping the Kafka broker during active workloads." | Step review displays target, fault duration, and recovery rules. |
| 14 | **Execute Chaos Test**| Click **Confirm & Start Experiment** | "The safety validator checks container boundaries and initiates the automated fault injection." | Live monitor displays fault phase countdown and red pulsing aura. |
| 15 | **Audit Resilience** | Wait for recovery; click **View Full Report** | "ChaosFlow measures exact time to recovery, verifying that the cluster healed within our 30-second SLA." | Report details page displays PASS badge, MTTR, and throughput comparison. |
| 16 | **Generic Adaptation**| Return to **Pipelines**, load `iot-json` template and run | "The platform handles arbitrary streaming and batch schemas with zero custom code changes." | `iot-json` pipeline validates, executes, and displays IoT telemetry metrics. |
