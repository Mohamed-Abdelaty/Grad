# Architecture Overview

This document provides a comprehensive architectural overview of the Resilient Big Data Pipeline Platform.

## System Components

```mermaid
flowchart TD
    %% User Interfaces
    User([User])
    User --> |HTTP| frontend[Frontend (React)]
    User --> |HTTP| grafana[Grafana (Dashboards)]
    User --> |HTTP| airflow_web[Airflow Webserver]
    
    %% Core Services
    frontend --> |API| backend[Backend (FastAPI)]
    
    %% Backend Interactions
    backend --> |SQL| postgres[(PostgreSQL\nPlatform DB)]
    backend --> |Publish| kafka[Kafka (KRaft)]
    backend --> |Submit| spark_master[Spark Master]
    backend --> |WebHDFS| namenode[HDFS NameNode]
    backend --> |Socket| docker[Docker Socket\nChaosLab Engine]
    
    %% Big Data Components
    spark_master --> spark_worker[Spark Worker]
    spark_worker --> |Read| kafka
    spark_worker --> |Write| namenode
    
    namenode --> datanode[HDFS DataNode]
    spark_worker --> |Write Blocks| datanode
    
    %% Orchestration Component
    airflow_scheduler[Airflow Scheduler] --> |Trigger/Poll| backend
    airflow_web --> airflow_postgres[(Airflow DB)]
    airflow_scheduler --> airflow_postgres
    
    %% Monitoring Component
    prometheus[Prometheus] --> |Scrape /metrics| backend
    grafana --> |Query| prometheus
```

## Services Table

| Service | Port | Purpose |
|---------|------|---------|
| frontend | 3000 | React-based user interface |
| backend | 8000 | FastAPI backend logic, pipeline orchestration, ChaosLab engine |
| postgres | 5432 | Primary platform database (pipelines, experiments, metrics) |
| kafka | 9092/9093 | Message broker for incoming data records |
| spark-master | 8080/7077 | Manages Spark application deployments |
| spark-worker | N/A | Executes Spark workloads |
| namenode | 9870/9000 | HDFS metadata manager |
| datanode | 9864 | HDFS block storage |
| airflow-postgres | 5432 | Dedicated database for Airflow metadata |
| airflow-scheduler | N/A | Schedules directed acyclic graphs (DAGs) |
| airflow-webserver | 8081 | Airflow UI |
| prometheus | 9090 | Scrapes backend metrics |
| grafana | 3001 | Visualizes Prometheus metrics |
| docker (socket) | N/A | Used by backend for fault injection |

## Key Design Decisions
- **Modularity**: Components are cleanly separated using Docker containers.
- **ChaosLab Integration**: Relies on docker.sock mounted only to the backend, using label validation and a strict service allow-list for safety.
- **Dedicated Airflow DB**: Prevents migration conflicts and resource starvation on the main database.
- **Structured Streaming Engine**: Pipelines with `streaming.enabled: true` run as real PySpark Structured Streaming jobs reading from Kafka and sinking to HDFS with atomic Parquet commits and fault-tolerant HDFS checkpoints (`/checkpoints/streaming-{run_id}`).
- **True Consumer Lag & Drain Detection**: The platform monitors real-time Kafka partition high watermarks vs Spark microbatch endOffsets to compute exact consumer lag and detect finite stream completion without manual intervention.
