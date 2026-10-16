# Data Flow

```mermaid
sequenceDiagram
    participant User
    participant Backend as Backend (Phase 3)
    participant Kafka as Kafka Plugin (Phase 4)
    participant Spark as Spark Job (Phase 5)
    participant HDFS as HDFS Client (Phase 6)

    User->>Backend: Upload CSV Data
    Backend->>Backend: Validate Data
    Backend->>Kafka: Publish records
    Kafka-->>Backend: Ack
    Spark->>Kafka: Consume records
    Spark->>Spark: Transform Data
    Spark->>HDFS: Write processed data (Parquet)
    HDFS-->>Spark: Ack
```

This sequence covers the primary path of a batch data record through the system.

## Structured Streaming Data Flow

```mermaid
sequenceDiagram
    participant Producer as Kafka Producer Thread
    participant Kafka as Kafka Broker (KRaft)
    participant Spark as Spark Structured Streaming
    participant HDFS as HDFS Storage
    participant Monitor as Stream Completion Detector

    Producer->>Kafka: Stream records with retries
    Kafka-->>Producer: Ack
    loop Micro-Batch Execution
        Spark->>Kafka: Poll offsets & batch messages
        Spark->>Spark: Enforce schema & filter/transform
        Spark->>HDFS: Commit Parquet batch & write checkpoint
        Spark-->>Monitor: Emit STREAM_PROGRESS (batchId, endOffset, rows)
        Monitor->>Kafka: Query partition high-water marks
        Monitor->>Monitor: Calculate consumer lag & check drain
    end
    Note over Monitor,Spark: When producer done and lag == 0, trigger graceful stop
    Spark->>HDFS: Commit final state
    Spark-->>Producer: Exit cleanly
```

Refer to the respective phase code (e.g., CsvSourcePlugin, Spark jobs, WebHDFS client, stream_progress) for implementation details.
