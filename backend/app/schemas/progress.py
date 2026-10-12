"""
app/schemas/progress.py
-----------------------
Pydantic schemas for pipeline run progress reporting (Section 8.1 & UI-3).
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field

StageState = Literal["pending", "active", "done", "error", "skipped"]


class StageInfo(BaseModel):
    key: str
    state: StageState
    startedAt: Optional[str] = None
    finishedAt: Optional[str] = None
    error: Optional[str] = None


class ActiveSparkStage(BaseModel):
    id: int
    name: str
    tasksTotal: int
    tasksDone: int


class SparkProgress(BaseModel):
    appId: Optional[str] = None
    tasksTotal: Optional[int] = None
    tasksDone: Optional[int] = None
    activeStage: Optional[ActiveSparkStage] = None
    recordsIn: Optional[int] = None
    recordsOut: Optional[int] = None
    durationMs: Optional[float] = None


class KafkaProgress(BaseModel):
    topic: Optional[str] = None
    partitions: Optional[int] = None
    published: Optional[int] = None
    failed: int = 0
    endOffsets: Optional[int] = None
    consumerLag: Optional[int] = None


class SourceProgress(BaseModel):
    type: Optional[str] = None
    rowsTotal: Optional[int] = None
    rowsRead: Optional[int] = None


class HDFSProgress(BaseModel):
    path: Optional[str] = None
    filesWritten: Optional[int] = None
    bytesWritten: Optional[int] = None
    rowsWritten: Optional[int] = None
    verified: Optional[bool] = None


class ThroughputProgress(BaseModel):
    recordsPerSecond: Optional[float] = None
    samples: List[Dict[str, Any]] = Field(default_factory=list)


class RunProgress(BaseModel):
    runId: str
    status: str
    mode: str = "batch"
    startedAt: Optional[str] = None
    updatedAt: str
    completedAt: Optional[str] = None
    stages: List[StageInfo]
    source: SourceProgress
    kafka: KafkaProgress
    spark: SparkProgress
    hdfs: HDFSProgress
    throughput: ThroughputProgress
    etaSeconds: Optional[int] = None
    errors: List[str] = Field(default_factory=list)
    fault: Optional[Any] = None
