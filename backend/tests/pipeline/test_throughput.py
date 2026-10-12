"""
backend/tests/pipeline/test_throughput.py
----------------------------------------
Unit tests for ThroughputEstimator EMA calculation, ring buffer capping,
guard conditions, ETA calculation, and RunProgressTracker integration.
"""

from __future__ import annotations

import math
import time

import pytest

from app.pipeline.progress import RunProgressTracker
from app.pipeline.throughput import ThroughputEstimator


def test_ema_arithmetic_fixed_inputs() -> None:
    """EMA correctly weights previous estimate and instantaneous rate."""
    estimator = ThroughputEstimator(alpha=0.3)

    # Initial baseline
    estimator.update(published=0, now=100.0)
    assert estimator.records_per_second is None
    assert len(estimator.samples) == 0

    # First interval: dt=1.0, delta=1000 -> rate=1000
    estimator.update(published=1000, now=101.0)
    assert estimator.records_per_second == 1000.0
    assert len(estimator.samples) == 1

    # Second interval: dt=1.0, delta=2000 -> rate=2000
    # EMA = 0.3 * 2000 + 0.7 * 1000 = 600 + 700 = 1300.0
    estimator.update(published=3000, now=102.0)
    assert estimator.records_per_second == 1300.0
    assert len(estimator.samples) == 2

    # Third interval: dt=1.0, delta=1000 -> rate=1000
    # EMA = 0.3 * 1000 + 0.7 * 1300 = 300 + 910 = 1210.0
    estimator.update(published=4000, now=103.0)
    assert estimator.records_per_second == 1210.0
    assert len(estimator.samples) == 3


def test_ring_buffer_cap_120() -> None:
    """Ring buffer never exceeds 120 samples and retains newest values."""
    estimator = ThroughputEstimator(maxlen=120)

    estimator.update(0, now=0.0)
    for i in range(1, 151):
        estimator.update(published=i * 100, now=float(i))

    assert len(estimator.samples) == 120
    # The oldest sample in the buffer should be at t=31.0
    assert estimator.samples[0]["t"] == 31.0
    assert estimator.samples[-1]["t"] == 150.0


def test_dt_zero_and_sub_half_second_ignored() -> None:
    """Updates closer than 0.5s or with dt <= 0 are ignored."""
    estimator = ThroughputEstimator()

    estimator.update(0, now=10.0)
    estimator.update(100, now=11.0)
    assert len(estimator.samples) == 1

    # Sub-half-second update ignored
    estimator.update(150, now=11.3)
    assert len(estimator.samples) == 1

    # dt = 0 ignored
    estimator.update(200, now=11.0)
    assert len(estimator.samples) == 1

    # Normal update after >= 0.5s succeeds
    estimator.update(250, now=11.6)
    assert len(estimator.samples) == 2


def test_negative_delta_ignored() -> None:
    """Negative published delta does not corrupt EMA."""
    estimator = ThroughputEstimator()

    estimator.update(500, now=10.0)
    estimator.update(600, now=11.0)
    rate_before = estimator.records_per_second

    # Negative delta
    estimator.update(400, now=12.0)
    assert estimator.records_per_second == rate_before
    assert len(estimator.samples) == 1


def test_eta_seconds_calculation_and_none_cases() -> None:
    """ETA returns ceil((total - pub) / ema) only when conditions are met."""
    estimator = ThroughputEstimator()

    estimator.update(0, now=0.0)
    estimator.update(100, now=1.0)
    estimator.update(200, now=2.0)

    # Only 2 samples collected -> ETA must be None
    assert len(estimator.samples) == 2
    assert estimator.eta_seconds(total=1000, published=200) is None

    # 3rd sample -> ETA is computed
    estimator.update(300, now=3.0)
    assert len(estimator.samples) == 3
    ema = estimator.records_per_second
    assert ema is not None and ema > 0
    eta = estimator.eta_seconds(total=1000, published=300)
    assert eta == math.ceil(700 / ema)

    # Unknown total -> None
    assert estimator.eta_seconds(total=None, published=300) is None

    # Published >= Total -> 0
    assert estimator.eta_seconds(total=300, published=300) == 0
    assert estimator.eta_seconds(total=200, published=300) == 0


def test_tracker_wiring_throughput_and_spark_and_success() -> None:
    """
    RunProgressTracker updates throughput/ETA during Kafka stage, resets
    them during Spark stage, and computes end-to-end average on success.
    """
    tracker = RunProgressTracker()
    run_id = "test-tracker-throughput-run"
    tracker.start(run_id, "test-pipe")

    # Set total rows known from source
    tracker.emit(run_id, "source_done", {"rows": 10000})

    # Kafka progress events
    tracker.emit(run_id, "kafka_start", {"topic": "test-topic"})
    tracker.emit(run_id, "kafka_progress", {"published": 0})
    time.sleep(0.55)
    tracker.emit(run_id, "kafka_progress", {"published": 2000})
    time.sleep(0.55)
    tracker.emit(run_id, "kafka_progress", {"published": 5000})
    time.sleep(0.55)
    tracker.emit(run_id, "kafka_progress", {"published": 8000})

    p_kafka = tracker.get_progress(run_id)
    assert p_kafka is not None
    assert p_kafka["throughput"]["recordsPerSecond"] is not None
    assert p_kafka["throughput"]["recordsPerSecond"] > 0
    assert len(p_kafka["throughput"]["samples"]) >= 3
    assert p_kafka["etaSeconds"] is not None

    tracker.emit(run_id, "kafka_done", {"published": 8000})

    # Spark active resets recordsPerSecond and etaSeconds to None
    tracker.emit(run_id, "spark_start", {"appId": "app-spark-123"})
    p_spark = tracker.get_progress(run_id)
    assert p_spark is not None
    assert p_spark["throughput"]["recordsPerSecond"] is None
    assert p_spark["etaSeconds"] is None

    # Success calculates end-to-end average
    tracker.emit(run_id, "run_success", {})
    p_success = tracker.get_progress(run_id)
    assert p_success is not None
    assert p_success["throughput"]["recordsPerSecond"] is not None
    assert p_success["etaSeconds"] is None
