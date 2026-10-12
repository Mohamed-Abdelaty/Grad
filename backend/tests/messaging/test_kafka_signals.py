"""
tests/messaging/test_kafka_signals.py
-------------------------------------
Unit tests for Step 4 Kafka progress signals:
- delivery callback counting (success and error)
- end-offset sum over 3 partitions
- probe failure returns None
- probe consumer is always closed
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch
import pytest

from app.messaging.kafka_producer import KafkaProducerClient, get_topic_end_offsets


def test_delivery_callback_counting_success_and_error():
    """Verify published/failed counts increment accurately via on_delivery callbacks."""
    mock_raw_producer = MagicMock()

    # Capture the on_delivery callback passed to produce
    def fake_produce(*args, **kwargs):
        callback = kwargs.get("on_delivery")
        payload = kwargs.get("value")
        # trigger error if value contains 'fail'
        if b"fail" in payload:
            mock_err = MagicMock()
            callback(mock_err, MagicMock())
        else:
            callback(None, MagicMock())

    mock_raw_producer.produce.side_effect = fake_produce

    client = KafkaProducerClient(producer=mock_raw_producer)

    records = [
        {"id": 1, "status": "ok"},
        {"id": 2, "status": "fail"},
        {"id": 3, "status": "ok"},
    ]

    progress_calls = []
    def on_prog(pub, fail):
        progress_calls.append((pub, fail))

    result = client.produce_batch(
        topic="test-topic",
        records=records,
        on_progress=on_prog,
    )

    assert result["produced"] == 2
    assert result["failed"] == 1
    assert len(progress_calls) >= 1
    assert progress_calls[-1] == (2, 1)


def test_end_offset_sum_over_3_partitions():
    """Verify get_topic_end_offsets sums high watermarks across 3 partitions."""
    mock_consumer = MagicMock()

    # Mock list_topics
    mock_meta = MagicMock()
    mock_topic_meta = MagicMock()
    mock_topic_meta.error = None
    mock_topic_meta.partitions = {0: MagicMock(), 1: MagicMock(), 2: MagicMock()}
    mock_meta.topics = {"my-topic": mock_topic_meta}
    mock_consumer.list_topics.return_value = mock_meta

    # Mock watermark offsets: (low, high)
    def fake_watermarks(tp, timeout, cached):
        if tp.partition == 0:
            return (0, 100)
        elif tp.partition == 1:
            return (0, 250)
        else:
            return (0, 150)

    mock_consumer.get_watermark_offsets.side_effect = fake_watermarks

    with patch("app.messaging.kafka_producer.Consumer", return_value=mock_consumer):
        total = get_topic_end_offsets("my-topic")

    assert total == 500
    mock_consumer.close.assert_called_once()


def test_probe_failure_returns_none():
    """Verify get_topic_end_offsets returns None on any probe failure."""
    mock_consumer = MagicMock()
    mock_consumer.list_topics.side_effect = RuntimeError("Broker connection timeout")

    with patch("app.messaging.kafka_producer.Consumer", return_value=mock_consumer):
        total = get_topic_end_offsets("failed-topic")

    assert total is None
    mock_consumer.close.assert_called_once()


def test_probe_consumer_is_always_closed():
    """Verify probe consumer is closed even when get_watermark_offsets raises."""
    mock_consumer = MagicMock()
    mock_meta = MagicMock()
    mock_topic_meta = MagicMock()
    mock_topic_meta.error = None
    mock_topic_meta.partitions = {0: MagicMock()}
    mock_meta.topics = {"error-topic": mock_topic_meta}
    mock_consumer.list_topics.return_value = mock_meta
    mock_consumer.get_watermark_offsets.side_effect = Exception("KafkaException: offset error")

    with patch("app.messaging.kafka_producer.Consumer", return_value=mock_consumer):
        total = get_topic_end_offsets("error-topic")

    assert total is None
    mock_consumer.close.assert_called_once()
