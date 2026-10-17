"""
app/messaging/kafka_producer.py
-------------------------------
Producer wrapper for publishing JSON records to Kafka topics.

Wraps confluent_kafka.Producer to handle JSON serialization, delivery callbacks,
batch publishing, end-offsets probing, and error management.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any, Callable

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition

from app.core.config import Settings, get_settings
from app.messaging.exceptions import KafkaProduceError
from app.messaging.kafka_config import get_producer_config

logger = logging.getLogger(__name__)


def get_topic_partition_end_offsets(
    topic: str, settings: Settings | None = None
) -> dict[int, int] | None:
    """
    Query the end offset (high watermark) for each partition of *topic*.

    Creates a short-lived probe consumer, queries watermarks with timeout=5,
    returns a dict of {partition_id: high_watermark}, and always closes the consumer.
    Returns None on any error.
    """
    settings = settings or get_settings()
    conf = {
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"chaosflow-offset-probe-{topic}",
        "enable.auto.commit": False,
    }
    consumer = None
    try:
        consumer = Consumer(conf)
        meta = consumer.list_topics(topic=topic, timeout=5.0)
        if not meta or topic not in meta.topics:
            return None
        topic_meta = meta.topics[topic]
        if topic_meta.error is not None:
            return None
        partition_ids = list(topic_meta.partitions.keys())
        if not partition_ids:
            return None

        offsets: dict[int, int] = {}
        for p in partition_ids:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, p), timeout=5.0, cached=False
            )
            offsets[p] = high
        return offsets
    except Exception as exc:
        logger.warning(
            "Failed to get topic partition end offsets for '%s': %s", topic, exc
        )
        return None
    finally:
        if consumer is not None:
            try:
                consumer.close()
            except Exception:
                pass


def get_topic_end_offsets(topic: str, settings: Settings | None = None) -> int | None:
    """
    Query the sum of end offsets (high watermarks) across all partitions of *topic*.

    Returns the sum of high watermarks across all partitions, or None on error.
    """
    offsets = get_topic_partition_end_offsets(topic, settings)
    if offsets is None:
        return None
    return sum(offsets.values())


class KafkaProducerClient:
    """
    Wrapper around confluent_kafka.Producer.

    Provides synchronous or asynchronously-polled message publishing with
    delivery reports, batch helpers, and topic metadata discovery.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        producer: Producer | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        if producer is not None:
            self._producer = producer
        else:
            config = get_producer_config(self.settings)
            self._producer = Producer(config)

    def get_topic_partitions(self, topic: str, timeout: float = 5.0) -> int | None:
        """Query the number of partitions for *topic* from Kafka metadata."""
        try:
            metadata = self._producer.list_topics(topic=topic, timeout=timeout)
            if metadata and topic in metadata.topics:
                topic_meta = metadata.topics[topic]
                if topic_meta.error is None:
                    return len(topic_meta.partitions)
            return None
        except Exception as exc:
            logger.warning("Failed to get partitions for topic '%s': %s", topic, exc)
            return None

    def produce(
        self,
        topic: str,
        value: dict[str, Any],
        key: str | None = None,
        on_delivery: Callable[[KafkaError | None, Any], None] | None = None,
    ) -> None:
        """
        Produce a single JSON record to a Kafka topic.

        Parameters
        ----------
        topic : str
            Target topic name.
        value : dict[str, Any]
            Record payload dict to serialize as JSON.
        key : str | None
            Optional record key for partition routing.
        on_delivery : Callable | None
            Optional delivery callback override.

        Raises
        ------
        KafkaProduceError
            If message queueing fails (e.g. BufferError).
        """
        try:
            payload = json.dumps(value).encode("utf-8")
            key_bytes = key.encode("utf-8") if key is not None else None
        except Exception as exc:
            raise KafkaProduceError(
                f"Failed to serialize record to JSON for topic '{topic}': {exc}", cause=exc
            ) from exc

        def _default_delivery_report(err: KafkaError | None, msg: Any) -> None:
            if err is not None:
                logger.error(
                    "Kafka message delivery failed",
                    extra={
                        "topic": topic,
                        "error": str(err),
                        "err_code": err.code() if hasattr(err, "code") else None,
                    },
                )
            else:
                logger.debug(
                    "Kafka message delivered successfully",
                    extra={
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                    },
                )

        callback = on_delivery or _default_delivery_report

        try:
            self._producer.produce(
                topic=topic,
                value=payload,
                key=key_bytes,
                on_delivery=callback,
            )
            self._producer.poll(0)
        except BufferError as exc:
            raise KafkaProduceError(
                f"Producer local queue buffer full while producing to topic '{topic}': {exc}",
                cause=exc,
            ) from exc
        except KafkaException as exc:
            raise KafkaProduceError(
                f"Kafka producer exception for topic '{topic}': {exc}", cause=exc
            ) from exc
        except Exception as exc:
            raise KafkaProduceError(
                f"Unexpected error producing to topic '{topic}': {exc}", cause=exc
            ) from exc

    def produce_batch(
        self,
        topic: str,
        records: list[dict[str, Any]],
        key_field: str | None = None,
        timeout: float = 60.0,
        on_progress: Callable[[int, int], None] | None = None,
    ) -> dict[str, int]:
        """
        Produce a batch of JSON records to a topic with delivery callback tracking.

        Parameters
        ----------
        topic : str
            Target topic name.
        records : list[dict[str, Any]]
            List of dictionary payloads.
        key_field : str | None
            Optional field name in dicts to use as the record key.
        timeout : float
            Flush timeout in seconds.
        on_progress : Callable[[int, int], None] | None
            Callback invoked with ``(published, failed)`` at most every 500 ms
            or every 1000 messages.

        Returns
        -------
        dict[str, int]
            ``{"produced": int, "failed": int}``
        """
        lock = threading.Lock()
        published_count = 0
        failed_count = 0

        def _batch_delivery_report(err: KafkaError | None, msg: Any) -> None:
            nonlocal published_count, failed_count
            with lock:
                if err is None:
                    published_count += 1
                else:
                    failed_count += 1

        last_update_time = time.monotonic()
        last_update_msg_count = 0

        for i, rec in enumerate(records, 1):
            key = str(rec[key_field]) if (key_field and key_field in rec) else None
            try:
                payload = json.dumps(rec).encode("utf-8")
                key_bytes = key.encode("utf-8") if key is not None else None
            except Exception as exc:
                with lock:
                    failed_count += 1
                continue

            # Queue with retry on BufferError
            while True:
                try:
                    self._producer.produce(
                        topic=topic,
                        value=payload,
                        key=key_bytes,
                        on_delivery=_batch_delivery_report,
                    )
                    break
                except BufferError:
                    self._producer.poll(0.1)
                except Exception as exc:
                    logger.error("Failed to queue message: %s", exc)
                    with lock:
                        failed_count += 1
                    break

            # Poll every 1000 messages to trigger delivery callbacks during loop
            if i % 1000 == 0:
                self._producer.poll(0)

            # Check tracker update criteria: every 500 ms or every 1000 messages
            now = time.monotonic()
            if (i - last_update_msg_count >= 1000) or (now - last_update_time >= 0.5):
                if on_progress:
                    with lock:
                        pub, fail = published_count, failed_count
                    on_progress(pub, fail)
                last_update_time = now
                last_update_msg_count = i

        # Final flush waiting for all queued messages
        self.flush(timeout=timeout)

        # Final progress update after flush
        if on_progress:
            with lock:
                pub, fail = published_count, failed_count
            on_progress(pub, fail)

        with lock:
            return {"produced": published_count, "failed": failed_count}

    def flush(self, timeout: float = 60.0) -> int:
        """
        Flush outstanding producer messages and wait for delivery callbacks.

        Parameters
        ----------
        timeout : float
            Maximum time in seconds to wait for flush to complete.

        Returns
        -------
        int
            Number of messages still remaining in queue after timeout.
        """
        return self._producer.flush(timeout)

    def close(self) -> None:
        """Flush pending messages before closing the producer."""
        self.flush(timeout=10.0)
