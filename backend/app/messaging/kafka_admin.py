"""
app/messaging/kafka_admin.py
----------------------------
Admin client wrapper for managing Kafka topics and monitoring cluster health.

Wraps confluent_kafka.admin.AdminClient to provide topic creation, checking,
deletion, and cluster health probe functions.
"""

from __future__ import annotations

import logging
from typing import Any

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from app.core.config import Settings, get_settings
from app.messaging.exceptions import KafkaConnectionError, KafkaTopicError
from app.messaging.kafka_config import get_producer_config

logger = logging.getLogger(__name__)


class KafkaAdminClient:
    """
    Wrapper around confluent_kafka.admin.AdminClient.

    Handles topic administration tasks and connectivity probes safely.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        admin_client: AdminClient | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        if admin_client is not None:
            self._admin = admin_client
        else:
            config = get_producer_config(self.settings)
            self._admin = AdminClient(config)

    def topic_exists(self, topic_name: str, timeout: float = 5.0) -> bool:
        """
        Check if a topic exists on the Kafka cluster.

        Parameters
        ----------
        topic_name : str
            Topic name to check.
        timeout : float
            Request timeout in seconds.

        Returns
        -------
        bool
            True if topic exists, False otherwise.
        """
        try:
            metadata = self._admin.list_topics(timeout=timeout)
            if metadata is None or metadata.topics is None:
                return False
            return topic_name in metadata.topics
        except KafkaException as exc:
            raise KafkaConnectionError(
                f"Failed to check topic existence for '{topic_name}': {exc}", cause=exc
            ) from exc
        except Exception as exc:
            raise KafkaConnectionError(
                f"Unexpected error checking topic existence for '{topic_name}': {exc}", cause=exc
            ) from exc

    def list_topics(self, timeout: float = 5.0) -> list[str]:
        """Return list of all existing topic names on the cluster."""
        try:
            metadata = self._admin.list_topics(timeout=timeout)
            if metadata is None or metadata.topics is None:
                return []
            return list(metadata.topics.keys())
        except Exception as exc:
            logger.warning("Failed to list topics: %s", exc)
            return []

    def create_topic(
        self,
        topic_name: str,
        partitions: int | None = None,
        replication_factor: int | None = None,
        timeout: float = 10.0,
    ) -> dict[str, Any]:
        """
        Create a topic if it does not already exist.

        Parameters
        ----------
        topic_name : str
            Topic name to create.
        partitions : int | None
            Number of partitions. Defaults to settings.KAFKA_DEFAULT_PARTITIONS.
        replication_factor : int | None
            Replication factor. Defaults to settings.KAFKA_DEFAULT_REPLICATION_FACTOR.
        timeout : float
            Timeout for operation futures.

        Returns
        -------
        dict[str, Any]
            ``{"topic": str, "created": bool, "partitions": int, "already_existed": bool}``
        """
        num_partitions = partitions or self.settings.KAFKA_DEFAULT_PARTITIONS
        rep_factor = replication_factor or self.settings.KAFKA_DEFAULT_REPLICATION_FACTOR

        if self.topic_exists(topic_name, timeout=timeout):
            logger.info("Topic '%s' already exists.", topic_name)
            return {
                "topic": topic_name,
                "created": False,
                "partitions": num_partitions,
                "already_existed": True,
            }

        new_topic = NewTopic(
            topic=topic_name,
            num_partitions=num_partitions,
            replication_factor=rep_factor,
        )

        futures = self._admin.create_topics([new_topic])
        future = futures.get(topic_name)

        if future is None:
            raise KafkaTopicError(f"No creation future returned for topic '{topic_name}'")

        try:
            future.result(timeout=timeout)
            logger.info("Topic '%s' created successfully.", topic_name)
            return {
                "topic": topic_name,
                "created": True,
                "partitions": num_partitions,
                "already_existed": False,
            }
        except KafkaException as exc:
            # Handle race condition where topic was created between check and create
            if "TOPIC_ALREADY_EXISTS" in str(exc) or "TopicAlreadyExists" in str(exc):
                return {
                    "topic": topic_name,
                    "created": False,
                    "partitions": num_partitions,
                    "already_existed": True,
                }
            raise KafkaTopicError(
                f"Failed to create topic '{topic_name}': {exc}", cause=exc
            ) from exc
        except Exception as exc:
            raise KafkaTopicError(
                f"Error waiting for creation of topic '{topic_name}': {exc}", cause=exc
            ) from exc

    def delete_topic(self, topic_name: str, timeout: float = 10.0) -> bool:
        """
        Delete a topic from the cluster (primarily for test cleanup).

        Parameters
        ----------
        topic_name : str
            Topic name to delete.
        timeout : float
            Timeout for operation futures.

        Returns
        -------
        bool
            True if topic was deleted, False if it did not exist.
        """
        if not self.topic_exists(topic_name, timeout=timeout):
            return False

        futures = self._admin.delete_topics([topic_name])
        future = futures.get(topic_name)

        if future is None:
            return False

        try:
            future.result(timeout=timeout)
            logger.info("Topic '%s' deleted successfully.", topic_name)
            return True
        except Exception as exc:
            logger.warning("Failed to delete topic '%s': %s", topic_name, exc)
            return False

    def health_check(self, timeout: float = 5.0) -> dict[str, Any]:
        """
        Probe cluster health and return connectivity status.

        Never raises — catches all connection errors and returns a failure status.

        Parameters
        ----------
        timeout : float
            Timeout in seconds for cluster metadata fetch.

        Returns
        -------
        dict[str, Any]
            ``{"connected": bool, "broker_count": int, "topic_count": int, "error": str | None}``
        """
        try:
            metadata = self._admin.list_topics(timeout=timeout)
            if metadata is None:
                return {
                    "connected": False,
                    "broker_count": 0,
                    "topic_count": 0,
                    "error": "No metadata returned from broker",
                }

            broker_count = len(metadata.brokers) if metadata.brokers else 0
            topic_count = len(metadata.topics) if metadata.topics else 0

            return {
                "connected": True,
                "broker_count": broker_count,
                "topic_count": topic_count,
                "error": None,
            }
        except Exception as exc:
            logger.warning("Kafka health check failed: %s", exc)
            return {
                "connected": False,
                "broker_count": 0,
                "topic_count": 0,
                "error": str(exc),
            }
