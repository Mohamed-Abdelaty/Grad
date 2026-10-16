"""
app/messaging/kafka_config.py
-----------------------------
Helper functions for building confluent-kafka configuration dictionaries
from central application Settings.

Config options map to standard librdkafka properties:
https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
"""

from __future__ import annotations

from typing import Any

from app.core.config import Settings, get_settings


def get_producer_config(settings: Settings | None = None) -> dict[str, Any]:
    """
    Build a configuration dict for confluent_kafka.Producer.

    Parameters
    ----------
    settings : Settings | None
        Optional Settings instance.  If None, the singleton instance is used.

    Returns
    -------
    dict[str, Any]
        Dictionary of librdkafka producer configuration properties.
    """
    if settings is None:
        settings = get_settings()

    return {
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "client.id": settings.KAFKA_CLIENT_ID,
        "acks": settings.KAFKA_PRODUCER_ACKS,
        "enable.idempotence": True,
        "delivery.timeout.ms": 120000,
        "retries": 10,
        "max.in.flight.requests.per.connection": 5,
    }


def get_consumer_config(
    group_id: str,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """
    Build a configuration dict for confluent_kafka.Consumer.

    Disables automatic offset commits (enable.auto.commit=False) so that
    consumers must manually commit after processing batches, ensuring at-least-once
    delivery semantics.

    Parameters
    ----------
    group_id : str
        Consumer group ID.
    settings : Settings | None
        Optional Settings instance.  If None, the singleton instance is used.

    Returns
    -------
    dict[str, Any]
        Dictionary of librdkafka consumer configuration properties.
    """
    if settings is None:
        settings = get_settings()

    # Prefix group ID if not already present for namespace separation
    full_group_id = (
        group_id
        if group_id.startswith(settings.KAFKA_CONSUMER_GROUP_PREFIX)
        else f"{settings.KAFKA_CONSUMER_GROUP_PREFIX}-{group_id}"
    )

    return {
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": full_group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "client.id": f"{settings.KAFKA_CLIENT_ID}-consumer",
    }
