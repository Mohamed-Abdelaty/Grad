"""
app/messaging
-------------
Kafka messaging package for the Resilient Big Data Pipeline Platform.

Exports:
* KafkaAdminClient  — Topic creation, deletion, health probes
* KafkaProducerClient — Record publishing with JSON serialization and batch support
* KafkaConsumerClient — Topic subscription and batch consuming with manual offset commits
* Exceptions         — KafkaConnectionError, KafkaTopicError, KafkaProduceError, KafkaConsumeError
* Config helpers     — get_producer_config, get_consumer_config
"""

from app.messaging.exceptions import (
    KafkaConnectionError,
    KafkaConsumeError,
    KafkaMessagingError,
    KafkaProduceError,
    KafkaTopicError,
)
from app.messaging.kafka_admin import KafkaAdminClient
from app.messaging.kafka_config import get_consumer_config, get_producer_config
from app.messaging.kafka_consumer import KafkaConsumerClient
from app.messaging.kafka_producer import KafkaProducerClient, get_topic_end_offsets

__all__ = [
    "KafkaAdminClient",
    "KafkaProducerClient",
    "KafkaConsumerClient",
    "get_topic_end_offsets",
    "KafkaMessagingError",
    "KafkaConnectionError",
    "KafkaTopicError",
    "KafkaProduceError",
    "KafkaConsumeError",
    "get_producer_config",
    "get_consumer_config",
]
