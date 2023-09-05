from __future__ import annotations

import json
import logging
from typing import Protocol

from app.core.config import get_settings

try:
    from kafka import KafkaProducer
except Exception:  # pragma: no cover
    KafkaProducer = None


logger = logging.getLogger(__name__)


class Publisher(Protocol):
    def publish_facility(self, payload: dict[str, object]) -> None:
        ...

    def publish_gap(self, payload: dict[str, object]) -> None:
        ...


class NoopPublisher:
    def publish_facility(self, payload: dict[str, object]) -> None:
        logger.debug("skipping facility publish for %s", payload.get("id"))

    def publish_gap(self, payload: dict[str, object]) -> None:
        logger.debug("skipping gap publish for %s", payload.get("id"))


class KafkaEventPublisher:
    def __init__(self, bootstrap_servers: str, facilities_topic: str, gaps_topic: str):
        if KafkaProducer is None:
            raise RuntimeError("kafka-python is not available")
        self.facilities_topic = facilities_topic
        self.gaps_topic = gaps_topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )

    def publish_facility(self, payload: dict[str, object]) -> None:
        self.producer.send(self.facilities_topic, payload)

    def publish_gap(self, payload: dict[str, object]) -> None:
        self.producer.send(self.gaps_topic, payload)


def build_publisher() -> Publisher:
    settings = get_settings()
    if settings.publisher_backend == "kafka":
        return KafkaEventPublisher(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            facilities_topic=settings.kafka_topic_facilities,
            gaps_topic=settings.kafka_topic_gaps,
        )
    return NoopPublisher()

