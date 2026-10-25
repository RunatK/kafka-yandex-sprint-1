import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from confluent_kafka import Producer

from app.settings import config

logger = logging.getLogger(__name__)


def get_config(bootstrap_server: str = config.KAFKA_BASE_URL) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap_server,
        "acks": "all",
        "retries": config.KAFKA_PRODUCER_RETRIES,
        "enable.idempotence": True,
        "delivery.timeout.ms": 120000,
    }


@contextmanager
def get_producer(
    producer_config: dict[str, Any] | None = None,
) -> Iterator[Producer]:
    producer = Producer(get_config() if producer_config is None else producer_config)
    try:
        logger.info("Producer open")
        yield producer
    finally:
        remaining = producer.flush(130)
        if remaining:
            raise RuntimeError(f"{remaining} messages were not delivered")
        logger.info("Producer closed")
