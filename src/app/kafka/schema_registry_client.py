import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient

from app.settings import config

logger = logging.getLogger(__name__)


def get_config() -> dict[str, Any]:
    return {"url": config.SCHEMA_REGISTRY_BASE_URL}


@contextmanager
def get_schema_registry_client(
    get_config: Callable[[], dict[str, Any]] = get_config,
) -> Iterator[SchemaRegistryClient]:
    client = SchemaRegistryClient(get_config())
    try:
        logger.info("Schema registry client open")
        yield client
    finally:
        client.close()
        logger.info("Schema registry client closed")
