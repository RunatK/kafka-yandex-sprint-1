import os
from logging import config as logging_config

from .logger import LOGGING

logging_config.dictConfig(LOGGING)

SCHEMA_REGISTRY_BASE_URL = os.getenv("SCHEMA_REGISTRY_BASE_URL", "http://localhost:8081")
KAFKA_BASE_URL = os.getenv("KAFKA_BASE_URL", "localhost:9094,localhost:9095")
KAFKA_ORDER_TOPIC = os.getenv("KAFKA_ORDER_TOPIC", "orders")
KAFKA_SINGLE_GROUP_ID = os.getenv("KAFKA_SINGLE_GROUP_ID", "orders-single")
KAFKA_BATCH_GROUP_ID = os.getenv("KAFKA_BATCH_GROUP_ID", "orders-batch")
KAFKA_BATCH_SIZE = int(os.getenv("KAFKA_BATCH_SIZE", "10"))
KAFKA_FETCH_MIN_BYTES = int(os.getenv("KAFKA_FETCH_MIN_BYTES", "1024"))
KAFKA_FETCH_WAIT_MS = int(os.getenv("KAFKA_FETCH_WAIT_MS", "500"))
KAFKA_PRODUCER_RETRIES = int(os.getenv("KAFKA_PRODUCER_RETRIES", "10"))
PRODUCE_INTERVAL = float(os.getenv("PRODUCE_INTERVAL", "1"))

if KAFKA_BATCH_SIZE < 10:
    raise ValueError("KAFKA_BATCH_SIZE must be at least 10")
if KAFKA_SINGLE_GROUP_ID == KAFKA_BATCH_GROUP_ID:
    raise ValueError("Consumer group IDs must be different")
