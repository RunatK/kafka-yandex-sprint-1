import logging
from threading import Event

from confluent_kafka import Consumer, Message, TopicPartition
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

from app.kafka.consumers.base import OrderConsumer
from app.settings import config

logger = logging.getLogger(__name__)


class BatchMessageConsumer(OrderConsumer):
    group_id = config.KAFKA_BATCH_GROUP_ID
    auto_commit = False

    def consume(
        self, consumer: Consumer, deserializer: JSONDeserializer, stop: Event,
    ) -> None:
        pending: list[Message] = []

        def clear_pending(client: Consumer, partitions: list[TopicPartition]) -> None:
            pending.clear()

        consumer.subscribe([config.KAFKA_ORDER_TOPIC], on_revoke=clear_pending, on_lost=clear_pending)
        while not stop.is_set():
            try:
                messages = consumer.consume(
                    num_messages=config.KAFKA_BATCH_SIZE - len(pending), timeout=1,
                ) if len(pending) < config.KAFKA_BATCH_SIZE else []
                for message in messages:
                    if message.error():
                        logger.error(f"Batch fetch failed: {message.error()}")
                    else:
                        pending.append(message)
                if len(pending) < config.KAFKA_BATCH_SIZE:
                    continue
                for message in pending:
                    self.process(message, deserializer)
                    consumer.store_offsets(message=message)
                consumer.commit(asynchronous=False)
                logger.info(f"Batch processed and committed: {len(pending)} messages")
                pending.clear()
            except Exception:
                logger.exception("Batch consumer failed; continuing")
                stop.wait(1)
        if pending:
            logger.info(f"Leaving {len(pending)} records uncommitted for the next run")
