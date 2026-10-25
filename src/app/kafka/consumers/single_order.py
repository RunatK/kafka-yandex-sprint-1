import logging
from threading import Event

from confluent_kafka import Consumer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

from app.kafka.consumers.base import OrderConsumer
from app.settings import config

logger = logging.getLogger(__name__)


class SingleMessageConsumer(OrderConsumer):
    group_id = config.KAFKA_SINGLE_GROUP_ID
    auto_commit = True

    def consume(
        self, consumer: Consumer, deserializer: JSONDeserializer, stop: Event,
    ) -> None:
        consumer.subscribe([config.KAFKA_ORDER_TOPIC])
        while not stop.is_set():
            try:
                message = consumer.poll(1)
                if message is None:
                    continue
                self.check_message(message)
                self.process(message, deserializer)
                consumer.store_offsets(message=message)
            except Exception:
                logger.exception("Single consumer failed; continuing")
                stop.wait(1)
