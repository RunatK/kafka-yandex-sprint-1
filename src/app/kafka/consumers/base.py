import logging
from abc import ABC, abstractmethod
from threading import Event
from typing import Any

from confluent_kafka import Consumer, KafkaException, Message
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from app.kafka.schema_registry_client import get_schema_registry_client
from app.kafka.serialization import get_order_deserializer
from app.settings import config

logger = logging.getLogger(__name__)


class OrderConsumer(ABC):
    group_id: str
    auto_commit: bool

    def get_config(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": config.KAFKA_BASE_URL,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": self.auto_commit,
            "enable.auto.offset.store": False,
            "fetch.min.bytes": 1 if self.auto_commit else config.KAFKA_FETCH_MIN_BYTES,
            "fetch.wait.max.ms": config.KAFKA_FETCH_WAIT_MS,
        }

    def process(self, message: Message, deserializer: JSONDeserializer) -> None:
        # Invalid records are logged and skipped so that a poison record cannot stop the stream.
        try:
            topic = message.topic()
            if topic is None:
                raise ValueError("Message has no topic")
            order = deserializer(
                message.value(), SerializationContext(topic, MessageField.VALUE)
            )
            logger.info(f"{type(self).__name__} received order: {order!s}")
        except Exception:
            logger.exception(
                f"{type(self).__name__} deserialization/processing failed at "
                f"{message.topic()} [{message.partition()}] offset={message.offset()}"
            )

    def run(self, stop: Event) -> None:
        with get_schema_registry_client() as registry:
            consumer = Consumer(self.get_config())
            try:
                deserializer = get_order_deserializer(registry)
                self.consume(consumer, deserializer, stop)
            finally:
                consumer.close()

    @abstractmethod
    def consume(
        self, consumer: Consumer, deserializer: JSONDeserializer, stop: Event,
    ) -> None:
        raise NotImplementedError

    @staticmethod
    def check_message(message: Message) -> None:
        if message.error():
            raise KafkaException(message.error())
