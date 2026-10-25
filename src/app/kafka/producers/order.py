import logging
from threading import Event
from uuid import uuid4

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

from app.infra.entities.order import Order, OrderStatus
from app.kafka.producers.base import get_producer
from app.kafka.schema_registry_client import get_schema_registry_client
from app.kafka.serialization import get_order_serializer
from app.settings import config

logger = logging.getLogger(__name__)


def delivery_report(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] offset={msg.offset()}")


def send_message_to_order(
    messages: list[Order], *, topic: str = config.KAFKA_ORDER_TOPIC,
) -> None:
    with get_producer() as producer, get_schema_registry_client() as registry:
        serializer = get_order_serializer(registry)
        for message in messages:
            _send(producer, serializer, message, topic)


def _send(
    producer: Producer, serializer: JSONSerializer, message: Order, topic: str,
) -> None:
    try:
        key = StringSerializer("utf_8")(message.user_id)
        value = serializer(message, SerializationContext(topic, MessageField.VALUE))
    except Exception:
        logger.exception(f"Order serialization failed: {message}")
        return
    while True:
        try:
            producer.produce(topic=topic, key=key, value=value, on_delivery=delivery_report)
            logger.info(f"Sending order: {message}")
            producer.poll(0)
            return
        except BufferError:
            producer.poll(1)


def run_producer(stop: Event) -> None:
    with get_producer() as producer, get_schema_registry_client() as registry:
        serializer = get_order_serializer(registry)
        while not stop.is_set():
            for _ in range(30):
                if stop.is_set():
                    break
                order = Order(str(uuid4()), str(uuid4()), ["book", "coffee"], OrderStatus.WAITING)
                _send(producer, serializer, order, config.KAFKA_ORDER_TOPIC)
            producer.poll(0)
            stop.wait(config.PRODUCE_INTERVAL)
