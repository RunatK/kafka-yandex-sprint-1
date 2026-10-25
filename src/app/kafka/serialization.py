import json
from typing import TypedDict

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.serialization import SerializationContext

from app.infra.entities.order import Order, OrderStatus

ORDER_SCHEMA = json.dumps({
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Order",
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "user_id": {"type": "string"},
        "items": {"type": "array", "items": {"type": "string"}},
        "status": {"type": "string", "enum": [status.value for status in OrderStatus]},
    },
    "required": ["id", "user_id", "items", "status"],
    "additionalProperties": False,
})


class OrderData(TypedDict):
    id: str
    user_id: str
    items: list[str]
    status: str


def order_to_dict(order: Order, context: SerializationContext) -> OrderData:
    return {
        "id": order.id,
        "user_id": order.user_id,
        "items": order.items,
        "status": order.status,
    }


def order_from_dict(data: OrderData, context: SerializationContext) -> Order:
    return Order(**data)


def get_order_serializer(schema_registry_client: SchemaRegistryClient) -> JSONSerializer:
    serializer: JSONSerializer = JSONSerializer(
        ORDER_SCHEMA, schema_registry_client,
        to_dict=order_to_dict,
    )
    return serializer


def get_order_deserializer(schema_registry_client: SchemaRegistryClient) -> JSONDeserializer:
    deserializer: JSONDeserializer = JSONDeserializer(
        ORDER_SCHEMA,
        from_dict=order_from_dict,
        schema_registry_client=schema_registry_client,
    )
    return deserializer
