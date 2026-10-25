from enum import StrEnum
from typing import NamedTuple


class OrderStatus(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    WAITING = "waiting"


class Order(NamedTuple):
    id: str
    user_id: str
    items: list[str]
    status: str

