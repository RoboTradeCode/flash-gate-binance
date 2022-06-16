from typing import TypedDict


class Order(TypedDict):
    id: str
    symbol: str


class PendingOrder(TypedDict):
    symbol: str
    type: str
    side: str
    price: float
    amount: float
