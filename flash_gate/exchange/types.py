from typing import TypedDict, Optional


class OrderBook(TypedDict):
    bids: list
    asks: list
    symbol: str
    timestamp: Optional[int]


class Balance(TypedDict):
    assets: dict
    timestamp: Optional[int]


class Order(TypedDict):
    id: str
    client_order_id: str
    timestamp: int
    status: str
    symbol: str
    type: str
    side: str
    price: float
    amount: float
    filled: float


class FetchOrderParams(TypedDict):
    id: str
    symbol: str


class CreateOrderParams(TypedDict):
    client_order_id: str
    symbol: str
    type: str
    side: str
    amount: float
    price: float
