from abc import ABC, abstractmethod
from .enums import StructureType
from .types import OrderBook, Balance, Order
from .utils import filter_dict, get_timestamp_in_us


class Formatter(ABC):
    @abstractmethod
    def format(self, structure: dict):
        pass


class FormatterFactory(ABC):
    @abstractmethod
    def make_formatter(self, structure_type: StructureType) -> Formatter:
        pass


class CcxtOrderBookFormatter(Formatter):
    KEYS = ["symbol", "bids", "asks", "timestamp"]

    def format(self, structure: dict) -> OrderBook:
        order_book = filter_dict(structure, self.KEYS)
        order_book["timestamp"] = get_timestamp_in_us(structure)
        return order_book


class CcxtPartialBalanceFormatter(Formatter):
    def format(self, structure: dict) -> Balance:
        return {"assets": structure, "timestamp": get_timestamp_in_us(structure)}


class CcxtOrderFormatter(Formatter):
    KEYS = [
        "client_order_id",
        "symbol",
        "type",
        "side",
        "amount",
        "price",
        "id",
        "status",
        "filled",
        "timestamp",
        "info",
    ]

    def format(self, structure: dict) -> Order:
        client_order_id = structure["clientOrderId"]
        order = filter_dict(structure, self.KEYS)
        order["client_order_id"] = client_order_id
        order["timestamp"] = get_timestamp_in_us(structure)

        if order["type"] == "market":
            order["status"] = "closed"
            order["filled"] = order["amount"]

        return order


class CcxtFormatterFactory(FormatterFactory):
    def make_formatter(self, structure_type: StructureType) -> Formatter:
        match structure_type:
            case StructureType.ORDER_BOOK:
                return CcxtOrderBookFormatter()
            case StructureType.PARTIAL_BALANCE:
                return CcxtPartialBalanceFormatter()
            case StructureType.ORDER:
                return CcxtOrderFormatter()
            case _:
                raise ValueError(f"Invalid structure type: {structure_type}")
