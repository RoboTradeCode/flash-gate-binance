from enum import Enum


class StructureType(str, Enum):
    """
    Тип структуры
    """

    ORDER_BOOK = "order_book"
    PARTIAL_BALANCE = "partial_balance"
    ORDER = "order"
