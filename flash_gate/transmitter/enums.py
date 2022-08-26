from enum import Enum


class EventType(str, Enum):
    COMMAND = "command"
    DATA = "data"
    ERROR = "error"


class EventNode(str, Enum):
    CORE = "core"
    GATE = "gate"


class EventAction(str, Enum):
    GET_BALANCE = "get_balance"
    CREATE_ORDERS = "create_orders"
    CANCEL_ORDERS = "cancel_orders"
    CANCEL_ALL_ORDERS = "cancel_all_orders"
    GET_ORDERS = "get_orders"
    ORDER_BOOK_UPDATE = "order_book_update"
    BALANCE_UPDATE = "balance_update"
    ORDERS_UPDATE = "orders_update"
    PING = "ping"
    METRICS = "metrics"


class Destination(str, Enum):
    ORDER_BOOK = "orderbooks"
    BALANCE = "balances"
    CORE = "core"
    LOGS = "logs"
