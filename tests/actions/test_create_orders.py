import pytest

MARKET_BUY_ORDER = ...
MARKET_SELL_ORDER = ...
LIMIT_BUY_ORDER = ...
LIMIT_SELL_ORDER = ...

EVENTS = [MARKET_BUY_ORDER, MARKET_SELL_ORDER, LIMIT_BUY_ORDER, LIMIT_SELL_ORDER]


def create_orders_handler(message: str) -> None:



@pytest.mark.parametrize("event", EVENTS)
def test_create_orders(event):
    core_publisher


    subscriber = Subscriber()
    yield subscriber
    subscriber.close()
