import pytest
from flash_gate.exchange.types import OrderBook, Balance, Order


ORDER_BOOK_KEYS = [
    "bids",
    "asks",
    "symbol",
    "timestamp",
]
BALANCE_KEYS = [
    "assets",
    "timestamp",
]
ORDER_KEYS = [
    "id",
    "client_order_id",
    "timestamp",
    "status",
    "symbol",
    "type",
    "side",
    "price",
    "amount",
    "filled",
    "info",
]


class TestOrderBook:
    def test_order_book_is_dict(self, order_book: OrderBook):
        assert isinstance(order_book, dict)

    @pytest.mark.parametrize("key", ORDER_BOOK_KEYS)
    def test_order_book_has_key(self, order_book: OrderBook, key: str):
        assert key in order_book

    def test_order_book_has_not_additional_keys(self, order_book: OrderBook):
        assert set(order_book) == set(ORDER_BOOK_KEYS)

    def test_order_book_timestamp_contains_16_digits(self, order_book: OrderBook):
        if (timestamp_in_us := order_book["timestamp"]) is not None:
            assert len(str(timestamp_in_us)) == 16


class TestBalance:
    def test_balance_is_dict(self, balance: Balance):
        assert isinstance(balance, dict)

    @pytest.mark.parametrize("key", BALANCE_KEYS)
    def test_balance_has_key(self, balance: Balance, key: str):
        assert key in balance

    def test_balance_has_not_additional_keys(self, balance: Balance):
        assert set(balance) == set(BALANCE_KEYS)

    def test_balance_timestamp_contains_16_digits(self, balance: Balance):
        if (timestamp_in_us := balance["timestamp"]) is not None:
            assert len(str(timestamp_in_us)) == 16


class TestOrder:
    def test_order_is_dict(self, order: Order):
        assert isinstance(order, dict)

    @pytest.mark.parametrize("key", ORDER_KEYS)
    def test_order_has_key(self, order: Order, key: str):
        assert key in order

    def test_order_has_not_additional_keys(self, order: Order):
        assert set(order) == set(ORDER_KEYS)

    def test_order_timestamp_contains_16_digits(self, order: Order):
        if (timestamp_in_us := order["timestamp"]) is not None:
            assert len(str(timestamp_in_us)) == 16
