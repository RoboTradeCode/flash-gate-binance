import pytest
from flash_gate.exchange.enums import StructureType
from flash_gate.exchange.formatters import Formatter
from flash_gate.exchange.formatters import FormatterFactory, CcxtFormatterFactory
from flash_gate.exchange.types import OrderBook, Balance, Order
from .data import kuna


@pytest.fixture(scope="session", params=[CcxtFormatterFactory])
def formatter_factory(request) -> FormatterFactory:
    yield request.param()


@pytest.fixture(scope="session")
def order_book_formatter(formatter_factory: FormatterFactory) -> Formatter:
    yield formatter_factory.make_formatter(StructureType.ORDER_BOOK)


@pytest.fixture(scope="session")
def balance_formatter(formatter_factory: FormatterFactory) -> Formatter:
    yield formatter_factory.make_formatter(StructureType.PARTIAL_BALANCE)


@pytest.fixture(scope="session")
def order_formatter(formatter_factory: FormatterFactory) -> Formatter:
    yield formatter_factory.make_formatter(StructureType.ORDER)


@pytest.fixture(scope="session", params=[kuna])
def raw_order_book(request) -> dict:
    yield request.param.RAW_ORDER_BOOK


@pytest.fixture(scope="session", params=[kuna])
def raw_balance(request) -> dict:
    yield request.param.RAW_BALANCE


@pytest.fixture(scope="session", params=[kuna])
def raw_order(request) -> dict:
    yield request.param.RAW_ORDER


@pytest.fixture(scope="session")
def order_book(order_book_formatter: Formatter, raw_order_book: dict) -> OrderBook:
    yield order_book_formatter.format(raw_order_book)


@pytest.fixture(scope="session")
def balance(balance_formatter: Formatter, raw_balance: dict) -> Balance:
    yield balance_formatter.format(raw_balance)


@pytest.fixture(scope="session")
def order(order_formatter: Formatter, raw_order: dict) -> Order:
    yield order_formatter.format(raw_order)
