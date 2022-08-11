import time
import itertools
import logging
from abc import ABC, abstractmethod
import ccxtpro
from .enums import StructureType
from .formatters import CcxtFormatterFactory
from .types import OrderBook, Balance, Order, FetchOrderParams, CreateOrderParams
from .nonce import Nonce


class Exchange(ABC):
    """
    Класс для взаимодействия с биржей
    """

    @abstractmethod
    async def fetch_order_book(self, symbol: str, limit: str) -> OrderBook:
        """
        Получить биржевой стакан по HTTP

        :param symbol: Тикер
        :param limit: Предельное количество предложений
        """
        ...

    @abstractmethod
    async def watch_order_book(self, symbol: str, limit: str) -> OrderBook:
        """
        Получить биржевой стакан по WS

        :param symbol: Тикер
        :param limit: Предельное количество предложений
        """
        ...

    @abstractmethod
    async def fetch_partial_balance(self, parts: list[str]) -> Balance:
        """
        Получить баланс тикеров по HTTP

        :param parts: Список тикеров
        """
        ...

    @abstractmethod
    async def watch_partial_balance(self, parts: list[str]) -> Balance:
        """
        Получить баланс тикеров по WS

        :param parts: Список тикеров
        """
        ...

    @abstractmethod
    async def fetch_order(self, params: FetchOrderParams) -> Order:
        """
        Получить ордер по HTTP

        :param params: Параметры ордера
        """
        ...

    async def watch_orders(self) -> list[Order]:
        """
        Получить обновление ордеров по WS
        """
        ...

    @abstractmethod
    async def fetch_open_orders(self, symbols: list[str]) -> list[Order]:
        """
        Получить открытые ордера по HTTP

        :param symbols: Список тикеров
        """
        ...

    async def create_orders(self, orders: list[CreateOrderParams]) -> list[Order]:
        """
        Создать ордера

        :param orders: Список параметров ордеров
        """
        ...

    @abstractmethod
    async def cancel_all_orders(self, symbols: list[str]) -> list[Order]:
        """
        Отменить все открытые ордера

        :param symbols: Список тикеров
        """
        ...

    @abstractmethod
    async def cancel_orders(self, orders: list[FetchOrderParams]) -> list[Order]:
        """
        Отменить ордер

        :param orders: Параметры ордера
        """
        ...


class CcxtExchange(Exchange):
    """
    Класс для взаимодействия с биржей через CCXT
    """

    def __init__(self, exchange_id: str, config: dict):
        self.logger = logging.getLogger(__name__)
        self.exchange: ccxtpro.Exchange = getattr(ccxtpro, exchange_id)(config)
        self.exchange.nonce = self.nonce

    @staticmethod
    def nonce():
        return Nonce.get()

    async def fetch_order_book(self, symbol: str, limit: int) -> OrderBook:
        self.logger.info("Trying to fetch order book: %s", symbol)
        order_book = await self._fetch_order_book(symbol, limit)
        self.logger.info("Order book has been successfully fetched: %s", order_book)
        return order_book

    async def _fetch_order_book(self, symbol: str, limit: int) -> OrderBook:
        raw_order_book = await self.exchange.fetch_order_book(symbol, limit)
        order_book = self._format(raw_order_book, StructureType.ORDER_BOOK)
        return order_book

    async def fetch_order_books(
        self, symbols: list[str], limit: int
    ) -> list[OrderBook]:
        self.logger.info("Trying to fetch order books: %s", symbols)
        order_book = await self._fetch_order_books(symbols, limit)
        self.logger.info("Order book has been successfully fetched: %s", order_book)
        return order_book

    async def _fetch_order_books(
        self, symbols: list[str], limit: int
    ) -> list[OrderBook]:
        raw_order_books = await self.exchange.fetch_order_books(symbols, limit)
        order_books = []
        for symbol in symbols:
            order_book = self._format(raw_order_books[symbol], StructureType.ORDER_BOOK)
            order_books.append(order_book)

        return order_books

    async def watch_order_book(self, symbol: str, limit: int) -> OrderBook:
        self.logger.debug("Trying to watch order book: %s", symbol)
        order_book = await self._watch_order_book(symbol, limit)
        self.logger.debug("Order book has been successfully watched: %s", order_book)
        return order_book

    async def _watch_order_book(self, symbol: str, limit: int) -> OrderBook:
        raw_order_book = await self.exchange.watch_order_book(symbol, limit)
        order_book = self._format(raw_order_book, StructureType.ORDER_BOOK)
        return order_book

    async def fetch_partial_balance(self, parts: list[str]) -> Balance:
        self.logger.info("Trying to fetch partial balance: %s", parts)
        balance = await self._fetch_partial_balance(parts)
        self.logger.info("Partial balance has been successfully fetched: %s", balance)
        return balance

    async def _fetch_partial_balance(self, parts: list[str]) -> Balance:
        raw_balance = await self.exchange.fetch_balance()
        raw_partial_balance = self._get_partial_balance(raw_balance, parts)
        balance = self._format(raw_partial_balance, StructureType.PARTIAL_BALANCE)
        return balance

    async def watch_partial_balance(self, parts: list[str]) -> Balance:
        self.logger.info("Trying to watch partial balance: %s", parts)
        balance = await self._watch_partial_balance(parts)
        self.logger.info("Partial balance has been successfully watched: %s", balance)
        return balance

    async def _watch_partial_balance(self, parts: list[str]) -> Balance:
        raw_balance = await self.exchange.watch_balance()
        raw_partial_balance = self._get_partial_balance(raw_balance, parts)
        balance = self._format(raw_partial_balance, StructureType.PARTIAL_BALANCE)
        return balance

    @staticmethod
    def _get_partial_balance(raw_balance: dict, parts: list[str]) -> dict:
        default = {"free": 0.0, "used": 0.0, "total": 0.0}
        partial_balance = {part: raw_balance.get(part, default) for part in parts}
        return partial_balance

    async def fetch_order(self, params: FetchOrderParams) -> Order:
        self.logger.info("Trying to fetch order: %s", params)
        order = await self._fetch_order(params)
        self.logger.info("Order has been successfully fetched: %s", order)
        return order

    async def _fetch_order(self, params: FetchOrderParams) -> Order:
        # try:
        # raw_order = await self.exchange.fetch_order(params["id"], params["symbol"])
        # order = self._format(raw_order, StructureType.ORDER)
        # self.logger.info("Fetched from fetch: %s", order)
        # return order
        # except ccxtpro.OrderNotFound:
        if order := await self._fetch_order_from_open(params):
            self.logger.info("Fetched from open: %s", order)
            return order
        if order := await self._fetch_order_from_canceled(params):
            self.logger.info("Fetched from canceled: %s", order)
            return order

    async def _fetch_order_from_open(self, params: FetchOrderParams) -> Order:
        open_orders = await self.fetch_open_orders([params["symbol"]])
        for order in open_orders:
            if order["id"] == params["id"]:
                return order

    async def _fetch_order_from_canceled(self, params: FetchOrderParams) -> Order:
        raw_orders = await self.exchange.fetch_canceled_orders(params["symbol"])
        for raw_order in raw_orders:
            if raw_order["id"] == params["id"]:
                raw_order["status"] = "canceled"
                order = self._format(raw_order, StructureType.ORDER)
                return order

    async def fetch_open_orders(self, symbols: list[str]) -> list[Order]:
        self.logger.info("Trying to fetch open orders: %s", symbols)
        orders = await self._fetch_open_orders(symbols)
        self.logger.info("Open orders has been successfully fetched: %s", orders)
        return orders

    async def _fetch_open_orders(self, symbols: list[str]) -> list[Order]:
        raw_orders = await self._fetch_raw_open_orders(symbols)
        orders = [self._format(order, StructureType.ORDER) for order in raw_orders]
        return orders

    async def watch_orders(self) -> list[Order]:
        self.logger.info("Trying to watch orders")
        orders = await self._watch_orders()
        self.logger.info("Orders has been successfully watched: %s", orders)
        return orders

    async def _watch_orders(self) -> list[Order]:
        raw_orders = await self.exchange.watch_orders()
        orders = [self._format(order, StructureType.ORDER) for order in raw_orders]
        return orders

    async def create_orders(self, orders: list[CreateOrderParams]) -> list[Order]:
        self.logger.info("Trying to create orders: %s", orders)
        orders = await self._create_orders(orders)
        self.logger.info("Orders has been successfully created: %s", orders)
        return orders

    async def _create_orders(self, orders: list[CreateOrderParams]) -> list[Order]:
        created_orders = []
        for order in orders:
            created_order = await self._create_order(order)
            created_orders.append(created_order)
        return created_orders

    async def _create_order(self, params: CreateOrderParams) -> Order:
        self.logger.info("Trying to create order: %s", params)
        raw_order = await self.exchange.create_order(
            params["symbol"],
            params["type"],
            params["side"],
            params["amount"],
            params["price"] if params["type"] != "market" else 0,
        )
        order = self._format(raw_order, StructureType.ORDER)
        self.logger.info("Order has been successfully created: %s", order)
        return order

    async def cancel_orders(self, orders: list[FetchOrderParams]) -> None:
        self.logger.info("Trying to cancel orders: %s", orders)
        await self._cancel_orders(orders)
        self.logger.info("Orders has been successfully cancelled")

    async def _cancel_orders(self, orders: list[FetchOrderParams]) -> None:
        for order in orders:
            await self._cancel_order(order)

    async def _cancel_order(self, order: FetchOrderParams) -> None:
        await self.exchange.cancel_order(order["id"], order["symbol"])

    async def cancel_all_orders(self, symbols: list[str]) -> None:
        self.logger.info("Trying to cancel all orders: %s", symbols)
        await self._cancel_all_orders(symbols)
        self.logger.info("All orders has been successfully cancelled")

    async def _cancel_all_orders(self, symbols: list[str]) -> None:
        raw_orders = await self._fetch_raw_open_orders(symbols)
        for raw_order in raw_orders:
            await self.exchange.cancel_order(raw_order["id"], raw_order["symbol"])

    async def _fetch_raw_open_orders(self, symbols: list[str]) -> list[dict]:
        groups = [await self.exchange.fetch_open_orders(symbol) for symbol in symbols]
        raw_orders = list(itertools.chain.from_iterable(groups))
        return raw_orders

    @staticmethod
    def _format(ccxt_structure: dict, ccxt_structure_type: StructureType):
        factory = CcxtFormatterFactory()
        formatter = factory.make_formatter(ccxt_structure_type)
        structure = formatter.format(ccxt_structure)
        return structure

    async def close(self) -> None:
        """
        Закрыть соединение с биржей
        """
        await self.exchange.close()
