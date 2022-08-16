import asyncio
import json
import logging
import uuid
from typing import NoReturn, Coroutine
from flash_gate.exchange import CcxtExchange, ExchangePool
from flash_gate.exchange.types import FetchOrderParams, CreateOrderParams
from flash_gate.transmitter import AeronTransmitter
from flash_gate.transmitter.enums import EventAction, Destination
from flash_gate.transmitter.types import Event, EventType
from flash_gate.cache.memcached import Memcached
from .parsers import ConfigParser
from more_itertools import chunked

PING_DELAY_IN_SECONDS = 1


class Gate:
    """
    Шлюз, принимающий команды от торгового ядра и выполняющий их на бирже
    """

    def __init__(self, config: dict):
        config_parser = ConfigParser(config)
        exchange_id = config_parser.exchange_id
        exchange_config = config_parser.exchange_config

        self.logger = logging.getLogger(__name__)
        self.exchange = CcxtExchange(exchange_id, exchange_config)
        self.exchange.exchange.set_sandbox_mode(config_parser.sandbox_mode)
        self.transmitter = AeronTransmitter(self._handler, config)

        self.data_collection_method = config_parser.data_collection_method
        self.subscribe_delay = config_parser.subscribe_delay
        self.fetch_delays = config_parser.fetch_delays
        self.tickers = config_parser.tickers
        self.fetch_orderbooks = config_parser.fetch_orderbooks
        self.order_book_limit = config_parser.order_book_limit
        self.assets = config_parser.assets
        self.id_by_client_order_id = Memcached(
            key_prefix="client_order_id", default_noreply=False
        )

        self.tracked_orders: set[tuple[str, str]] = set()
        self.event_id_by_client_order_id = Memcached(
            key_prefix="event_id", default_noreply=False
        )
        self.order_books_received = 0
        self.lock = asyncio.Lock()

        self.public_pool = ExchangePool(
            exchange_id,
            config_parser.public_config,
            config_parser.public_ip,
            config_parser.public_delay,
        )
        self.private_pool = ExchangePool(
            exchange_id,
            exchange_config,
            config_parser.private_ip,
            config_parser.private_delay,
        )

        self.order_book_delay = config_parser.order_book_delay
        self.balance_delay = config_parser.balance_delay
        self.order_status_delay = config_parser.order_status_delay

    async def run(self) -> NoReturn:
        tasks = self._get_periodical_tasks()
        await asyncio.gather(*tasks)

    def _get_periodical_tasks(self) -> list[Coroutine]:
        return [
            self.transmitter.run(),
            self._watch_order_books(),
            self._watch_balance(),
            self._watch_orders(),
            self._health_check(),
        ]

    def _handler(self, message: str) -> None:
        self.logger.info("Received message: %s", message)
        event = self._deserialize_message(message)
        task = self._get_task(event)
        asyncio.create_task(task)

    def _deserialize_message(self, message: str) -> Event:
        try:
            event = json.loads(message)
            event_to_log = event
            event_to_log["node"] = "gate"
            self.transmitter.offer(event, Destination.LOGS)
            return event
        except Exception as e:
            self.logger.error("Message deserialize error: %s", e)

    def _get_task(self, event: Event) -> Coroutine:
        if not isinstance(event, dict):
            return asyncio.sleep(0)

        # TODO: Создавать полиморфные классы
        match event.get("action"):
            case EventAction.CREATE_ORDERS:
                return self._create_orders(event)
            case EventAction.CANCEL_ORDERS:
                return self._cancel_orders(event)
            case EventAction.CANCEL_ALL_ORDERS:
                return self._cancel_all_orders()
            case EventAction.GET_ORDERS:
                return self._get_orders(event)
            case EventAction.GET_BALANCE:
                return self._get_balance(event)
            case _:
                self.logger.error("Unknown action: %s", event.get("action"))
                return asyncio.sleep(0)

    async def _create_orders(self, event: Event):
        async with self.lock:
            try:
                event_id = event["event_id"]
                orders: list[CreateOrderParams] = event["data"]

                created_orders = []
                for order in orders:
                    try:
                        created_order = (await self.exchange.create_orders([order]))[0]
                    except Exception as e:
                        event: Event = {
                            "event_id": event_id,
                            "action": EventAction.CREATE_ORDERS,
                            "data": created_orders,
                        }
                        self.transmitter.offer(event, Destination.CORE)
                        self.transmitter.offer(event, Destination.LOGS)
                        continue

                    _order = (order["client_order_id"], order["symbol"])
                    self.tracked_orders.add(_order)
                    self.id_by_client_order_id.set(
                        str(order["client_order_id"]), created_order["id"]
                    )
                    self.id_by_client_order_id.set(
                        str(created_order["id"]), order["client_order_id"]
                    )
                    created_order["client_order_id"] = order["client_order_id"]
                    created_orders.append(created_order)

                self._associate_with_event(event_id, orders)

                event: Event = {
                    "event_id": event_id,
                    "action": EventAction.CREATE_ORDERS,
                    "data": created_orders,
                }
                self.transmitter.offer(event, Destination.CORE)
                self.transmitter.offer(event, Destination.LOGS)

            except Exception as e:
                self.logger.exception(e)
                log_event: Event = {
                    "event_id": str(uuid.uuid4()),
                    "event": EventType.ERROR,
                    "action": EventAction.CREATE_ORDERS,
                    "data": str(e),
                }
                self.transmitter.offer(log_event, Destination.LOGS)

    def _update_client_order_id(self, order: dict) -> dict:
        order = order.copy()
        order["clientOrderId"] = self._get_client_order_id_by_id(order["id"])
        return order

    def _get_id_by_client_order_id(self, client_order_id: str) -> str:
        if order_id := self.id_by_client_order_id.get(str(client_order_id)):
            return order_id

    def _get_client_order_id_by_id(self, order_id: str) -> str:
        if client_order_id := self.id_by_client_order_id.get(str(order_id)):
            return client_order_id

    def _associate_with_event(
        self, event_id: str, orders: list[CreateOrderParams]
    ) -> None:
        client_order_ids = self._get_client_order_ids(orders)
        for client_order_id in client_order_ids:
            self.event_id_by_client_order_id.set(str(client_order_id), event_id)
            self.event_id_by_client_order_id.set(str(event_id), client_order_id)

    @staticmethod
    def _get_client_order_ids(orders: list[CreateOrderParams]) -> list[str]:
        return [order["client_order_id"] for order in orders]

    async def _cancel_orders(self, event: Event) -> None:
        async with self.lock:
            try:
                orders: list[FetchOrderParams] = []
                for order in event["data"]:
                    orders.append(
                        {
                            "id": self._get_id_by_client_order_id(
                                order["client_order_id"]
                            ),
                            "symbol": order["symbol"],
                        }
                    )
                await self.exchange.cancel_orders(orders)

            except Exception as e:
                self.logger.exception(e)
                log_event: Event = {
                    "event_id": str(uuid.uuid4()),
                    "event": EventType.ERROR,
                    "action": EventAction.CANCEL_ORDERS,
                    "message": str(e),
                    "data": event["data"],
                }
                self.transmitter.offer(log_event, Destination.LOGS)

            try:
                orders: list[FetchOrderParams] = []
                for order in event["data"]:
                    orders.append(
                        {
                            "id": self._get_id_by_client_order_id(
                                order["client_order_id"]
                            ),
                            "symbol": order["symbol"],
                        }
                    )

                for order in orders:
                    await self._get_order(order)

            except Exception as e:
                self.logger.exception(e)

    async def _cancel_all_orders(self) -> None:
        async with self.lock:
            try:
                await self.exchange.cancel_all_orders(self.tickers)
            except Exception as e:
                self.logger.exception(e)
                log_event: Event = {
                    "event_id": str(uuid.uuid4()),
                    "event": EventType.ERROR,
                    "action": EventAction.CANCEL_ALL_ORDERS,
                    "data": str(e),
                }
                self.transmitter.offer(log_event, Destination.LOGS)

    async def _get_orders(self, event: Event) -> None:
        async with self.lock:
            orders: list[FetchOrderParams] = []
            for order in event["data"]:
                orders.append(
                    {
                        "id": self._get_id_by_client_order_id(order["client_order_id"]),
                        "symbol": order["symbol"],
                    }
                )

            for order in orders:
                await self._get_order(order)

    async def _get_order(self, order):
        try:
            order = await self.exchange.fetch_order(order)
            order["client_order_id"] = self.id_by_client_order_id.get(str(order["id"]))

            event: Event = {
                "event_id": self.event_id_by_client_order_id.get(
                    str(order["client_order_id"])
                ),
                "action": EventAction.GET_ORDERS,
                "data": [order],
            }
            self.transmitter.offer(event, Destination.CORE)
            self.transmitter.offer(event, Destination.LOGS)
        except Exception as e:
            self.logger.exception(e)
            log_event: Event = {
                "event_id": str(uuid.uuid4()),
                "event": EventType.ERROR,
                "action": EventAction.GET_ORDERS,
                "data": str(e),
            }
            self.transmitter.offer(log_event, Destination.LOGS)

    async def _get_balance(self, event: Event) -> None:
        async with self.lock:
            try:
                if not (assets := event["data"]):
                    assets = self.assets

                balance = await self.exchange.fetch_partial_balance(assets)
                event: Event = {
                    "event_id": event["event_id"],
                    "action": EventAction.GET_BALANCE,
                    "data": balance,
                }
                self.transmitter.offer(event, Destination.BALANCE)
                self.transmitter.offer(event, Destination.LOGS)

            except Exception as e:
                self.logger.exception(e)
                log_event: Event = {
                    "event_id": str(uuid.uuid4()),
                    "event": EventType.ERROR,
                    "action": EventAction.GET_BALANCE,
                    "data": str(e),
                }
                self.transmitter.offer(log_event, Destination.LOGS)

    async def _watch_order_books(self):
        while True:
            try:
                for chunk in chunked(self.tickers, self.fetch_orderbooks):
                    await asyncio.sleep(self.order_book_delay)
                    exchange = await self.public_pool.acquire()

                    order_books = await exchange.fetch_order_books(
                        chunk, self.order_book_limit
                    )

                    self.order_books_received += len(order_books)
                    for order_book in order_books:
                        event: Event = {
                            "event_id": str(uuid.uuid4()),
                            "action": EventAction.ORDER_BOOK_UPDATE,
                            "data": order_book,
                        }
                        self.transmitter.offer(event, Destination.ORDER_BOOK)

            except Exception as e:
                self.logger.exception(e)
                log_event: Event = {
                    "event_id": str(uuid.uuid4()),
                    "event": EventType.ERROR,
                    "action": EventAction.ORDER_BOOK_UPDATE,
                    "data": str(e),
                }
                self.transmitter.offer(log_event, Destination.LOGS)

    async def _watch_balance(self) -> None:
        while True:
            try:
                exchange = await self.private_pool.acquire()

                async with self.lock:
                    balance = await exchange.fetch_partial_balance(self.assets)
                    event: Event = {
                        "event_id": str(uuid.uuid4()),
                        "action": EventAction.BALANCE_UPDATE,
                        "data": balance,
                    }
                    self.transmitter.offer(event, Destination.BALANCE)
                    self.transmitter.offer(event, Destination.LOGS)

                await asyncio.sleep(self.balance_delay)

            except Exception as e:
                self.logger.exception(e)
                log_event: Event = {
                    "event_id": str(uuid.uuid4()),
                    "event": EventType.ERROR,
                    "action": EventAction.BALANCE_UPDATE,
                    "data": str(e),
                }
                self.transmitter.offer(log_event, Destination.LOGS)

    async def _watch_orders(self) -> None:
        while True:
            exchange = await self.private_pool.acquire()

            async with self.lock:
                try:
                    orders = []
                    cp = self.tracked_orders.copy()
                    for _order in cp:
                        params = {
                            "id": self._get_id_by_client_order_id(_order[0]),
                            "symbol": _order[1],
                        }

                        try:
                            order = await exchange.fetch_order(params)
                            order["client_order_id"] = _order[0]
                            orders.append(order)

                            if order["status"] != "open":
                                self.tracked_orders.discard(_order)

                        except ValueError as e:
                            print(e)

                    for order in orders:
                        event: Event = {
                            "event_id": self.event_id_by_client_order_id.get(
                                str(order["client_order_id"])
                            ),
                            "action": EventAction.ORDERS_UPDATE,
                            "data": [order],
                        }
                        self.transmitter.offer(event, Destination.CORE)
                        self.transmitter.offer(event, Destination.LOGS)

                except Exception as e:
                    self.logger.exception(e)
                    log_event: Event = {
                        "event_id": str(uuid.uuid4()),
                        "event": EventType.ERROR,
                        "action": EventAction.GET_BALANCE,
                        "data": str(e),
                    }
                    self.transmitter.offer(log_event, Destination.LOGS)

            await asyncio.sleep(self.order_status_delay)

    async def _health_check(self) -> NoReturn:
        while True:
            self._ping()
            await asyncio.sleep(PING_DELAY_IN_SECONDS)

    def _ping(self):
        event: Event = {
            "event_id": str(uuid.uuid4()),
            "action": EventAction.PING,
            "data": self.order_books_received,
        }
        self.transmitter.offer(event, Destination.LOGS)

    async def close(self):
        await self.public_pool.close()
        await self.private_pool.close()

        await self.exchange.close()
        self.transmitter.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
