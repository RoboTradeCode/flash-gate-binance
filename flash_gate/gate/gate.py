import asyncio
import json
import logging
import uuid
from typing import NoReturn, Coroutine
from bidict import bidict
from flash_gate.exchange import CcxtExchange
from flash_gate.exchange.types import FetchOrderParams, CreateOrderParams
from flash_gate.transmitter import AeronTransmitter
from flash_gate.transmitter.enums import EventAction, Destination
from flash_gate.transmitter.types import Event, EventType
from .parsers import ConfigParser

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
        self.transmitter = AeronTransmitter(self._handler, config)

        self.data_collection_method = config_parser.data_collection_method
        self.subscribe_delay = config_parser.subscribe_delay
        self.fetch_delays = config_parser.fetch_delays
        self.tickers = config_parser.tickers
        self.order_book_limit = config_parser.order_book_limit
        self.assets = config_parser.assets

        self.tracked_orders: set[tuple[str, str]] = set()
        self.event_id_by_client_order_id = bidict()
        self.order_books_received = 0

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
        try:
            event_id = event["event_id"]
            orders: list[CreateOrderParams] = event["data"]

            for order in orders:
                _order = (order["client_order_id"], order["symbol"])
                self.tracked_orders.add(_order)

            self._associate_with_event(event_id, orders)
            orders = await self.exchange.create_orders(orders)

            event: Event = {
                "event_id": event_id,
                "action": EventAction.CREATE_ORDERS,
                "data": orders,
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

    def _associate_with_event(
        self, event_id: str, orders: list[CreateOrderParams]
    ) -> None:
        client_order_ids = self._get_client_order_ids(orders)
        for client_order_id in client_order_ids:
            self.event_id_by_client_order_id[client_order_id] = event_id

    @staticmethod
    def _get_client_order_ids(orders: list[CreateOrderParams]) -> list[str]:
        return [order["client_order_id"] for order in orders]

    async def _cancel_orders(self, event: Event) -> None:
        orders: list[FetchOrderParams] = event["data"]
        await self.exchange.cancel_orders(orders)

    async def _cancel_all_orders(self) -> None:
        await self.exchange.cancel_all_orders(self.tickers)

    async def _get_orders(self, event: Event) -> None:
        orders: list[FetchOrderParams] = event["data"]
        for order in orders:
            await self._get_order(order)

    async def _get_order(self, order: FetchOrderParams):
        order = await self.exchange.fetch_order(order)

        event: Event = {
            "event_id": self.event_id_by_client_order_id.get(order["client_order_id"]),
            "action": EventAction.GET_ORDERS,
            "data": [order],
        }
        self.transmitter.offer(event, Destination.CORE)
        self.transmitter.offer(event, Destination.LOGS)

    async def _get_balance(self, event: Event) -> None:
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

    async def _watch_order_books(self):
        tasks = [
            self._watch_order_book(symbol, self.order_book_limit)
            for symbol in self.tickers
        ]
        await asyncio.gather(*tasks)

    async def _watch_order_book(self, symbol, limit):
        await asyncio.sleep(self.subscribe_delay)
        while True:

            if self.data_collection_method["order_book"] == "websocket":
                order_book = await self.exchange.watch_order_book(symbol, limit)
            else:
                order_book = await self.exchange.fetch_order_book(symbol, limit)
                await asyncio.sleep(self.fetch_delays["order_book"])

            self.order_books_received += 1
            event: Event = {
                "event_id": str(uuid.uuid4()),
                "action": EventAction.ORDER_BOOK_UPDATE,
                "data": order_book,
            }
            self.transmitter.offer(event, Destination.ORDER_BOOK)

    async def _watch_balance(self) -> None:
        while True:

            if self.data_collection_method["balance"] == "websocket":
                balance = await self.exchange.watch_partial_balance(self.assets)
            else:
                balance = await self.exchange.fetch_partial_balance(self.assets)
                await asyncio.sleep(self.fetch_delays["balance"])

            event: Event = {
                "event_id": str(uuid.uuid4()),
                "action": EventAction.BALANCE_UPDATE,
                "data": balance,
            }
            self.transmitter.offer(event, Destination.BALANCE)
            self.transmitter.offer(event, Destination.LOGS)

    async def _watch_orders(self) -> None:
        while True:

            try:
                if self.data_collection_method["order"] == "websocket":
                    orders = await self.exchange.watch_orders()
                else:
                    orders = []
                    cp = self.tracked_orders.copy()
                    for _order in cp:
                        params = {
                            "client_order_id": _order[0],
                            "symbol": _order[1],
                        }

                        try:
                            order = await self.exchange.fetch_order(params)
                            orders.append(order)

                            if order["status"] != "open":
                                self.tracked_orders.discard(_order)

                        except ValueError:
                            pass

                await asyncio.sleep(1)
                for order in orders:
                    event: Event = {
                        "event_id": self.event_id_by_client_order_id.get(
                            order["client_order_id"]
                        ),
                        "action": EventAction.ORDERS_UPDATE,
                        "data": [order],
                    }
                    self.transmitter.offer(event, Destination.CORE)
                    self.transmitter.offer(event, Destination.LOGS)
            except Exception as e:
                self.logger.error(e)

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
        await self.exchange.close()
        self.transmitter.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
