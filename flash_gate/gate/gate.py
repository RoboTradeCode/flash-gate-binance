import asyncio
import json
import logging
import uuid
from typing import NoReturn, Coroutine
from flash_gate.exchange import CcxtExchange, ExchangePool
from flash_gate.transmitter import AeronTransmitter
from flash_gate.transmitter.enums import EventAction, Destination
from flash_gate.transmitter.types import Event, EventNode
from .parsers import ConfigParser
from flash_gate.cache.memcached import Memcached

logger = logging.getLogger(__name__)
lock = asyncio.Lock()


class Gate:
    """
    Шлюз, принимающий команды от торгового ядра и выполняющий их на бирже
    """

    def __init__(self, config: dict):
        config_parser = ConfigParser(config)
        exchange_id = config_parser.exchange_id
        exchange_config = config_parser.exchange_config

        self.event_id_by_client_order_id = Memcached(key_prefix="event_id")
        self.order_id_by_client_order_id = Memcached(key_prefix="order_id")
        self.transmitter = AeronTransmitter(self.handler, config)
        self.exchange = CcxtExchange(exchange_id, exchange_config)
        self.exchange_pool = ExchangePool(
            exchange_id,
            config_parser.public_config,
            config_parser.public_ip,
            config_parser.public_delay,
        )

        self.tickers = config_parser.tickers
        self.assets = config_parser.assets

        self.orderbooks_received = 0
        self.open_orders = set()

        self.balance_delay = config_parser.balance_delay
        self.orders_delay = config_parser.order_status_delay
        self.orderbooks_delay = config_parser.order_book_delay

    async def run(self) -> NoReturn:
        tasks = self.get_periodical_tasks()
        await asyncio.gather(*tasks)

    def get_periodical_tasks(self) -> list[Coroutine]:
        return [
            self.transmitter.run(),
            self.watch_orderbooks(),
            # self.watch_balance(),
            # self.watch_orders(),
            # self.health_check(),
        ]

    def handler(self, message: str):
        logger.info("Message: %s", message)
        event = self.deserialize_message(message)
        task = self.get_task(event)
        asyncio.create_task(task)

    def deserialize_message(self, message: str) -> Event:
        try:
            event = json.loads(message)
            self.log(event)
            return event
        except Exception as e:
            logger.error("Message deserialize error: %s", e)

    def log(self, event: Event):
        event = event.copy()
        event["node"] = EventNode.GATE
        self.transmitter.offer(event, Destination.LOGS)

    def get_task(self, event: Event) -> Coroutine:
        if not isinstance(event, dict):
            return asyncio.sleep(0)

        match event.get("action"):
            case EventAction.CREATE_ORDERS:
                return self.create_orders(event)
            case EventAction.CANCEL_ORDERS:
                return self.cancel_orders(event)
            case EventAction.CANCEL_ALL_ORDERS:
                return self.cancel_all_orders()
            case EventAction.GET_ORDERS:
                return self.get_orders(event)
            case EventAction.GET_BALANCE:
                return self.get_balance(event)

    async def create_orders(self, event: Event):
        for param in event.get("data", []):
            await self.create_order(param, event.get("event_id"))

    async def get_orders(self, event: Event):
        for param in event.get("data", []):
            await self.get_order(param)

    async def cancel_orders(self, event: Event):
        for param in event.get("data", []):
            await self.cancel_order(param)

    async def cancel_all_orders(self):
        try:
            async with lock:
                await self.exchange.cancel_all_orders(self.tickers)

        except Exception as e:
            logger.exception(e)

    async def create_order(self, param: dict, event_id: str):
        try:
            async with lock:
                order = await self.exchange.create_order(param)

            order["client_order_id"] = param["client_order_id"]
            self.event_id_by_client_order_id.set(order["client_order_id"], event_id)
            self.order_id_by_client_order_id.set(order["client_order_id"], order["id"])
            self.open_orders.add(
                {"client_order_id": order["client_order_id"], "symbol": order["symbol"]}
            )

            print(order)

            event: Event = {
                "event_id": event_id,
                "action": EventAction.CREATE_ORDERS,
                "data": [order],
            }
            self.transmitter.offer(event, Destination.CORE)
            self.transmitter.offer(event, Destination.LOGS)

        except Exception as e:
            logger.exception(e)

    async def cancel_order(self, param: dict):
        try:
            order_id = self.order_id_by_client_order_id.get(param["client_order_id"])
            symbol = param["symbol"]

            async with lock:
                await self.exchange.cancel_order({"id": order_id, "symbol": symbol})

        except Exception as e:
            logger.exception(e)

    async def get_order(self, param: dict):
        try:
            order_id = self.order_id_by_client_order_id.get(param["client_order_id"])
            symbol = param["symbol"]

            async with lock:
                order = await self.exchange.fetch_order(
                    {"id": order_id, "symbol": symbol}
                )

            order["client_order_id"] = param["client_order_id"]

            event: Event = {
                "event_id": self.event_id_by_client_order_id.get(
                    order["client_order_id"]
                ),
                "action": EventAction.GET_ORDERS,
                "data": [order],
            }
            self.transmitter.offer(event, Destination.CORE)
            self.transmitter.offer(event, Destination.LOGS)

        except Exception as e:
            logger.exception(e)

    async def get_balance(self, event: Event):
        try:
            if not (assets := event.get("data", [])):
                assets = self.assets

            async with lock:
                balance = await self.exchange.fetch_partial_balance(assets)

            event: Event = {
                "event_id": event["event_id"],
                "action": EventAction.GET_BALANCE,
                "data": balance,
            }
            self.transmitter.offer(event, Destination.BALANCE)
            self.transmitter.offer(event, Destination.LOGS)

        except Exception as e:
            logger.exception(e)

    async def watch_orderbooks(self):
        while True:
            try:
                exchange = await self.exchange_pool.acquire()

                async with lock:
                    orderbooks = await exchange.fetch_order_books(self.tickers, 10)

                self.orderbooks_received += len(orderbooks)

                for orderbook in orderbooks:
                    event: Event = {
                        "event_id": str(uuid.uuid4()),
                        "action": EventAction.ORDER_BOOK_UPDATE,
                        "data": orderbook,
                    }
                    self.transmitter.offer(event, Destination.ORDER_BOOK)

            except Exception as e:
                logger.exception(e)

            await asyncio.sleep(self.orderbooks_delay)

    async def watch_balance(self):
        while True:
            try:
                async with lock:
                    balance = await self.exchange.fetch_partial_balance(self.assets)

                event: Event = {
                    "event_id": str(uuid.uuid4()),
                    "action": EventAction.BALANCE_UPDATE,
                    "data": balance,
                }
                self.transmitter.offer(event, Destination.BALANCE)
                self.transmitter.offer(event, Destination.LOGS)

            except Exception as e:
                logger.exception(e)

            await asyncio.sleep(self.balance_delay)

    async def watch_orders(self):
        while True:
            for param in self.open_orders.copy():
                try:
                    order_id = self.order_id_by_client_order_id.get(
                        param["client_order_id"]
                    )
                    symbol = param["symbol"]

                    async with lock:
                        order = await self.exchange.fetch_order(
                            {"id": order_id, "symbol": symbol}
                        )

                    order["client_order_id"] = param["client_order_id"]

                    if order["status"] != "open":
                        self.open_orders.discard(param)

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
                    logger.exception(e)

                await asyncio.sleep(self.orders_delay)
            await asyncio.sleep(0)

    async def health_check(self) -> NoReturn:
        while True:
            await self.ping()
            await asyncio.sleep(1)

    async def ping(self):
        event: Event = {
            "event_id": str(uuid.uuid4()),
            "action": EventAction.PING,
            "data": self.orderbooks_received,
        }
        self.transmitter.offer(event, Destination.LOGS)

    async def close(self):
        await self.exchange.close()
        await self.exchange_pool.close()
        self.transmitter.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
