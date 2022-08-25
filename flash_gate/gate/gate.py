import asyncio
import json
import logging
import uuid
from typing import NoReturn, Coroutine
import ccxt.base.errors
from flash_gate.cache.memcached import Memcached
from flash_gate.exchange import CcxtExchange, ExchangePool
from flash_gate.transmitter import AeronTransmitter
from flash_gate.transmitter.enums import EventAction, Destination
from flash_gate.transmitter.types import Event, EventNode, EventType
from .parsers import ConfigParser
from ..exchange.pool import PrivateExchangePool

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

        self._exchange = (
            CcxtExchange(exchange_id, exchange_config)
            if config_parser.accounts is None
            else None
        )
        self._private_exchange_pool = (
            PrivateExchangePool(
                exchange_id=exchange_id,
                config=exchange_config,
                accounts=config_parser.accounts,
            )
            if config_parser.accounts is not None
            else None
        )
        self.sem = asyncio.Semaphore(len(config_parser.accounts))

        # Событие, которое наступает после обработки команды ядра
        # Когда событие очищено, шлюз не запрашивает периодические данные
        self.no_priority_commands = asyncio.Event()
        self.no_priority_commands.set()

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

    async def run(self) -> NoReturn:
        tasks = self.get_periodical_tasks()
        await asyncio.gather(*tasks)

    def get_periodical_tasks(self) -> list[Coroutine]:
        return [
            self.transmitter.run(),
            self.watch_orderbooks(),
            self.watch_balance(),
            self.watch_orders(),
            self.health_check(),
        ]

    def handler(self, message: str):
        logger.debug("Message: %s", message)
        event = self.deserialize_message(message)
        task = self.get_task(event)
        asyncio.create_task(task)

    async def get_exchange(self):
        """
        Получить экземпляр биржи

        Возваращет очередной экземпляр из пула или один и тот же экземлпяр, если мульти-аккаунты не используются.
        Позволяет работать с пулом таким образом, как если бы это был атрибут класса.
        """
        if self._private_exchange_pool is not None:
            return await self._private_exchange_pool.acquire()
        return self._exchange

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
        try:
            self.no_priority_commands.clear()
            for param in event.get("data", []):
                await self.create_order(param, event.get("event_id"))
        finally:
            self.no_priority_commands.set()

    async def get_orders(self, event: Event):
        for param in event.get("data", []):
            await self.get_order(param)

    async def cancel_orders(self, event: Event):
        for param in event.get("data", []):
            await self.cancel_order(param)

    async def cancel_all_orders(self):
        try:
            async with self.sem:
                exchange = await self.get_exchange()
                await exchange.cancel_all_orders(self.tickers)

        except Exception as e:
            logger.exception(e)

    async def create_order(self, param: dict, event_id: str):
        try:
            if self.sem.locked():
                logger.info("Need more tokens!")

            async with self.sem:
                exchange = await self.get_exchange()
                order = await exchange.create_order(param)

            order["client_order_id"] = param["client_order_id"]
            self.event_id_by_client_order_id.set(order["client_order_id"], event_id)
            self.order_id_by_client_order_id.set(order["client_order_id"], order["id"])
            self.open_orders.add((order["client_order_id"], order["symbol"]))

            event: Event = {
                "event_id": event_id,
                "action": EventAction.CREATE_ORDERS,
                "data": [order],
            }
            self.transmitter.offer(event, Destination.CORE)
            self.transmitter.offer(event, Destination.LOGS)

        except Exception as e:
            logger.exception(e)
            log_event: Event = {
                "event_id": event_id,
                "event": EventType.ERROR,
                "action": EventAction.CREATE_ORDERS,
                "message": str(e),
                "data": [param],
            }
            self.transmitter.offer(log_event, Destination.CORE)
            self.transmitter.offer(log_event, Destination.LOGS)

    async def cancel_order(self, param: dict):
        order_id = self.order_id_by_client_order_id.get(param["client_order_id"])
        symbol = param["symbol"]

        async with self.sem:
            exchange = await self.get_exchange()
            await exchange.cancel_order({"id": order_id, "symbol": symbol})

        try:
            await self.exchange.cancel_order({"id": order_id, "symbol": symbol})

        except ccxt.base.errors.OrderNotFound as e:
            event: Event = {
                "event_id": self.event_id_by_client_order_id.get(
                    param["client_order_id"]
                ),
                "action": EventAction.ORDERS_UPDATE,
                "data": [
                    {
                        "id": order_id,
                        "client_order_id": param["client_order_id"],
                        "timestamp": None,
                        "status": "canceled",
                        "symbol": symbol,
                        "type": None,
                        "side": None,
                        "price": None,
                        "amount": None,
                        "filled": None,
                    }
                ],
            }
            self.transmitter.offer(event, Destination.CORE)
            self.transmitter.offer(event, Destination.LOGS)

            logger.exception(e)
            log_event: Event = {
                "event_id": str(uuid.uuid4()),
                "event": EventType.ERROR,
                "action": EventAction.CANCEL_ORDERS,
                "message": str(e),
                "data": [param],
            }
            self.transmitter.offer(log_event, Destination.CORE)
            self.transmitter.offer(log_event, Destination.LOGS)

        except Exception as e:
            logger.exception(e)
            log_event: Event = {
                "event_id": str(uuid.uuid4()),
                "event": EventType.ERROR,
                "action": EventAction.CANCEL_ORDERS,
                "message": str(e),
                "data": [param],
            }
            self.transmitter.offer(log_event, Destination.CORE)
            self.transmitter.offer(log_event, Destination.LOGS)

    async def get_order(self, param: dict):
        try:
            order_id = self.order_id_by_client_order_id.get(param["client_order_id"])
            symbol = param["symbol"]

            async with self.sem:
                exchange = await self.get_exchange()
                order = await exchange.fetch_order({"id": order_id, "symbol": symbol})

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
            log_event: Event = {
                "event_id": str(uuid.uuid4()),
                "event": EventType.ERROR,
                "action": EventAction.GET_ORDERS,
                "message": str(e),
                "data": [param],
            }
            self.transmitter.offer(log_event, Destination.CORE)
            self.transmitter.offer(log_event, Destination.LOGS)

    async def get_balance(self, event: Event):
        if not (assets := event.get("data", [])):
            assets = self.assets

        try:
            async with self.sem:
                exchange = await self.get_exchange()
                balance = await exchange.fetch_partial_balance(assets)

            event: Event = {
                "event_id": event["event_id"],
                "action": EventAction.GET_BALANCE,
                "data": balance,
            }
            self.transmitter.offer(event, Destination.BALANCE)
            self.transmitter.offer(event, Destination.LOGS)

        except Exception as e:
            logger.exception(e)
            log_event: Event = {
                "event_id": event["event_id"],
                "event": EventType.ERROR,
                "action": EventAction.GET_BALANCE,
                "message": str(e),
                "data": assets,
            }
            self.transmitter.offer(log_event, Destination.CORE)
            self.transmitter.offer(log_event, Destination.LOGS)

    async def watch_orderbooks(self):
        while True:
            try:
                exchange = await self.exchange_pool.acquire()
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
                log_event: Event = {
                    "event_id": str(uuid.uuid4()),
                    "event": EventType.ERROR,
                    "action": EventAction.ORDER_BOOK_UPDATE,
                    "message": str(e),
                    "data": self.tickers,
                }
                self.transmitter.offer(log_event, Destination.CORE)
                self.transmitter.offer(log_event, Destination.LOGS)

    async def watch_balance(self):
        while True:
            try:
                await self.no_priority_commands.wait()

                async with self.sem:
                    exchange = await self.get_exchange()
                    balance = await exchange.fetch_partial_balance(self.assets)

                event: Event = {
                    "event_id": str(uuid.uuid4()),
                    "action": EventAction.BALANCE_UPDATE,
                    "data": balance,
                }
                self.transmitter.offer(event, Destination.BALANCE)
                self.transmitter.offer(event, Destination.LOGS)

            except Exception as e:
                logger.exception(e)
                log_event: Event = {
                    "event_id": str(uuid.uuid4()),
                    "event": EventType.ERROR,
                    "action": EventAction.BALANCE_UPDATE,
                    "message": str(e),
                    "data": self.assets,
                }
                self.transmitter.offer(log_event, Destination.CORE)
                self.transmitter.offer(log_event, Destination.LOGS)

            await asyncio.sleep(self.balance_delay)

    async def watch_orders(self):
        while True:
            for client_order_id, symbol in self.open_orders.copy():
                try:
                    order_id = self.order_id_by_client_order_id.get(client_order_id)

                    await self.no_priority_commands.wait()

                    async with self.sem:
                        exchange = await self.get_exchange()
                        order = await exchange.fetch_order(
                            {"id": order_id, "symbol": symbol}
                        )

                    order["client_order_id"] = client_order_id

                    if order["status"] != "open":
                        self.open_orders.discard((client_order_id, symbol))

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
                    log_event: Event = {
                        "event_id": str(uuid.uuid4()),
                        "event": EventType.ERROR,
                        "action": EventAction.ORDERS_UPDATE,
                        "message": str(e),
                        "data": [
                            {"client_order_id": client_order_id, "symbol": symbol}
                        ],
                    }
                    self.transmitter.offer(log_event, Destination.CORE)
                    self.transmitter.offer(log_event, Destination.LOGS)
                    self.open_orders.discard((client_order_id, symbol))

                logger.info("Open orders: %s", len(self.open_orders))
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
        await self.exchange_pool.close()
        self.transmitter.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
