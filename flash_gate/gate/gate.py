import asyncio
import json
import logging
import uuid
from asyncio import ALL_COMPLETED
from time import monotonic_ns
from typing import NoReturn, Coroutine
import ccxt.base.errors
from flash_gate.cache.memcached import Memcached
from flash_gate.exchange import CcxtExchange, ExchangePool
from flash_gate.exchange.pool import PrivateExchangePool
from flash_gate.transmitter import AeronTransmitter
from flash_gate.transmitter.enums import EventAction, Destination
from flash_gate.transmitter.types import Event, EventNode, EventType
from .formatters import EventFormatter
from .parsers import ConfigParser
from .statistics import latency_percentile, ns_to_us
from .typing import Metrics

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
        self.client_order_id_by_order_id = Memcached(key_prefix="client_order_id")
        self.transmitter = AeronTransmitter(self.handler, config)

        self._exchange = (
            CcxtExchange(exchange_id, exchange_config)
        )
        self._exchange.exchange.set_sandbox_mode(config_parser.sandbox_mode)
        self._private_exchange_pool = (
            PrivateExchangePool(
                exchange_id=exchange_id,
                config=exchange_config,
                accounts=config_parser.accounts,
            )
            if config_parser.accounts is not None
            else None
        )

        self.exchange_pool = ExchangePool(
            exchange_id,
            config_parser.public_config,
            config_parser.public_ip,
            config_parser.public_delay,
        )

        self.tickers = config_parser.tickers
        self.assets = config_parser.assets
        self.open_orders = set()

        self.balance_delay = config_parser.balance_delay
        self.orders_delay = config_parser.order_status_delay

        # Метрики
        self.orderbook_latencies = []
        self.orderbook_rps = 0
        self.private_api_total_rps = 0

        # Strong references to tasks
        self.background_tasks = set()

        # References to tasks that are considered priority. Periodic data
        # will not be requested if the list of priority tasks is not empty
        self.priority_tasks = set()

        # Задержка при получении ордербуков, для того, чтобы не превышать rate limit биржи
        self.orderbook_delay = config_parser.orderbook_delay

    async def run(self) -> NoReturn:
        tasks = self.get_periodical_tasks()
        await asyncio.gather(*tasks)

    def get_periodical_tasks(self) -> list[Coroutine]:
        return [
            self.transmitter.run(),
            self.watch_orderbooks(),
            self.watch_balance(),
            self.watch_orders(),
            self.metrics(),
        ]

    def handler(self, message: str):
        logger.debug("Message: %s", message)
        event = self.deserialize_message(message)
        if event is not None:
            self.create_task(event)

    async def get_exchange(self):
        """
        Получить экземпляр биржи

        Возваращет очередной экземпляр из пула или один и тот же экземлпяр, если мульти-аккаунты не используются.
        Позволяет работать с пулом таким образом, как если бы это был атрибут класса.
        """
        self.private_api_total_rps += 1
        if self._private_exchange_pool is not None:
            return await self._private_exchange_pool.acquire()
        return self._exchange

    def deserialize_message(self, message: str) -> Event | None:
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

    def create_task(self, event: Event):
        priority_task = False

        match event.get("action"):
            case EventAction.CREATE_ORDERS:
                priority_task = True
                action = self.create_orders(event)
            case EventAction.CANCEL_ORDERS:
                priority_task = True
                action = self.cancel_orders(event)
            case EventAction.CANCEL_ALL_ORDERS:
                priority_task = True
                action = self.cancel_all_orders()
            case EventAction.GET_ORDERS:
                action = self.get_orders(event)
            case EventAction.GET_BALANCE:
                action = self.get_balance(event)
            case _:
                logger.error("Unsupported action: %s", event.get("action"))
                action = asyncio.sleep(0)

        task = asyncio.create_task(action)

        # Save reference to result, to avoid task disappearing
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)

        if priority_task:
            self.priority_tasks.add(task)
            task.add_done_callback(self.priority_tasks.discard)

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
            exchange = await self.get_exchange()
            await exchange.cancel_all_orders(self.tickers)

        except Exception as e:
            logger.exception(e)

    async def create_order(self, param: dict, event_id: str):
        try:
            exchange = await self.get_exchange()
            order = await exchange.create_order(param)

            order["client_order_id"] = param["client_order_id"]
            self.client_order_id_by_order_id.set(order["id"], order["client_order_id"])
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
            message = self.describe_exception(e)
            log_event: Event = {
                "event_id": event_id,
                "event": EventType.ERROR,
                "action": EventAction.CREATE_ORDERS,
                "message": message,
                "data": [param],
            }
            self.transmitter.offer(log_event, Destination.CORE)
            self.transmitter.offer(log_event, Destination.LOGS)

    async def cancel_order(self, param: dict):
        order_id = self.order_id_by_client_order_id.get(param["client_order_id"])
        symbol = param["symbol"]

        try:
            exchange = await self.get_exchange()
            await exchange.cancel_order({"id": order_id, "symbol": symbol})

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
            message = self.describe_exception(e)
            log_event: Event = {
                "event_id": str(uuid.uuid4()),
                "event": EventType.ERROR,
                "action": EventAction.CANCEL_ORDERS,
                "message": message,
                "data": [param],
            }
            self.transmitter.offer(log_event, Destination.CORE)
            self.transmitter.offer(log_event, Destination.LOGS)

    async def get_order(self, param: dict):
        try:
            order_id = self.order_id_by_client_order_id.get(param["client_order_id"])
            symbol = param["symbol"]

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
            message = self.describe_exception(e)
            log_event: Event = {
                "event_id": str(uuid.uuid4()),
                "event": EventType.ERROR,
                "action": EventAction.GET_ORDERS,
                "message": message,
                "data": [param],
            }
            self.transmitter.offer(log_event, Destination.CORE)
            self.transmitter.offer(log_event, Destination.LOGS)

    @staticmethod
    def describe_exception(exception: Exception):
        """
        Получить небольшое сообщение, описывающее исключение.
        Логгирует исключение, если оно не относится к ожидаемым.
        """
        if isinstance(exception, ccxt.errors.RequestTimeout):
            message = "Timeout error"
        elif isinstance(exception, ccxt.errors.RateLimitExceeded):
            message = "Rate limit exceeded"
        else:
            logger.exception(exception)
            message = str(exception)
        return message

    async def get_balance(self, event: Event):
        if not (assets := event.get("data", [])):
            assets = self.assets

        try:
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
            message = self.describe_exception(e)
            log_event: Event = {
                "event_id": event["event_id"],
                "event": EventType.ERROR,
                "action": EventAction.GET_BALANCE,
                "message": message,
                "data": assets,
            }
            self.transmitter.offer(log_event, Destination.CORE)
            self.transmitter.offer(log_event, Destination.LOGS)

    async def watch_orderbooks(self):
        while True:
            try:
                await asyncio.sleep(self.orderbook_delay)

                start = monotonic_ns()
                coroutines = []
                for symbol in self.tickers:
                    exchange = await self.exchange_pool.acquire()
                    coroutines.append(exchange.fetch_order_book(symbol, 10))
                orderbooks = await asyncio.gather(*coroutines)
                end = monotonic_ns()

                self.save_orderbook_metric(start, end)

                for orderbook in orderbooks:
                    event: Event = {
                        "event_id": str(uuid.uuid4()),
                        "action": EventAction.ORDER_BOOK_UPDATE,
                        "data": orderbook,
                    }
                    self.transmitter.offer(event, Destination.ORDER_BOOK)

            except Exception as e:
                logger.error(e, exc_info=True)
                message = self.describe_exception(e)
                log_event: Event = {
                    "event_id": str(uuid.uuid4()),
                    "event": EventType.ERROR,
                    "action": EventAction.ORDER_BOOK_UPDATE,
                    "message": message,
                    "data": self.tickers,
                }
                self.transmitter.offer(log_event, Destination.CORE)
                self.transmitter.offer(log_event, Destination.LOGS)

                if isinstance(e, ccxt.errors.DDoSProtection) or isinstance(e, ccxt.errors.RateLimitExceeded):
                    logger.error('Insufficient ip addresses.')

    def save_orderbook_metric(self, start: int, end: int) -> None:
        """
        Сохранить целевые метрики для ордербука
        """
        latency = ns_to_us(end - start)
        self.orderbook_latencies.append(latency)
        self.orderbook_rps += len(self.tickers)

    async def watch_balance(self):
        while True:
            try:
                # Wait for priority commands to complete
                if self.priority_tasks:
                    await asyncio.wait(self.priority_tasks, return_when=ALL_COMPLETED)

                balance = await self._exchange.watch_partial_balance(self.assets)

                event: Event = {
                    "event_id": str(uuid.uuid4()),
                    "action": EventAction.BALANCE_UPDATE,
                    "data": balance,
                }
                self.transmitter.offer(event, Destination.BALANCE)
                self.transmitter.offer(event, Destination.LOGS)

            except Exception as e:
                message = self.describe_exception(e)
                log_event: Event = {
                    "event_id": str(uuid.uuid4()),
                    "event": EventType.ERROR,
                    "action": EventAction.BALANCE_UPDATE,
                    "message": message,
                    "data": self.assets,
                }
                self.transmitter.offer(log_event, Destination.CORE)
                self.transmitter.offer(log_event, Destination.LOGS)


    async def watch_orders(self):
        while True:
            try:
                # Wait for priority commands to complete
                if self.priority_tasks:
                    await asyncio.wait(self.priority_tasks, return_when=ALL_COMPLETED)

                orders = await self._exchange.watch_orders()
                for order in orders:
                    order["client_order_id"] = self.client_order_id_by_order_id.get(order["id"])
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
                message = self.describe_exception(e)
                log_event: Event = {
                    "event_id": str(uuid.uuid4()),
                    "event": EventType.ERROR,
                    "action": EventAction.ORDERS_UPDATE,
                    "message": message,
                    "data": None,
                }
                self.transmitter.offer(log_event, Destination.CORE)
                self.transmitter.offer(log_event, Destination.LOGS)

    async def metrics(self) -> NoReturn:
        while True:
            if len(self.orderbook_latencies) > 1:
                self.offer_metrics()
            await asyncio.sleep(1)

    def offer_metrics(self) -> None:
        """
        Отправить целевые метрики на сервер логирования и сбросить данные
        """
        data = self.get_metrics()
        event = EventFormatter.metrics(data)
        self.transmitter.offer(event, Destination.LOGS)
        self.reset_metrics()

    def get_metrics(self) -> Metrics:
        """
        Получить целевые метрики
        """
        percentile = latency_percentile(self.orderbook_latencies)
        orderbook_rps = self.orderbook_rps
        private_rps = self.private_api_total_rps

        data = EventFormatter.metrics_data(percentile, orderbook_rps, private_rps)
        return data

    def reset_metrics(self) -> None:
        """
        Сбросить данные, по которым считаются метрики
        """
        self.orderbook_latencies = []
        self.orderbook_rps = 0
        self.private_api_total_rps = 0

    async def close(self):
        await self.exchange_pool.close()
        self.transmitter.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
