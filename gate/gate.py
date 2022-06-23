import asyncio
import json
import logging
from aeron.concurrent import AsyncSleepingIdleStrategy
from .core import Core
from .exchange import Exchange
from .formatter import Formatter
from .logging_handlers import AeronHandler

IDLE_SLEEP_MS = 1


class Gate:
    def __init__(self, config: dict, sandbox_mode: bool = False):
        assets: list = config["data"]["assets_labels"]
        markets: list = config["data"]["markets"]
        gate_config = config["data"]["configs"]["gate_config"]
        aeron_handler = AeronHandler(**gate_config["aeron"]["publishers"]["logs"])

        self.formatter = Formatter(config)
        self.core = Core(gate_config["aeron"], self._core_handler)
        self.idle_strategy = AsyncSleepingIdleStrategy(IDLE_SLEEP_MS)
        self.logger = logging.getLogger("aeron")
        self.logger.addHandler(aeron_handler)

        self.assets: list[str] = [asset["common"] for asset in assets]
        self.symbols: list[str] = [market["common_symbol"] for market in markets]
        self.depth: int = gate_config["info"]["depth"]
        self.data = 0
        self.ping_delay = gate_config["info"]["ping_delay"]

        self.subscribe_timeout = gate_config["rate_limits"]["subscribe_timeout"]

        self.exchange = Exchange(
            gate_config["info"]["exchange"],
            self.symbols,
            sandbox_mode,
            gate_config["rate_limits"]["enable_ccxt_rate_limiter"],
            **gate_config["account"]
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def _core_handler(self, message: str):
        try:
            logging.info("Received message from core: %s", message)
            message = json.loads(message)

            log_message = dict(message)
            log_message["node"] = "gate"
            self.logger.info(json.dumps(log_message))

            match message:
                case {"event": "command", "action": "create_order"}:
                    logging.info("Creating orders: %s", message["data"])
                    task = self.create_orders(message["data"])

                case {"event": "command", "action": "cancel_order"}:
                    logging.info("Cancelling order: %s", message["data"])
                    task = self.cancel_orders(message["data"])

                case {"event": "command", "action": "cancel_all_orders"}:
                    logging.info("Cancelling all orders")
                    task = self.cancel_all_orders()

                case {"event": "command", "action": "order_status"}:
                    logging.info("Getting order status: %s", message["data"])
                    task = self._order_status(message["data"])

                case {"event": "command", "action": "get_balances"}:
                    logging.info("Getting balances: %s", message["data"]["assets"])
                    task = self._get_balances(message["data"]["assets"])

                case _:
                    logging.warning("Unknown message type: %s", message)
                    task = None

            if task is not None:
                asyncio.create_task(task)

        except Exception as e:
            logging.error("Error in processing core command: %s", e)

    async def _poll(self):
        while True:
            fragments_read = self.core.poll()
            await self.idle_strategy.idle(fragments_read)

    async def create_orders(self, orders):
        orders = await self.exchange.create_orders(orders)
        logging.info("Received orders from exchange: %s", orders)

        for order in orders:
            match order["status"]:
                case "open":
                    action = "order_created"
                case "closed":
                    action = "order_closed"
                case _:
                    action = "order_status"

            message = self.formatter.format(order, action)
            logging.info("Sending order to core: %s", message)
            self.logger.info(json.dumps(message))

            self.core.offer(message)

    async def cancel_orders(self, orders):
        orders = await self.exchange.cancel_orders(orders)
        logging.info("Received orders from exchange: %s", orders)

        for order in orders:
            match order["status"]:
                case "open":
                    action = "order_created"
                case "closed":
                    action = "order_closed"
                case _:
                    action = "order_status"

            message = self.formatter.format(order, action)
            logging.info("Sending order to core: %s", message)
            self.logger.info(json.dumps(message))

            self.core.offer(message)

    async def cancel_all_orders(self):
        orders = await self.exchange.cancel_all_orders()
        logging.info("Received orders from exchange: %s", orders)

        for order in orders:
            match order["status"]:
                case "open":
                    action = "order_created"
                case "closed":
                    action = "order_closed"
                case _:
                    action = "order_status"

            message = self.formatter.format(order, action)
            logging.info("Sending order to core: %s", message)
            self.logger.info(json.dumps(message))

            self.core.offer(message)

    async def _order_status(self, order):
        order = await self.exchange.get_order(order)
        logging.info("Received order from exchange: %s", order)

        message = self.formatter.format(order, "order_status")
        logging.info("Sending order status to core: %s", message)
        self.logger.info(json.dumps(message))

        self.core.offer(message)

    async def _get_balances(self, parts):
        balance = await self.exchange.get_balance()
        default_balance = {"free": 0.0, "used": 0.0, "total": 0.0}
        balance = {part: balance.get(part, default_balance) for part in parts}
        logging.info("Received balance from exchange: %s", balance)

        message = self.formatter.format(balance, "balances")
        logging.info("Sending balance to core: %s", message)
        self.logger.info(json.dumps(message))

        self.core.offer(message)

    async def _watch_order_book(self, symbol, limit):
        await asyncio.sleep(self.subscribe_timeout)
        while True:
            orderbook = await self.exchange.get_order_book(symbol, limit)
            self.data += 1

            message = self.formatter.format(orderbook, "orderbook", symbol)
            self.core.offer(message)

    async def _watch_order_books(self):
        tasks = [self._watch_order_book(symbol, self.depth) for symbol in self.symbols]
        await asyncio.gather(*tasks)

    async def _watch_balance(self) -> None:
        while True:
            balance = await self.exchange.get_balance()
            default_balance = {"free": 0.0, "used": 0.0, "total": 0.0}
            balance = {part: balance.get(part, default_balance) for part in self.assets}
            logging.info("Received balance from exchange: %s", balance)

            message = self.formatter.format(balance, "balances")
            logging.info("Sending balance to core: %s", message)
            self.logger.info(json.dumps(message))

            self.core.offer(message)

    async def _watch_orders(self) -> None:
        while True:
            orders = await self.exchange.get_orders()
            logging.info("Received orders from exchange: %s", orders)

            for order in orders:
                match order["status"]:
                    case "open":
                        action = "order_created"
                    case "closed":
                        action = "order_closed"
                    case _:
                        action = "order_status"

                message = self.formatter.format(order, action)
                logging.info("Sending order to core: %s", message)
                self.logger.info(json.dumps(message))

                self.core.offer(message)

    async def _ping(self):
        while True:
            message = self.formatter.format(self.data, "ping")
            self.logger.info(json.dumps(message))
            await asyncio.sleep(self.ping_delay)

    async def run(self):
        tasks = [
            self._poll(),
            self._watch_order_books(),
            self._watch_balance(),
            self._watch_orders(),
            self._ping(),
        ]
        await asyncio.gather(*tasks)

    async def close(self):
        await self.exchange.close()
        self.core.close()
