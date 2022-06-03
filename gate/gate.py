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

        self.exchange = Exchange(
            gate_config["info"]["exchange"], sandbox_mode, **gate_config["account"]
        )
        self.formatter = Formatter(config)
        self.core = Core(gate_config["aeron"], self._core_handler)
        self.idle_strategy = AsyncSleepingIdleStrategy(IDLE_SLEEP_MS)
        self.logger = logging.getLogger()
        self.logger.addHandler(aeron_handler)

        self.assets: list[str] = [asset["common"] for asset in assets]
        self.symbols: list[str] = [market["common_symbol"] for market in markets]
        self.depth: int = gate_config["info"]["depth"]
        self.data = 0
        self.ping_delay = gate_config["info"]["ping_delay"]

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def _core_handler(self, message: str):
        try:
            logging.info("Сообщение от ядра: %s", message)
            message = json.loads(message)

            match message:
                case {"event": "command", "action": "create_order"}:
                    logging.info("Создание ордеров: %s", message["data"])
                    task = self.exchange.create_orders(message["data"])
                case {"event": "command", "action": "cancel_order"}:
                    logging.info("Отмена ордеров: %s", message["data"])
                    task = self.exchange.cancel_orders(message["data"])
                case {"event": "command", "action": "cancel_all_orders"}:
                    logging.info("Отмена всех ордеров")
                    task = self.exchange.cancel_all_orders()
                case {"event": "command", "action": "order_status"}:
                    logging.info("Получение статуса ордера: %s", message["data"])
                    task = self._order_status(message["data"])
                case {"event": "command", "action": "get_balances"}:
                    logging.info("Получение баланса: %s", message["data"]["assets"])
                    parts = message["data"]["assets"]
                    task = self._get_balances(parts)
                case _:
                    task = None
                    logging.warning("Неизвестная команда от ядра: %s", message)

            if task is not None:
                asyncio.create_task(task)

        except Exception as e:
            logging.error("Ошибка в обработчике команды от ядра: %s", e)

    async def _poll(self):
        while True:
            fragments_read = self.core.poll()
            await self.idle_strategy.idle(fragments_read)

    async def _order_status(self, order):
        order = await self.exchange.fetch_order(order)
        message = self.formatter.format(order, "order_status")
        logging.info("Отправка ордера ядру")
        self.logger.info(json.dumps(message))
        self.core.offer(message)

    async def _get_balances(self, parts):
        balance = await self.exchange.fetch_partial_balances(parts)
        message = self.formatter.format(balance, "balances")
        logging.info("Отправка баланса ядру")
        self.logger.info(json.dumps(message))
        self.core.offer(message)

    async def _watch_order_book(self, symbol, limit):
        while True:
            orderbook = await self.exchange.watch_order_book(symbol, limit)
            self.data += 1

            message = self.formatter.format(orderbook, "orderbook", symbol)
            self.core.offer(message)

    async def _watch_order_books(self):
        tasks = [self._watch_order_book(symbol, self.depth) for symbol in self.symbols]
        await asyncio.gather(*tasks)

    async def _watch_balance(self) -> None:
        while True:
            balance = await self.exchange.watch_balance()
            default_balance = {"free": 0.0, "used": 0.0, "total": 0.0}
            balance = {part: balance.get(part, default_balance) for part in self.assets}
            message = self.formatter.format(balance, "balances")
            logging.info("Отправка баланса ядру")
            self.logger.info(json.dumps(message))
            self.core.offer(message)

    async def _watch_orders(self) -> None:
        while True:
            orders = await self.exchange.watch_orders()

            for order in orders:
                match order["status"]:
                    case "open":
                        action = "order_created"
                    case "closed":
                        action = "order_closed"
                    case _:
                        action = "order_status"

                message = self.formatter.format(order, action)
                logging.info("Отправка ордера ядру")
                self.logger.info(json.dumps(message))
                self.core.offer(message)

    async def _ping(self):
        while True:
            message = self.formatter.format(self.data, "ping")
            logging.info("Пинг")
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
