"""
Реализация торгового гейта
"""
import asyncio
import json
import logging
from typing import Optional
from .core import Core
from .exchange import instantiate_exchange


class Gate:
    """
    Торговый гейт. Подключается к бирже и посылает торговому ядру информацию о биржевых
    стаканах, балансе и ордерах. Выполняет поступающие от торгового ядра команды
    """

    def __init__(self, config: dict):
        # Сохранение конфигурации
        self.config = config

        # Создание экземпляра класса биржы для подключения и начала торговли
        self.exchange = instantiate_exchange(config)

        # Создание каналов Aeron для ядра
        self.core = Core(config, self.handle_command)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.exchange.close()
        self.core.close()

    def handle_command(self, message: str) -> None:
        """
        Функция обратного вызова для приёма команд от ядра

        :param message: Сообщение от ядра
        """
        try:
            command = json.loads(message)

            match command.get("action"):
                case "create_order":
                    task = self.create_orders(command["data"])
                case "cancel_order":
                    task = self.cancel_orders(command["data"])
                case "cancel_all_orders":
                    task = self.cancel_all_orders()
                case "order_status":
                    task = self.order_status(command["data"])
                case "get_balances":
                    task = self.fetch_balance(command["data"])
                case _:
                    task = None
                    logging.warning("Unknown command: %s", command)

            if task is not None:
                asyncio.create_task(task)

        except Exception as e:
            logging.exception(e)

    async def create_orders(self, orders: list[dict]) -> None:
        """
        Создать ордера

        :param orders: Ордера
        """
        try:
            tasks = [self.exchange.create_order(**order) for order in orders]
            await asyncio.gather(*tasks)

        except Exception as e:
            logging.exception(e)

    async def cancel_orders(self, orders: [list[dict]]) -> None:
        """
        Отменить ордера

        :param orders: Ордера
        """
        try:
            orders = [{key: order[key] for key in ["id", "symbol"]} for order in orders]
            tasks = [self.exchange.cancel_order(**order) for order in orders]
            await asyncio.gather(*tasks)

        except Exception as e:
            logging.exception(e)

    async def cancel_all_orders(self) -> None:
        """
        Отменить все ордера
        """
        try:
            orders = await self.exchange.fetch_open_orders()
            await self.cancel_orders(orders)

        except Exception as e:
            logging.exception(e)

    async def fetch_balance(self, parts: Optional[list[str]]) -> None:
        """
        Получить баланс по активам

        :param parts: Активы
        """
        try:
            balance = await self.exchange.fetch_balance()

            if parts is not None:
                balance = {part: balance[part] for part in parts}

            self.core.offer(balance, "balances")

        except Exception as e:
            logging.exception(e)

    async def order_status(self, order) -> None:
        """
        Получить статус ордера
        """
        order_status = await self.exchange.fetch_order_status(**order)
        self.core.offer(order_status, "order_status")

    async def poll(self) -> None:
        """
        Проверять наличие новых сообщений от торгового ядра
        """
        while True:
            self.core.poll()
            await asyncio.sleep(0.1)

    async def watch_order_books(self) -> None:
        """
        Получать биржевые стаканы и отправлять их торговому ядру
        """
        symbols = [market["common_symbol"] for market in self.config["data"]["markets"]]
        tasks = [self.watch_order_book(symbol) for symbol in symbols]
        await asyncio.gather(*tasks)

    async def watch_order_book(self, symbol) -> None:
        """
        Получать биржевой стакан и отправлять его торговому ядру

        :param symbol: Актив
        """
        logging.info("Watching order book: %s", symbol)

        while True:
            try:
                orderbook = await self.exchange.watch_order_book(symbol, 10)
                self.core.offer(orderbook, "orderbook")

            except Exception as e:
                logging.exception(e)

    async def watch_balance(self) -> None:
        """
        Получать баланс и отправлять его торговому ядру
        """
        logging.info("Watching balance")

        while True:
            try:
                balance = await self.exchange.watch_balance()
                self.core.offer(balance, "balances")

            except Exception as e:
                logging.exception(e)

    async def watch_orders(self) -> None:
        """
        Получать ордера и отправлять их торговому ядру
        """
        logging.info("Watching orders")

        while True:
            try:
                orders = await self.exchange.watch_orders()

                for order in orders:
                    match order["status"]:
                        case "open":
                            action = "order_created"
                        case "closed":
                            action = "order_closed"
                        case _:
                            action = "order_status"

                    self.core.offer(order, action)

            except Exception as e:
                logging.exception(e)
