"""
Реализация торгового гейта
"""
import asyncio
import json
import logging
from configparser import ConfigParser
from typing import Optional
from .configurator import Configurator
from .core import Core
from .exchange import instantiate_exchange


class Gate:
    """
    Торговый гейт. Подключается к бирже и посылает торговому ядру информацию о биржевых
    стаканах, балансе и ордерах. Выполняет поступающие от торгового ядра команды
    """

    def __init__(self, config: ConfigParser):
        # Получение актуальной конфигурации
        configurator = Configurator(config)
        self.config = configurator.get_configuration()

        # Создание экземпляра класса биржы для подключения и начала торговли
        self.exchange = instantiate_exchange(config)

        # Создание каналов Aeron для ядра
        self.core = Core(config, self.handle_command)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.exchange.close()

    def handle_command(self, message: str) -> None:
        """
        Функция обратного вызова для приёма команд от ядра

        :param message: Сообщение от ядра
        """
        command = json.loads(message)

        match command.get("action"):
            case "create_order":
                self.create_orders(command["data"])
            case "cancel_order":
                self.cancel_orders(command["data"])
            case "get_balances":
                self.fetch_balance(command.get("data", {}).get("assets"))
            case "cancel_all_orders":
                self.cancel_orders()
            case "order_status":
                self.order_status(**command.get("data", {}))
            case _:
                logging.warning("Unknown command: %s", command)

    def create_orders(self, orders: list[dict]) -> None:
        """
        Создать ордера

        :param orders: Ордера
        """
        tasks = [self.exchange.create_order(**order) for order in orders]
        asyncio.gather(*tasks)

    def cancel_orders(self, orders: Optional[list[dict]] = None) -> None:
        """
        Отменить ордера

        :param orders:
        """
        if orders is None:
            orders = self.exchange.fetch_orders()

        tasks = [self.exchange.cancel_order(**order) for order in orders]
        asyncio.gather(*tasks)

    def fetch_balance(self, parts: Optional[list[str]] = None) -> None:
        """
        Получить баланс по активам

        :param parts: Активы
        """
        try:
            balance = await self.exchange.fetch_balance()

            if parts is not None:
                partial_balance = {part: balance[part] for part in parts}
                self.core.offer_balance(partial_balance)

            self.core.offer_balance(balance)

        except Exception as e:
            logging.exception(e)

    def order_status(self, **kwargs) -> None:
        """
        Проверить статус ордера
        """
        order_status = await self.exchange.fetch_order_status(**kwargs)
        # TODO: send to core

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
        symbols = json.loads(self.config.get("gate", "symbols"))
        tasks = [self.watch_order_book(symbol) for symbol in symbols]
        await asyncio.gather(*tasks)

    async def watch_order_book(self, symbol) -> None:
        """
        Получать биржевой стакан и отправлять его торговому ядру

        :param symbol: Актив
        """
        limit = self.config.getint("watch_order_book", "limit", fallback=None)

        logging.info("Watching order book: %s", symbol)

        while True:
            try:
                orderbook = await self.exchange.watch_order_book(symbol, limit)
                self.core.offer_order_book(orderbook)
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
                self.core.offer_balance(balance)
            except Exception as e:
                logging.exception(e)

    async def watch_orders(self) -> None:
        """
        Получать ордера и отправлять их торговому ядру
        """
        symbol = self.config.get("watch_orders", "symbol", fallback=None)
        since = self.config.get("watch_orders", "since", fallback=None)
        limit = self.config.getint("watch_orders", "limit", fallback=None)

        logging.info("Watching orders")

        while True:
            try:
                orders = await self.exchange.watch_orders(symbol, since, limit)
                self.core.offer_orders(orders)
            except Exception as e:
                logging.exception(e)
