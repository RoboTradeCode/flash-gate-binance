"""
Реализация торгового гейта
"""
import asyncio
import logging
from configparser import ConfigParser
from .configurator import Configurator
from .core import Core
from .exchange import instantiate_exchange
import json


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
        message = json.loads(message)

        match message:
            case {"action": "create_order"}:
                self.creating_orders(message["data"])
            case {"action": "cancel_order"}:
                self.cancel_orders(message["data"])
            case _:
                logging.warning("Unknown message type: %s", message)

    def creating_orders(self, orders: list[dict]) -> None:
        """
        Создать ордера
        :param orders: Ордера
        """
        tasks = [self.exchange.create_order(**order) for order in orders]
        asyncio.gather(*tasks)

    def cancel_orders(self, orders: list[dict]) -> None:
        """
        Отменить ордера
        :param orders:
        """
        tasks = [self.exchange.cancel_order(**order) for order in orders]
        asyncio.gather(*tasks)

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
