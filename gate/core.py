"""
Реализация обмена сообщениями с торговым ядром
"""
from configparser import ConfigParser
from typing import Callable
from aeron import Subscriber, Publisher
from .formatter import Formatter


class Core:
    """
    Класс для коммуникации с торговым ядром через каналы Aeron
    """

    def __init__(self, config: ConfigParser, handler: Callable[[str], None]):
        # Создание объекта для форматирования отправляемых сообщений
        self.formatter = Formatter(config)

        # Создание подписки Aeron для получения команд
        self.commands = Subscriber(
            handler,
            config.get("core", "commands_channel"),
            config.getint("core", "commands_stream_id"),
            config.getint("core", "commands_fragment_limit"),
        )

        # Создание публикации для отправки биржевых стаканов
        self.order_book = Publisher(
            config.get("core", "order_book_channel"),
            config.getint("core", "order_book_stream_id"),
        )

        # Создание публикации для отправки баланса
        self.balance = Publisher(
            config.get("core", "balance_channel"),
            config.getint("core", "balance_stream_id"),
        )

        # Создание публикации для отправки ордеров
        self.orders = Publisher(
            config.get("core", "orders_channel"),
            config.getint("core", "orders_stream_id"),
        )

    def poll(self) -> None:
        """
        Проверить наличие новых сообщений
        """
        self.commands.poll()

    def offer_order_book(self, order_book: dict) -> None:
        """
        Отправить биржевой стакан
        """
        message = self.formatter.format_order_book(order_book)
        self.order_book.offer(message)

    def offer_balance(self, balance: dict):
        """
        Отправить баланс
        """
        message = self.formatter.format_balance(balance)
        self.balance.offer(message)

    def offer_orders(self, orders):
        """
        Отправить ордера
        """
        message = self.formatter.format_orders(orders)
        self.orders.offer(message)
