"""
Реализация обмена сообщениями с торговым ядром
"""
from configparser import ConfigParser
from typing import Callable
from aeron import Subscriber, Publisher

# Сигнатура функции обратного вызова для подписки Aeron
AeronMessageHandler = Callable[[str], None]


class Formatter:
    """
    Класс для форматирования сообщений, отправляемых торговому ядру
    """

    pass


class Core:
    """
    Класс для коммуникации с торговым ядром через каналы Aeron
    """

    def __init__(self, config: ConfigParser, handler: AeronMessageHandler):
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
