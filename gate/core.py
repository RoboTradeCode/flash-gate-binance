"""
Реализация обмена сообщениями с торговым ядром
"""
import logging
from typing import Any, Callable
from aeron import Subscriber, Publisher, AeronPublicationNotConnectedError
from .formatter import Formatter
from .logging_handlers import AeronHandler


class Core:
    """
    Класс для коммуникации с торговым ядром через каналы Aeron
    """

    def __init__(self, config: dict, handler: Callable[[str], None]):
        config = config["data"]["configs"]["gate_config"]

        # Создание объекта для форматирования отправляемых сообщений
        self.formatter = Formatter(config)

        # Создание каналов Aeron
        self.commands = Subscriber(handler, **config["aeron"]["subscribers"]["core"])
        self.order_book = Publisher(**config["aeron"]["publishers"]["orderbooks"])
        self.balance = Publisher(**config["aeron"]["publishers"]["balances"])
        self.orders = Publisher(**config["aeron"]["publishers"]["orders_statuses"])

        # Создание логгера
        logging_handler = AeronHandler(**config["aeron"]["publishers"]["logs"])
        self.logger = logging.getLogger("aeron")
        self.logger.addHandler(logging_handler)

    def close(self) -> None:
        """
        Закрыть соединение
        """
        self.commands.close()
        self.order_book.close()
        self.balance.close()
        self.orders.close()

    def poll(self) -> None:
        """
        Проверить наличие новых сообщений
        """
        self.commands.poll()

    def offer(self, data: Any, action: str) -> None:
        """
        Отправить ответ на команду ядра

        :param data:   Ответ от биржы
        :param action: Действие
        """
        message = self.formatter.format(data, action)

        try:
            match action:
                case "orderbook":
                    self.order_book.offer(message)
                case "balances":
                    self.balance.offer(message)
                case "order_status" | "order_created" | "order_cancelled":
                    self.orders.offer(message)
                case "ping":
                    self.logger.info(message)

        except AeronPublicationNotConnectedError:
            pass
