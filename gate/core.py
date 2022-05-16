"""
Реализация обмена сообщениями с торговым ядром
"""
from typing import Callable
from aeron import Subscriber, Publisher
from .formatter import Formatter


class Core:
    """
    Класс для коммуникации с торговым ядром через каналы Aeron
    """

    def __init__(self, config: dict, handler: Callable[[str], None]):
        config = config["data"]["configs"]["gate_config"]

        # Создание объекта для форматирования отправляемых сообщений
        self.formatter = Formatter(config)

        # Создание каналов Aeron
        self.commands = Subscriber(handler, **config["aeron"]["core"])
        self.order_book = Publisher(**config["aeron"]["orderbooks"])
        self.balance = Publisher(**config["aeron"]["balances"])
        self.orders = Publisher(**config["aeron"]["orders_statuses"])

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

    def offer(self, data: dict, action: str) -> None:
        """
        Отправить ответ на команду ядра

        :param data:   Ответ от биржы
        :param action: Действие
        """
        message = self.formatter.format(data, action)

        match action:
            case "orderbook":
                self.order_book.offer(message)
            case "balances":
                self.balance.offer(message)
            case "order_status" | "order_created" | "order_cancelled":
                self.orders.offer(message)
