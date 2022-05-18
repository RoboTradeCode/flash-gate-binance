"""
Форматирование сообщений
"""
import json
from datetime import datetime
from typing import Any


class Formatter:
    """
    Класс для форматирования сообщений, отправляемых торговому ядру
    """

    def __init__(self, config: dict):
        self.exchange = config["info"]["exchange"]
        self.instance = config["info"]["instance"]
        self.node = config["info"]["node"]

    def base(self):
        return {
            "event": "data",
            "exchange": self.exchange,
            "node": self.node,
            "instance": self.instance,
            "message": None,
            "algo": "spread_bot_cpp",
            "timestamp": int(datetime.now().timestamp()),
        }

    def format(self, data: Any, action: str):
        """
        Привести биржевой стакан к общему формату обмена данными

        :param data: Ответ от биржы
        :param action: Действие
        :return: Отформатированное сообщение
        """

        message = self.base()
        message["action"] = action
        message["data"] = data

        if action == "ping":
            message["event"] = "info"

        return json.dumps(message)
