"""
Форматирование сообщений для передачи другим микросервисам
"""
import json
from configparser import ConfigParser
from datetime import datetime


class Formatter:
    """
    Класс для форматирования сообщений, отправляемых торговому ядру
    """

    def __init__(self, config: ConfigParser):
        # Сохранение конфигурации
        self.config = config

    def format_order_book(self, order_book):
        return json.dumps(
            {
                "event": "data",
                "exchange": self.config.get("gate", "exchange_id"),
                "node": "gate",
                "instance": self.config.get("gate", "instance"),
                "action": "orderbook",
                "message": None,
                "algo": "signal",
                "timestamp": int(datetime.now().timestamp()),
                "data": order_book,
            }
        )

    def format_balance(self, balance):
        return json.dumps(
            {
                "event": "data",
                "exchange": self.config.get("gate", "exchange_id"),
                "node": "gate",
                "instance": self.config.get("gate", "instance"),
                "action": "balances",
                "message": None,
                "algo": "3t_php",
                "timestamp": int(datetime.now().timestamp()),
                "data": balance,
            }
        )

    def format_orders(self, orders):
        return json.dumps(
            {
                "event": "data",
                "exchange": self.config.get("gate", "exchange_id"),
                "node": "gate",
                "instance": self.config.get("gate", "instance"),
                "action": "order_created",
                "message": None,
                "algo": "spread_bot_cpp",
                "timestamp": int(datetime.now().timestamp()),
                "data": orders,
            }
        )
