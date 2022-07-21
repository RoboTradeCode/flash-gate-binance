import json
from datetime import datetime
from .types import Event
from .enums import EventType


class JsonFormatter:
    def __init__(self, config: dict):
        gate_config = config["data"]["configs"]["gate_config"]

        self.algo = config["algo"]
        self.node = gate_config["info"]["node"]
        self.instance = gate_config["info"]["instance"]
        self.exchange = gate_config["exchange"]["exchange_id"]

    def format(self, event: Event) -> str:
        template = self._get_template()
        filled = self._fill_template(template, event)
        return self._serialize(filled)

    def _get_template(self) -> dict:
        return {
            "event_id": None,
            "event": EventType.DATA,
            "exchange": self.exchange,
            "node": self.node,
            "instance": self.instance,
            "algo": self.algo,
            "action": None,
            "message": None,
            "timestamp": self._get_timestamp_in_us(),
            "data": None,
        }

    @staticmethod
    def _get_timestamp_in_us() -> int:
        return int(datetime.now().timestamp() * 1_000_000)

    @staticmethod
    def _fill_template(template: dict, data: dict) -> dict:
        filled = template.copy()
        for key, value in data.items():
            filled[key] = value
        return filled

    @staticmethod
    def _serialize(message: dict) -> str:
        return json.dumps(message)
