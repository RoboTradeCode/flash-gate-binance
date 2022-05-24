from datetime import datetime


class Formatter:
    def __init__(self, config: dict):
        self.exchange = config["data"]["configs"]["gate_config"]["info"]["exchange"]
        self.instance = config["data"]["configs"]["gate_config"]["info"]["instance"]
        self.node = config["data"]["configs"]["gate_config"]["info"]["node"]
        self.algo = config["algo"]
        self.assets = [asset["common"] for asset in config["data"]["assets_labels"]]

    def base(self):
        return {
            "event": "data",
            "exchange": self.exchange,
            "node": self.node,
            "instance": self.instance,
            "message": None,
            "algo": self.algo,
            "timestamp": int(datetime.now().timestamp()),
        }

    def format(self, data, action: str, symbol: str = None):
        message = self.base()
        message["action"] = action
        match action:
            case "orderbook":
                keys = ["bids", "asks", "symbol", "timestamp"]
                message["data"] = {key: data[key] for key in data if key in keys}
                if message["data"]["symbol"] is None:
                    message["data"]["symbol"] = symbol
            case "balances":
                message["data"] = data
            case "order_created" | "order_closed" | "order_status":
                keys = [
                    "id",
                    "timestamp",
                    "status",
                    "symbol",
                    "type",
                    "side",
                    "price",
                    "amount",
                    "filled",
                ]
                message["data"] = {key: data[key] for key in data if key in keys}
            case "ping":
                message["data"] = data

        return message
