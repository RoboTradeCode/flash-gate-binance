import json
import logging
from typing import Callable
from aeron import (
    Subscriber,
    Publisher,
    AeronPublicationAdminActionError,
    AeronPublicationNotConnectedError,
)


class Core:
    def __init__(self, aeron_config: dict, handler: Callable[[str], None]):
        subscriber: dict = aeron_config["subscribers"]
        publishers: dict = aeron_config["publishers"]

        self.core = Subscriber(handler, **subscriber["core"])
        self.orderbooks = Publisher(**publishers["orderbooks"])
        self.balances = Publisher(**publishers["balances"])
        self.orders_statuses = Publisher(**publishers["orders_statuses"])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def poll(self):
        return self.core.poll()

    def offer(self, message: dict):
        successful = False

        while not successful:
            try:
                match message.get("action"):
                    case "orderbook":
                        self.orderbooks.offer(json.dumps(message))
                    case "balances":
                        self.balances.offer(json.dumps(message))
                    case "order_status" | "order_created" | "order_cancelled":
                        self.orders_statuses.offer(json.dumps(message))

                successful = True

            except AeronPublicationNotConnectedError:
                successful = True
            except AeronPublicationAdminActionError:
                pass

    def close(self):
        self.core.close()
        self.orderbooks.close()
        self.balances.close()
        self.orders_statuses.close()
