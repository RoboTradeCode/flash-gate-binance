import logging
from typing import Callable, NoReturn
import aeron
from aeron import Publisher, Subscriber
from aeron.concurrent import AsyncSleepingIdleStrategy
from .formatters import JsonFormatter
from .types import Event
from .enums import Destination


IDLE_SLEEP_MS = 1


class AeronTransmitter:
    def __init__(self, handler: Callable[[str], None], config: dict):
        aeron_config = config["data"]["configs"]["gate_config"]["aeron"]
        subscribers = aeron_config["subscribers"]
        publishers = aeron_config["publishers"]

        self.logger = logging.getLogger(__name__)
        self.formatter = JsonFormatter(config)
        self.idle_strategy = AsyncSleepingIdleStrategy(IDLE_SLEEP_MS)

        self.subscriber = Subscriber(handler, **subscribers["core"])
        self.order_book = Publisher(**publishers["orderbooks"])
        self.balance = Publisher(**publishers["balances"])
        self.core = Publisher(**publishers["core"])
        self.logs = Publisher(**publishers["logs"])

    async def run(self) -> NoReturn:
        while True:
            await self._poll()

    async def _poll(self):
        fragments_read = self.subscriber.poll()
        await self.idle_strategy.idle(fragments_read)

    def offer(self, event: Event, destination: Destination) -> None:
        try:
            self._offer(event, destination)
        except Exception as e:
            self.logger.error(e)

    def _offer(self, event: Event, destination: Destination):
        publisher = self._get_publisher(destination)
        message = self.formatter.format(event)
        self.logger.debug("Trying to offer message: %s", message)
        self._offer_while_not_successful(publisher, message)

    def _offer_while_not_successful(self, publisher: Publisher, message: str) -> None:
        while True:
            try:
                result = publisher.offer(message)
                self.logger.debug(
                    f"Message has been successfully offered [{result}]: %s", message
                )
                break
            except aeron.AeronPublicationNotConnectedError as e:
                self.logger.debug(e)
                break
            except aeron.AeronPublicationAdminActionError as e:
                self.logger.warning(e)
            except Exception as e:
                self.logger.exception(e)

    def _get_publisher(self, destination) -> Publisher:
        match destination:
            case Destination.ORDER_BOOK:
                return self.order_book
            case Destination.BALANCE:
                return self.balance
            case Destination.CORE:
                return self.core
            case Destination.LOGS:
                return self.logs
            case _:
                raise ValueError(f"Invalid destination: {destination}")

    def close(self):
        self.subscriber.close()
        self.order_book.close()
        self.balance.close()
        self.core.close()
        self.logs.close()
