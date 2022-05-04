import asyncio
import logging
from configparser import ConfigParser
from .helpers import subscription, publication, exchange


class OKXGate:
    def __init__(self, config: ConfigParser):
        self.config = config

        logging.info("Creating sub for config, Cfg: %s", {**config["config"]})
        self.config_sub = subscription(self.config_handler, config["config"])

        logging.info("Creating sub for commands, Cfg: %s", {**config["commands"]})
        self.commands_sub = subscription(self.commands_handler, config["commands"])

        logging.info("Creating pub for orderbooks, Cfg: %s", {**config["orderbooks"]})
        self.orderbooks_pub = publication(config["orderbooks"])

        logging.info("Creating pub for balance, Cfg: %s", {**config["balance"]})
        self.balance_pub = publication(config["balance"])

        logging.info("Creating pub for orders, Cfg: %s", {**config["orders"]})
        self.orders_pub = publication(config["orders"])

        logging.info("Instantiating okx exchange, Cfg: %s", {**config["exchange"]})
        self.exchange = exchange(config["exchange"])
        self.exchange.check_required_credentials()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.exchange.close()

    def config_handler(self, message: str) -> None:
        """
        Функция обратного вызова для приёма конфигурации от агента

        :param message: Конфигурация от агента
        """
        logging.debug("Handle config, Cfg: %s", message)
        self.config.read_string(message)

    def commands_handler(self, message: str) -> None:
        """
        Функция обратного вызова для приёма команд от ядра

        :param message: Команда от ядра
        """
        logging.debug("Handle command, Cmd: %s", message)

    async def aeron_poll(self) -> None:
        while True:
            self.config_sub.poll()
            self.commands_sub.poll()
            await asyncio.sleep(0)

    async def watch_order_book(self) -> None:
        while True:
            kwargs = {
                "symbol": self.config.get("watch_order_book", "symbol"),
                "limit": self.config.getint("watch_order_book", "limit"),
                "params": {
                    "depth": self.config.get("watch_order_book", "depth"),
                },
            }
            orderbook = await self.exchange.watch_order_book(**kwargs)

            logging.debug("Sending orderbook to core, Orderbook: %s", orderbook)
            self.orderbooks_pub.offer(str(orderbook))
