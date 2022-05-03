import asyncio
import logging
from configparser import ConfigParser
from .helpers import subscription, publication, exchange


class OKXGate:
    def __init__(self, config: ConfigParser):
        # Сохранение конфигурации, для последующей инициализации класса биржы
        self.config = config

        # Создание подписки для агента
        logging.info("Creating subscription for agent...", {"config": config["agent"]})
        self.agent_sub = subscription(self.agent_handler, config["agent"])
        logging.info("Subscription successfully created!")

        # Создание публикация для ядра
        logging.info("Creating publication for core...", {"config": config["core"]})
        self.core_pub = publication(config["core"])
        logging.info("Publication successfully created!")

    async def __aenter__(self):
        # Получение конфигурации
        config = self.config

        # Создание класса биржы для последующего подключения и начала торговли
        logging.info("Instantiating exchange class...", {"config": config["exchange"]})
        self.exchange = exchange(config["exchange"])
        self.exchange.check_required_credentials()
        logging.info("Exchange successfully instantiated!")

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.exchange.close()

    def agent_handler(self, message: str) -> None:
        """
        Функция обратного вызова для приёма сообщений от агента

        :param message: Сообщение от агента
        """
        logging.debug("Received message from agent!", {"message": message})

    async def aeron_poll(self) -> None:
        while True:
            self.agent_sub.poll()
            await asyncio.sleep(0)

    async def watch_order_book(self) -> None:
        while True:
            orderbook = await self.exchange.watch_order_book("BTC/USDT", limit=1)

            # Отправка биржевого стакана ядру
            logging.debug("Sending orderbook to core...", {"orderbook": orderbook})
            self.core_pub.offer(str(orderbook))
            logging.debug("Orderbook successfully sent!", {"orderbook": orderbook})
