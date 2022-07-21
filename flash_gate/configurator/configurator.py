import logging
from .drivers import SourceDriverFactory
from .enums import DriverType
from .drivers import Driver


class Configurator:
    """
    Объект для получения конфигурации
    """

    def __init__(self, driver_type: DriverType, source: str):
        """
        :param driver_type: Тип используемого драйвера
        :param source: Источник для получения конфигурации
        """
        self.logger = logging.getLogger(__name__)
        self.driver_type = driver_type
        self.source = source

    async def get_config(self) -> dict:
        """
        Получить конфигурацию
        """
        self.logger.info("Trying to get config: %s", self.source)
        config = await self._get_config_from_driver()
        self.logger.info("Config has been successfully received: %s", config)
        return config

    async def _get_config_from_driver(self):
        driver = self._get_driver()
        config = await driver.get_config()
        return config

    def _get_driver(self) -> Driver:
        factory = SourceDriverFactory(self.source)
        driver = factory.make_driver(self.driver_type)
        return driver
