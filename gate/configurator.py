"""
Реализация универсального получения конфигурации
"""
import logging
from configparser import ConfigParser
from requests import Session


class Configurator:
    """
    Класс для универсального получения конфигурации
    """

    def __init__(self, config: ConfigParser):
        # Сохранение конфигурации по-умолчанию
        self.config = config

        # Проверка использования внешнего конфигуратора
        self.is_external = config.getboolean("configurator", "enabled")
        logging.info("External configurator enabled: %s", self.is_external)

        # Создание сессии для получения конфигурации от внешнего конфигуратора
        if self.is_external:
            self._session = Session()
            self._url = config.get("configurator", "url")

    def get_configuration(self) -> ConfigParser:
        """
        Получить конфигурацию

        При включённом параметре configurator.enabled конфигурация будет получена от
        внешнего конфигуратора по адресу configurator.url

        :return: Актуальная конфигурация
        """
        # Получение конфигурации от внешнего конфигуратора
        if self.is_external:
            logging.info("Receiving configuration: %s", self._url)
            configuration = self._session.get(self._url).text
            self.config.read_string(configuration)
            logging.info("Configuration receive has been successful")

        return self.config
