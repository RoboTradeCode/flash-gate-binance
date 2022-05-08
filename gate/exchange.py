"""
Набор функций для работы с классом биржы
"""
import logging
from asyncio import get_running_loop
from configparser import ConfigParser
import ccxtpro
from ccxtpro import Exchange


def instantiate_exchange(config: ConfigParser) -> Exchange:
    """
    Создать экземпляр класса биржы на основе переданной конфигурации и проверить
    обязательные учётные данные

    :param config: Конфигурация гейта
    :return: Экземпляр класса биржы
    """
    # Получение названия биржы
    exchange_id = config.get("gate", "exchange_id")

    # Получение класса биржы и конфигурации
    logging.info("Instantiating exchange: %s", exchange_id)
    exchange_class = getattr(ccxtpro, exchange_id)
    exchange_config = {**config["exchange"], "asyncio_loop": get_running_loop()}

    # Создание экземпляра класса биржы
    exchange: Exchange = exchange_class(exchange_config)

    # Проверка тестового режима
    sandbox_mode = config.getboolean("gate", "sandbox_mode")
    exchange.set_sandbox_mode(sandbox_mode)

    # Проверка обязательных учётных данных
    check_credentials(exchange)
    logging.info("Exchange instantiation has been successful")

    return exchange


def check_credentials(exchange: Exchange) -> None:
    """
    Проверить обязательные учётные данные для экземпляра класса биржы

    :param exchange: Экземпляр класса биржы
    """
    required_credentials = exchange.requiredCredentials

    logging.info("Checking required credentials: %s", required_credentials)
    exchange.check_required_credentials()
    logging.info("Required credentials check has been successful")
