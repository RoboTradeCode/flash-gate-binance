"""
Набор функций для работы с классом биржы
"""
import logging
from asyncio import get_running_loop
import ccxtpro
from ccxtpro import okx


def instantiate_exchange(config: dict) -> okx:
    """
    Создать экземпляр класса биржы на основе переданной конфигурации и проверить
    обязательные учётные данные

    :param config: Конфигурация гейта
    :return: Экземпляр класса биржы
    """
    config = config["data"]["configs"]["gate_config"]

    # Получение названия биржы
    exchange_id = config["info"]["exchange"]

    # Получение класса биржы
    logging.info("Instantiating exchange: %s", exchange_id)
    exchange_class = getattr(ccxtpro, exchange_id)
    exchange_config = {
        **config["account"],
        "asyncio_loop": get_running_loop(),
    }

    # Создание экземпляра класса биржы
    exchange: okx = exchange_class(exchange_config)

    # Установка тестового режима
    exchange.set_sandbox_mode(config["sandbox_mode"])

    # Проверка обязательных учётных данных
    check_credentials(exchange)
    logging.info("Exchange instantiation has been successful")

    return exchange


def check_credentials(exchange: okx) -> None:
    """
    Проверить обязательные учётные данные для экземпляра класса биржы

    :param exchange: Экземпляр класса биржы
    """
    required_credentials = exchange.requiredCredentials

    logging.info("Checking required credentials: %s", required_credentials)
    exchange.check_required_credentials()
    logging.info("Required credentials check has been successful")
