import asyncio
import logging.config
from configparser import ConfigParser
import aiohttp
from gate import Gate

# Названия файлов с конфигурацией
LOGGING_CONFIG_FNAME: str = "logging.conf"
CONFIG_FILENAME: str = "config.ini"


async def main():
    # Инициализация логирования
    logging.config.fileConfig(LOGGING_CONFIG_FNAME)

    # Получение начальной конфигурации
    config = ConfigParser()
    config.read(CONFIG_FILENAME)

    # Получение конфигурации от конфигуратора
    async with aiohttp.ClientSession() as session:
        base_url = config.get("configurator", "base_url")
        exchange = config.get("gate", "exchange")
        instance = config.get("gate", "instance")
        url = f"{base_url}/{exchange}/{instance}"

        params = {"only_new": "false"}
        async with session.get(url, params=params) as response:
            config = await response.json()

    # Запуск торгового гейта
    async with Gate(config) as gate:
        tasks = [
            gate.poll(),
            gate.watch_order_books(),
            gate.watch_balance(),
            gate.watch_orders(),
            gate.ping(),
        ]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
