import asyncio
import logging.config
from configparser import ConfigParser
from gate import Gate

# Названия файлов с конфигурацией
LOGGING_CONFIG_FNAME: str = "logging.conf"
CONFIG_FILENAME: str = "config.ini"


async def main():
    # Инициализация логирования
    logging.config.fileConfig(LOGGING_CONFIG_FNAME)

    # Получение конфигурации по-умолчанию
    config = ConfigParser()
    config.optionxform = str
    config.read(CONFIG_FILENAME)

    # Запуск торгового гейта
    async with Gate(config) as gate:
        tasks = [
            gate.poll(),
            gate.watch_order_books(),
            gate.watch_balance(),
            gate.watch_orders(),
        ]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
