import asyncio
import logging.config
from configparser import ConfigParser
from gate import OKXGate

LOGGING_CONFIG_FNAME: str = "logging.conf"
CONFIG_FILENAME: str = "config.ini"


async def main():
    # Инициализация логирования
    logging.config.fileConfig(LOGGING_CONFIG_FNAME)

    # Чтение конфигурации гейта
    config = ConfigParser()
    config.read(CONFIG_FILENAME)

    # Запуск гейта
    async with OKXGate(config) as gate:
        tasks = [gate.aeron_poll(), gate.watch_order_book()]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
