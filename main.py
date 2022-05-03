import asyncio
import logging
from configparser import ConfigParser
from src.gates import OKXGate

CONFIG_FILENAME: str = "config.ini"


async def main():
    # Включение логирования
    logging.basicConfig(level=logging.INFO)

    # Чтение конфигурации
    logging.info("Reading configuration...")
    config = ConfigParser()
    config.read(CONFIG_FILENAME)
    logging.info("Configuration successfully read!", {"config": config})

    # Запуск гейта
    async with OKXGate(config) as gate:
        tasks = [gate.aeron_poll(), gate.watch_order_book()]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
