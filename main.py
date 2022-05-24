import asyncio
import logging.config
from configparser import ConfigParser
from gate import Gate, Configurator

LOGGING_FNAME = "logging.conf"
CONFIG_FILENAME = "config.ini"


async def main():
    logging.config.fileConfig(LOGGING_FNAME)
    initial_config = ConfigParser()
    initial_config.read(CONFIG_FILENAME)

    base_url = initial_config.get("configurator", "base_url")
    exchange = initial_config.get("configurator", "exchange")
    instance = initial_config.get("configurator", "instance")
    sandbox_mode = initial_config.getboolean("gate", "sandbox_mode")

    async with Configurator(base_url, exchange, instance) as configurator:
        config = await configurator.get_config()

    async with Gate(config, sandbox_mode) as gate:
        await gate.run()


if __name__ == "__main__":
    asyncio.run(main())
