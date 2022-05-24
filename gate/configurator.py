from aiohttp import ClientSession
import logging


class Configurator:
    def __init__(self, base_url: str, exchange: str, instance: str):
        self.session = ClientSession(base_url)
        self.exchange = exchange
        self.instance = instance

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    @property
    def method(self):
        return f"/{self.exchange}/{self.instance}"

    async def get_config(self, only_new: bool = False) -> dict:
        params = {"only_new": "true" if only_new else "false"}
        async with self.session.get(self.method, params=params) as response:
            logging.info("Configurator: %s", await response.text())
            return await response.json()

    async def close(self):
        await self.session.close()
