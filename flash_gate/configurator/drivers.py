import json
from abc import ABC, abstractmethod
import aiofiles
from aiohttp import ClientSession
from .enums import DriverType


class Driver(ABC):
    """
    Драйвер, позволяющий получить конфигурацию
    """

    @abstractmethod
    async def get_config(self) -> dict:
        """
        Получить конфигурацию
        """
        ...


class DriverFactory(ABC):
    """
    Фабрика для получения конкретной реализации драйвера
    """

    @abstractmethod
    def make_driver(self, driver_type: DriverType) -> Driver:
        """
        Создать экземпляр драйвера

        :param driver_type: Тип используемого драйвера
        """
        ...


class FileDriver(Driver):
    """
    Драйвер, получающий конфигурацию из локального файла
    """

    def __init__(self, source: str):
        self.source = source

    async def get_config(self):
        content = await self._get_content()
        config = self._decode_content(content)
        return config

    async def _get_content(self) -> str:
        async with aiofiles.open(self.source) as f:
            return await f.read()

    @staticmethod
    def _decode_content(content: str) -> dict:
        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            raise ValueError("Invalid config format") from e


class HTTPDriver(Driver):
    """
    Драйвер, получающий конфигурацию по протоколу HTTP
    """

    def __init__(self, source: str):
        self.source = source

    async def get_config(self) -> dict:
        content = await self._get_content()
        config = self._decode_content(content)
        return config

    async def _get_content(self) -> str:
        async with ClientSession() as session:
            async with session.get(self.source) as response:
                return await response.text()

    @staticmethod
    def _decode_content(content: str) -> dict:
        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            raise ValueError("Invalid config format") from e


class SourceDriverFactory(DriverFactory):
    """
    Фабрика для создания драйверов, получающих конфигурацию из переданного источника
    """

    def __init__(self, source: str):
        self.source = source

    def make_driver(self, driver_type: DriverType):
        match driver_type:
            case DriverType.FILE:
                return FileDriver(self.source)
            case DriverType.HTTP:
                return HTTPDriver(self.source)
            case _:
                raise ValueError(f"Invalid driver type: {driver_type}")
