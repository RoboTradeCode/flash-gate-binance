import asyncio
from dataclasses import dataclass
from queue import Queue
from time import monotonic, sleep
from aiohttp import ClientSession, TCPConnector
from .exchanges import CcxtExchange


@dataclass
class AcquiredExchange:
    exchange: CcxtExchange
    last_acquire: float
    delay: float

    @property
    def remaining(self):
        now = monotonic()
        return self.last_acquire + self.delay - now


class ExchangePool:
    # Ephemeral port
    _LOCAL_PORT = 0

    def __init__(self, exchange_id: str, config: dict, local_hosts: list[str], delay):
        self._exchange_id = exchange_id
        self._config = config | {"session": None}  # CCXT does not own session

        self._queue: Queue[AcquiredExchange] = Queue()
        for exchange in self._create_exchanges(local_hosts):
            self._queue.put(AcquiredExchange(exchange, monotonic(), delay))

    def _create_exchanges(self, local_hosts: list[str]) -> list[CcxtExchange]:
        exchanges = [self._create_exchange(local_host) for local_host in local_hosts]
        return exchanges

    def _create_exchange(self, local_host: str) -> CcxtExchange:
        connector = TCPConnector(local_addr=(local_host, self._LOCAL_PORT))
        session = ClientSession(connector=connector)
        exchange = CcxtExchange(self._exchange_id, self._config)
        exchange.exchange.session = session
        return exchange

    async def acquire(self) -> CcxtExchange:
        acquired_exchange = self._queue.get()
        if (remaining := acquired_exchange.remaining) > 0:
            await asyncio.sleep(remaining)

        now = monotonic()
        acquired_exchange.last_acquire = now
        self._queue.put(acquired_exchange)

        return acquired_exchange.exchange

    async def close(self):
        while not self._queue.empty():
            acquired_exchange = self._queue.get()
            session = acquired_exchange.exchange.exchange.session
            await session.close()


class PrivateExchangePool:
    def __init__(self, exchange_id: str, config: dict, accounts: list[dict], delay=0):
        """
        Пул exchange с приватным соединением. Создает подключения с помощью переданных ключей.
        """
        self._exchange_id = exchange_id
        self._config = config

        self._queue: Queue[AcquiredExchange] = Queue()
        for exchange in self._create_exchanges(accounts):
            self._queue.put(AcquiredExchange(exchange, monotonic(), delay))

    def _create_exchanges(self, accounts: list[dict]) -> list[CcxtExchange]:
        """
        Создать подключения к бирже
        """
        exchanges = [self._create_exchange(keys) for keys in accounts]
        return exchanges

    def _create_exchange(self, keys: dict) -> CcxtExchange:
        """
        Подключиться к бирже
        :param keys: словарь с ключами api_key, secret_key
        """
        config = self._config | keys
        exchange = CcxtExchange(self._exchange_id, config)
        return exchange

    async def acquire(self) -> CcxtExchange:
        """
        Получить очередной экземпляр exchange
        """
        acquired_exchange = self._queue.get()
        self._queue.put(acquired_exchange)
        return acquired_exchange.exchange
