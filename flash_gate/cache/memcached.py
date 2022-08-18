from pymemcache.client.base import Client
from pymemcache.serde import pickle_serde


class Memcached:
    def __init__(self, key_prefix: str = ""):
        self.client = Client(
            "localhost", pickle_serde, default_noreply=False, key_prefix=key_prefix
        )

    def set(self, key, value) -> None:
        self.client.set(key, value)

    def get(self, key: str):
        value = self.client.get(key)
        return value
