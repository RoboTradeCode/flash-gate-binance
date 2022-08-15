from pymemcache.client.base import Client
from pymemcache.serde import pickle_serde


class Memcached:
    def __init__(self, *args, **kwargs):
        self.client = Client("localhost", pickle_serde, *args, **kwargs)

    def set(self, key, value) -> None:
        self.client.set(key, value)

    def get(self, key: str):
        value = self.client.get(key)
        return value
