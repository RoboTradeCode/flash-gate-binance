import asyncio
from asyncio import get_running_loop
from typing import Type
import ccxtpro
from ccxtpro import Exchange as BaseExchange
import itertools
from .custom_types import Order, PendingOrder


class Exchange:
    def __init__(
        self,
        exchange_id: str,
        symbols: list[str],
        sandbox_mode: bool = False,
        enable_rate_limit: bool = True,
        api_key: str = None,
        secret_key: str = None,
    ):
        # TODO: Refactor this
        exchange_class: Type[BaseExchange] = getattr(ccxtpro, exchange_id)
        exchange_config = {
            "apiKey": api_key,
            "secret": secret_key,
            "asyncio_loop": get_running_loop(),
            "enableRateLimit": enable_rate_limit,
        }

        self.exchange_id = exchange_id
        self.exchange = exchange_class(exchange_config)
        self.exchange.set_sandbox_mode(sandbox_mode)
        self.exchange.check_required_credentials()

        self.symbols = symbols

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def get_order_book(self, symbols: list[str], limit: int):
        if self.exchange.has.get("watchOrderBook"):
            order_book = await self.exchange.watch_order_book(symbols, limit)
        elif self.exchange.has.get("fetchOrderBook"):
            order_book = await self.exchange.fetch_order_book(symbols, limit)
        else:
            raise NotImplementedError

        return order_book

    async def get_balance(self):
        if self.exchange.has.get("watchBalance"):
            balance = await self.exchange.watch_balance()
        elif self.exchange.has.get("fetchBalance"):
            balance = await self.exchange.fetch_balance()
        else:
            raise NotImplementedError

        return balance

    async def get_order(self, order: Order):
        return await self.exchange.fetch_order(order["id"], order["symbol"])

    async def get_orders(self):
        if self.exchange.has.get("watchOrders"):
            orders = await self.exchange.watch_orders()
        elif self.exchange.has.get("fetchOpenOrders"):
            tasks = [self.exchange.fetch_open_orders(symbol) for symbol in self.symbols]
            results = await asyncio.gather(*tasks)
            return list(itertools.chain.from_iterable(results))
        else:
            raise NotImplementedError

        return orders

    async def create_order(self, order: PendingOrder):
        return await self.exchange.create_order(
            order["symbol"],
            order["type"],
            order["side"],
            order["amount"],
            order["price"],
        )

    async def create_orders(self, orders: list[PendingOrder]):
        tasks = [self.create_order(order) for order in orders]
        return await asyncio.gather(*tasks)

    async def cancel_order(self, order):
        return await self.exchange.cancel_order(order["id"], order["symbol"])

    async def cancel_orders(self, orders):
        tasks = [self.cancel_order(order) for order in orders]
        return await asyncio.gather(*tasks)

    async def cancel_all_orders(self):
        orders = await self.get_orders()
        return await self.cancel_orders(orders)

    async def close(self):
        await self.exchange.close()
