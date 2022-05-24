import asyncio
from asyncio import get_running_loop
from typing import Type
import ccxtpro
from ccxtpro import okx


class Exchange:
    def __init__(self, exchange_id: str, sandbox_mode: bool = False, **kwargs):
        exchange_class: Type[okx] = getattr(ccxtpro, exchange_id)
        exchange_config = {**kwargs, "asyncio_loop": get_running_loop()}

        self.exchange = exchange_class(exchange_config)
        self.exchange.set_sandbox_mode(sandbox_mode)
        self.exchange.check_required_credentials()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def create_orders(self, orders: list[dict[str, str | float]]):
        # [
        #     {
        #         "symbol": "BTC/USDT",
        #         "type": "limit",
        #         "side": "sell",
        #         "price": 41500.34,
        #         "amount": 0.023
        #     },
        #     {
        #         "symbol": "ETH/USDT",
        #         "type": "limit",
        #         "side": "sell",
        #         "price": 41500.34,
        #         "amount": 0.023
        #     }
        # ]
        tasks = [self.exchange.create_order(**order) for order in orders]
        await asyncio.gather(*tasks)

    async def cancel_orders(self, orders: list[dict[str, str]]):
        # [
        #     {
        #         "id": "86579506507056097",
        #         "symbol": "BTC/USDT"
        #     },
        #     {
        #         "id": "86579506507056044",
        #         "symbol": "ETH/USDT"
        #     }
        # ]
        tasks = [self.exchange.cancel_order(**order) for order in orders]
        await asyncio.gather(*tasks)

    async def cancel_all_orders(self):
        orders = await self.exchange.fetch_open_orders()
        orders = [{arg: order[arg] for arg in ["id", "symbol"]} for order in orders]
        await self.cancel_orders(orders)

    async def fetch_order(self, order: dict[str, str]):
        # {
        #     "id": "86579506507056097",
        #     "symbol": "BTC/USDT"
        # }
        return await self.exchange.fetch_order(**order)

    async def fetch_partial_balances(self, parts: list[str]):
        # [
        #     "BTC",
        #     "ETH",
        #     "USDT"
        # ]
        balance = await self.exchange.fetch_balance()
        return {part: balance[part] for part in parts}

    async def watch_order_book(self, symbols, limit):
        return await self.exchange.watch_order_book(symbols, limit)

    async def watch_balance(self):
        return await self.exchange.watch_balance()

    async def watch_orders(self):
        return await self.exchange.watch_orders()

    async def close(self):
        await self.exchange.close()
