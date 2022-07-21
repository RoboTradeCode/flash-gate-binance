import asyncio
from ccxtpro import exmo
from pprint import pprint
import logging
from flash_gate.exchange import CcxtExchange

ORDER = {
    "client_order_id": "1",
    "symbol": "BTC/USDT",
    "type": "limit",
    "side": "sell",
    "amount": 0.0001,
    "price": 100000,
}


async def main():
    logging.basicConfig(level=logging.DEBUG)

    api_key = "K-5c48ca01887ddf50ea7094e021b1f37c37ced971"
    secret = "S-127383e2a3cc853a0be497520de08029d1016b9f"

    exchange = CcxtExchange("exmo", {"apiKey": api_key, "secret": secret})

    try:
        await exchange.cancel_all_orders(["BTC/USDT"])

        orders = await exchange.create_orders([ORDER])
        order = orders[0]
        pprint(order)

        await exchange.cancel_orders(
            [{"client_order_id": order["client_order_id"], "symbol": "BTC/USDT"}]
        )

        order = await exchange.fetch_order(
            {"client_order_id": order["client_order_id"], "symbol": "BTC/USDT"}
        )
        pprint(order)
    finally:
        await exchange.close()

    # async with exmo({"apiKey": api_key, "secret": secret}) as exchange:
    # order = await exchange.create_order("BTC/USDT", "market", "sell", 0.0001)
    # pprint(order)

    # order = await exchange.fetch_open_orders()
    # pprint(order)
    #
    # order = await exchange.fetch_order(28895197431)
    # pprint(order)


if __name__ == "__main__":
    asyncio.run(main())
