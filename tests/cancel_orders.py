from aeron import Publisher
import json


event = {
    "event_id": "0bf733e3-da9b-4516-8288-c34c8d838d29",
    "event": "command",
    "exchange": "kuna",
    "node": "core",
    "instance": "test",
    "algo": "multi_3t_php",
    "action": "cancel_orders",
    "message": None,
    "timestamp": 1502962946216000,
    "data": [
        {
            "symbol": "BTC/USDT",
            "client_order_id": "0fa56216-fb3e-11ec-b939-0242ac120002",
        },
    ],
}


def main():
    message = json.dumps(event)
    publisher = Publisher("aeron:ipc", 1004)
    publisher.offer(message)
    publisher.close()


if __name__ == "__main__":
    main()
