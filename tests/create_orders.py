from aeron import Publisher
import json
import uuid


event = {
    "event_id": "0bf733e3-da9b-4516-8288-c34c8d838d31",
    "event": "command",
    "exchange": "exmo",
    "node": "core",
    "instance": "test",
    "algo": "multi_3t_php",
    "action": "create_orders",
    "message": None,
    "timestamp": 1502962946216000,
}


def main():
    for i in range(10):
        _event = event
        _event["event_id"] = str(uuid.uuid4())
        _event["data"] = [
            {
                "symbol": "BTC/USDT",
                "type": "limit",
                "side": "sell",
                "amount": 0.0001,
                "price": 100000,
                "client_order_id": str(i),
            }
        ]
        message = json.dumps(_event)
        publisher = Publisher("aeron:ipc", 1004)
        publisher.offer(message)
        publisher.close()


if __name__ == "__main__":
    main()
