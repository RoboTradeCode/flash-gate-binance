from aeron import Publisher
import json


event = {
    "event_id": "9978009a-eb10-11ec-8fea-0242ac120002",
    "event": "command",
    "exchange": "kuna",
    "node": "core",
    "instance": "test",
    "algo": "multi_3t_php",
    "action": "get_balance",
    "message": None,
    "timestamp": 1502962946216000,
    "data": [],
}


def main():
    message = json.dumps(event)
    publisher = Publisher("aeron:ipc", 1004)
    publisher.offer(message)
    publisher.close()


if __name__ == "__main__":
    main()
