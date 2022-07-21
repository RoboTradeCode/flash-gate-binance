from aeron import Publisher
import json


event = "ping!"


def main():
    message = json.dumps(event)
    publisher = Publisher("aeron:ipc", 1004)
    publisher.offer(message)
    publisher.close()


if __name__ == "__main__":
    main()
