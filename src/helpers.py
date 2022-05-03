from configparser import SectionProxy
from typing import Callable
from aeron import Subscriber, Publisher
from ccxtpro import okx
import asyncio

AeronHandler = Callable[[str], None]


def subscription(handler: AeronHandler, config: SectionProxy) -> Subscriber:
    return Subscriber(
        handler,
        config.get("channel", "aeron:udp?control-mode=manual"),
        config.getint("stream_id", 1001),
        config.getint("fragment_limit", 10),
    )


def publication(config: SectionProxy) -> Publisher:
    return Publisher(
        config.get("channel", "aeron:udp?control-mode=manual"),
        config.getint("stream_id", 1001),
    )


def exchange(config: SectionProxy) -> okx:
    return okx(
        {
            "asyncio_loop": asyncio.get_running_loop(),
            "apiKey": config["apiKey"],
            "secret": config["secret"],
            "password": config["password"],
        }
    )
