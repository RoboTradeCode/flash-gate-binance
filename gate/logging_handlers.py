from logging import Handler, LogRecord
from aeron import Publisher


class AeronHandler(Handler):
    def __init__(self, channel: str, stream_id: int):
        super().__init__()
        self.publication = Publisher(channel, stream_id)

    def emit(self, record: LogRecord) -> None:
        self.publication.offer(str(record))
