from logging import Handler, LogRecord
from aeron import (
    Publisher,
    AeronPublicationNotConnectedError,
    AeronPublicationAdminActionError,
)


class AeronHandler(Handler):
    def __init__(self, channel: str, stream_id: int):
        super().__init__()
        self.publication = Publisher(channel, stream_id)

    def emit(self, record: LogRecord):
        successful = False

        while not successful:
            try:
                self.publication.offer(record.msg)
                successful = True

            except AeronPublicationNotConnectedError:
                successful = True
            except AeronPublicationAdminActionError:
                pass
