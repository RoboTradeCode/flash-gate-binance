"""
Обработчики логирования
"""
from logging import Handler, LogRecord
from aeron import Publisher


class AeronHandler(Handler):
    """
    Обработчик для отправки логов в канал Aeron
    """

    def __init__(self, channel: str, stream_id: int):
        super().__init__()

        # Создание подписки Aeron для отправки логов
        self.publication = Publisher(channel, stream_id)

    def emit(self, record: LogRecord) -> None:
        self.publication.offer(str(record))
