from typing import TypedDict, Any
from .enums import EventType, EventNode, EventAction


class Event(TypedDict, total=False):
    event_id: str
    event: EventType
    exchange: str
    node: EventNode
    instance: str
    algo: str
    action: EventAction
    message: str
    timestamp: int
    data: Any
