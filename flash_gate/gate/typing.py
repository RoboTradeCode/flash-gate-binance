from typing import TypedDict

LatencyPercentile = TypedDict(
    "LatencyPercentile",
    {
        "50": float,
        "90": float,
        "99": float,
        "99.99": "float",
    },
)


class OrderbookMetrics(TypedDict):
    latency_percentile: LatencyPercentile
    rps: int


class PublicApiMetrics(TypedDict):
    orderbook: OrderbookMetrics


class PrivateApiMetrics(TypedDict):
    total_rps: int


class Metrics(TypedDict):
    public_api: PublicApiMetrics
    private_api: PrivateApiMetrics
