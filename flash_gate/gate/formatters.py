from uuid import uuid4
from flash_gate.transmitter.enums import EventAction
from .typing import LatencyPercentile, Metrics


class EventFormatter:
    @staticmethod
    def metrics(data: Metrics) -> dict:
        return {
            "event_id": str(uuid4()),
            "action": EventAction.METRICS,
            "data": data,
        }

    @staticmethod
    def metrics_data(
        orderbook_latency_percentile: LatencyPercentile,
        orderbook_rps: int,
        private_api_total_rps: int,
    ) -> Metrics:
        return {
            "public_api": {
                "orderbook": {
                    "latency_percentile": orderbook_latency_percentile,
                    "rps": orderbook_rps,
                }
            },
            "private_api": {
                "total_rps": private_api_total_rps,
            },
        }
