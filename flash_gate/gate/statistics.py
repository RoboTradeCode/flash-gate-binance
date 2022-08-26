import statistics
from decimal import Decimal
from .typing import LatencyPercentile


def ns_to_us(ns: int) -> int:
    """
    Преобразовать наносекунды в микросекунды
    """
    us = int(ns / 1_000)
    return us


def latency_percentile(data: list) -> LatencyPercentile:
    """
    Получить 50, 90, 99 и 99.99 процентили из переданного списка
    """
    quantiles = statistics.quantiles(data, n=10000, method="inclusive")
    percentiles = {
        "50": int(percentile(quantiles, "50")),
        "90": int(percentile(quantiles, "90")),
        "99": int(percentile(quantiles, "99")),
        "99.99": int(percentile(quantiles, "99.99")),
    }
    return percentiles


def percentile(quantiles: list, n: str) -> float:
    """
    Получить n-й процентиль из квантилей
    """
    return quantiles[int(len(quantiles) * (Decimal(n) / 100) - 1)]
