import statistics
from flash_gate.gate.statistics import percentile
import pytest

DATA = [1, 2, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 7, 7, 8, 8, 10, 10]


class TestPercentile:
    quantiles = statistics.quantiles(DATA, n=10000, method="inclusive")

    def test_empty_quantiles_raises_exception(self):
        with pytest.raises(Exception):
            percentile([], "0")

    def test_fifty(self):
        fifty_percentile = percentile(self.quantiles, "50")
        assert round(fifty_percentile, 2) == 5.5

    def test_ninety(self):
        ninety_percentile = percentile(self.quantiles, "90")
        assert round(ninety_percentile, 2) == 8.2

    def test_ninety_nine(self):
        ninety_nine_percentile = percentile(self.quantiles, "99")
        assert round(ninety_nine_percentile, 2) == 10

    def test_ninety_nine_and_ninety_nine(self):
        ninety_nine_and_ninety_nine_percentile = percentile(self.quantiles, "99.99")
        assert round(ninety_nine_and_ninety_nine_percentile, 2) == 10

    def test_hundred(self):
        hundred_percentile = percentile(self.quantiles, "100")
        assert round(hundred_percentile, 2) == 10
