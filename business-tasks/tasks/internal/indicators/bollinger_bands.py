"""Bollinger Bands indicator."""
from typing import List, Dict, Any
from .base import Indicator
import math


class BollingerBands(Indicator):
    """Bollinger Bands indicator."""

    def __init__(self, period: int = 20, std_dev: float = 2.0):
        """
        Initialize Bollinger Bands indicator.

        Args:
            period: Number of periods for moving average
            std_dev: Number of standard deviations for bands
        """
        super().__init__(f"BB_{period}_{std_dev}")
        self.period = period
        self.std_dev = std_dev

    def calculate(self, price_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate Bollinger Bands (Middle, Upper, Lower).

        Args:
            price_data: List of price records

        Returns:
            List of indicator records
        """
        if len(price_data) < self.period:
            return []

        close_prices = self._get_close_prices(price_data)
        results = []

        for i in range(self.period - 1, len(price_data)):
            window = close_prices[i - self.period + 1:i + 1]

            middle = sum(window) / self.period

            variance = sum((x - middle) ** 2 for x in window) / self.period
            std = math.sqrt(variance)

            upper = middle + (self.std_dev * std)
            lower = middle - (self.std_dev * std)

            results.append({
                'ticker': price_data[i]['ticker'],
                'date': price_data[i]['date'],
                'name': self.name,
                'value': round(middle, 4),
                'metadata': {
                    'middle': round(middle, 4),
                    'upper': round(upper, 4),
                    'lower': round(lower, 4),
                    'period': self.period,
                    'std_dev': self.std_dev
                }
            })

        return results


# Celery task wrapper
def calculate_bollinger_bands(period: int = 20, std_dev: float = 2.0, **kwargs) -> Dict[str, Any]:
    """Calculate Bollinger Bands indicator - Celery task entry point."""
    indicator = BollingerBands(period=period, std_dev=std_dev)
    return {"status": "success", "indicator": "BollingerBands", "period": period, "std_dev": std_dev}
