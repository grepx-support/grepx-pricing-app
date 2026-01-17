"""Exponential Moving Average (EMA) indicator."""
from typing import List, Dict, Any
from .base import Indicator


class EMA(Indicator):
    """Exponential Moving Average indicator."""

    def __init__(self, period: int = 20):
        """
        Initialize EMA indicator.

        Args:
            period: Number of periods for moving average
        """
        super().__init__(f"EMA_{period}")
        self.period = period
        self.multiplier = 2 / (period + 1)

    def calculate(self, price_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate Exponential Moving Average.

        Args:
            price_data: List of price records

        Returns:
            List of indicator records
        """
        if len(price_data) < self.period:
            return []

        close_prices = self._get_close_prices(price_data)
        results = []

        ema = sum(close_prices[:self.period]) / self.period

        for i in range(self.period - 1, len(price_data)):
            if i == self.period - 1:
                pass
            else:
                ema = (close_prices[i] - ema) * self.multiplier + ema

            results.append({
                'ticker': price_data[i]['ticker'],
                'date': price_data[i]['date'],
                'name': self.name,
                'value': round(ema, 4),
                'metadata': {'period': self.period}
            })

        return results


# Celery task wrapper
def calculate_ema(period: int = 20, **kwargs) -> Dict[str, Any]:
    """Calculate EMA indicator - Celery task entry point."""
    indicator = EMA(period=period)
    return {"status": "success", "indicator": "EMA", "period": period}
