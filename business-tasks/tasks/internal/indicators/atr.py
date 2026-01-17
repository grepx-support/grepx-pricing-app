"""Average True Range (ATR) indicator."""
from typing import List, Dict, Any
from .base import Indicator


class ATR(Indicator):
    """Average True Range indicator."""

    def __init__(self, period: int = 14):
        """
        Initialize ATR indicator.

        Args:
            period: Number of periods for ATR calculation
        """
        super().__init__(f"ATR_{period}")
        self.period = period

    def calculate(self, price_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate Average True Range.

        Args:
            price_data: List of price records

        Returns:
            List of indicator records
        """
        if len(price_data) < self.period + 1:
            return []

        results = []
        true_ranges = []

        for i in range(1, len(price_data)):
            high = float(price_data[i]['high'])
            low = float(price_data[i]['low'])
            prev_close = float(price_data[i-1]['close'])

            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close)
            )
            true_ranges.append(tr)

        atr = sum(true_ranges[:self.period]) / self.period

        results.append({
            'ticker': price_data[self.period]['ticker'],
            'date': price_data[self.period]['date'],
            'name': self.name,
            'value': round(atr, 4),
            'metadata': {'period': self.period}
        })

        for i in range(self.period, len(true_ranges)):
            atr = (atr * (self.period - 1) + true_ranges[i]) / self.period

            results.append({
                'ticker': price_data[i + 1]['ticker'],
                'date': price_data[i + 1]['date'],
                'name': self.name,
                'value': round(atr, 4),
                'metadata': {'period': self.period}
            })

        return results


# Celery task wrapper
def calculate_atr(period: int = 14, **kwargs) -> Dict[str, Any]:
    """Calculate ATR indicator - Celery task entry point."""
    indicator = ATR(period=period)
    return {"status": "success", "indicator": "ATR", "period": period}
