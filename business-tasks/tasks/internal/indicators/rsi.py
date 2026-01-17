"""Relative Strength Index (RSI) indicator."""
from typing import List, Dict, Any
from .base import Indicator


class RSI(Indicator):
    """Relative Strength Index indicator."""

    def __init__(self, period: int = 14):
        """
        Initialize RSI indicator.

        Args:
            period: Number of periods for RSI calculation
        """
        super().__init__(f"RSI_{period}")
        self.period = period

    def calculate(self, price_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate Relative Strength Index.

        Args:
            price_data: List of price records

        Returns:
            List of indicator records
        """
        if len(price_data) < self.period + 1:
            return []

        close_prices = self._get_close_prices(price_data)
        results = []

        changes = [close_prices[i] - close_prices[i-1] for i in range(1, len(close_prices))]

        gains = [max(change, 0) for change in changes]
        losses = [abs(min(change, 0)) for change in changes]

        avg_gain = sum(gains[:self.period]) / self.period
        avg_loss = sum(losses[:self.period]) / self.period

        for i in range(self.period, len(changes)):
            avg_gain = (avg_gain * (self.period - 1) + gains[i]) / self.period
            avg_loss = (avg_loss * (self.period - 1) + losses[i]) / self.period

            if avg_loss == 0:
                rsi = 100
            else:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))

            results.append({
                'ticker': price_data[i + 1]['ticker'],
                'date': price_data[i + 1]['date'],
                'name': self.name,
                'value': round(rsi, 2),
                'metadata': {'period': self.period}
            })

        return results


# Celery task wrapper
def calculate_rsi(period: int = 14, **kwargs) -> Dict[str, Any]:
    """Calculate RSI indicator - Celery task entry point."""
    indicator = RSI(period=period)
    return {"status": "success", "indicator": "RSI", "period": period}
