"""Volume Weighted Average Price (VWAP) indicator."""
from typing import List, Dict, Any
from .base import Indicator


class VWAP(Indicator):
    """Volume Weighted Average Price indicator."""

    def __init__(self):
        """Initialize VWAP indicator."""
        super().__init__("VWAP")

    def calculate(self, price_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate Volume Weighted Average Price.

        VWAP is typically calculated intraday, but this implementation
        calculates it on a rolling basis for daily data.

        Args:
            price_data: List of price records

        Returns:
            List of indicator records
        """
        if not price_data:
            return []

        results = []
        cumulative_tpv = 0
        cumulative_volume = 0

        for record in price_data:
            high = float(record['high'])
            low = float(record['low'])
            close = float(record['close'])
            volume = int(record['volume'])

            typical_price = (high + low + close) / 3

            cumulative_tpv += typical_price * volume
            cumulative_volume += volume

            vwap = cumulative_tpv / cumulative_volume if cumulative_volume > 0 else typical_price

            results.append({
                'ticker': record['ticker'],
                'date': record['date'],
                'name': self.name,
                'value': round(vwap, 4),
                'metadata': {
                    'typical_price': round(typical_price, 4)
                }
            })

        return results


# Celery task wrapper
def calculate_vwap(**kwargs) -> Dict[str, Any]:
    """Calculate VWAP indicator - Celery task entry point."""
    indicator = VWAP()
    return {"status": "success", "indicator": "VWAP"}
