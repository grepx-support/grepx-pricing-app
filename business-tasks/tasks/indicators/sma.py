"""Simple Moving Average (SMA) indicator."""
from typing import List, Dict, Any
from tasks.indicators.base import Indicator


class SMA(Indicator):
    """Simple Moving Average indicator."""
    
    def __init__(self, period: int = 20):
        """
        Initialize SMA indicator.
        
        Args:
            period: Number of periods for moving average
        """
        super().__init__(f"SMA_{period}")
        self.period = period
    
    def calculate(self, price_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate Simple Moving Average.
        
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
            sma_value = sum(window) / self.period
            
            results.append({
                'ticker': price_data[i]['ticker'],
                'date': price_data[i]['date'],
                'name': self.name,
                'value': round(sma_value, 4),
                'metadata': {'period': self.period}
            })
        
        return results

