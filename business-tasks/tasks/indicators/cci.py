"""Commodity Channel Index (CCI) indicator."""
from typing import List, Dict, Any
from tasks.indicators.base import Indicator


class CCI(Indicator):
    """Commodity Channel Index indicator."""
    
    def __init__(self, period: int = 20):
        """
        Initialize CCI indicator.
        
        Args:
            period: Number of periods for CCI calculation
        """
        super().__init__(f"CCI_{period}")
        self.period = period
        self.constant = 0.015
    
    def calculate(self, price_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate Commodity Channel Index.
        
        Args:
            price_data: List of price records
        
        Returns:
            List of indicator records
        """
        if len(price_data) < self.period:
            return []
        
        typical_prices = []
        for record in price_data:
            tp = (float(record['high']) + float(record['low']) + float(record['close'])) / 3
            typical_prices.append(tp)
        
        results = []
        
        for i in range(self.period - 1, len(price_data)):
            window = typical_prices[i - self.period + 1:i + 1]
            
            sma = sum(window) / self.period
            
            mean_deviation = sum(abs(tp - sma) for tp in window) / self.period
            
            if mean_deviation == 0:
                cci = 0
            else:
                cci = (typical_prices[i] - sma) / (self.constant * mean_deviation)
            
            results.append({
                'ticker': price_data[i]['ticker'],
                'date': price_data[i]['date'],
                'name': self.name,
                'value': round(cci, 2),
                'metadata': {'period': self.period}
            })
        
        return results

