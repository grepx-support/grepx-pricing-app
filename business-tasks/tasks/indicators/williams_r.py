"""Williams %R indicator."""
from typing import List, Dict, Any
from tasks.indicators.base import Indicator


class WilliamsR(Indicator):
    """Williams %R indicator."""
    
    def __init__(self, period: int = 14):
        """
        Initialize Williams %R indicator.
        
        Args:
            period: Number of periods for Williams %R calculation
        """
        super().__init__(f"WILLIAMS_R_{period}")
        self.period = period
    
    def calculate(self, price_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate Williams %R.
        
        Williams %R = (Highest High - Close) / (Highest High - Lowest Low) * -100
        
        Args:
            price_data: List of price records
        
        Returns:
            List of indicator records
        """
        if len(price_data) < self.period:
            return []
        
        close_prices = self._get_close_prices(price_data)
        high_prices = self._get_high_prices(price_data)
        low_prices = self._get_low_prices(price_data)
        
        results = []
        
        for i in range(self.period - 1, len(price_data)):
            window_high = max(high_prices[i - self.period + 1:i + 1])
            window_low = min(low_prices[i - self.period + 1:i + 1])
            
            if window_high == window_low:
                williams_r = -50.0
            else:
                williams_r = ((window_high - close_prices[i]) / (window_high - window_low)) * -100
            
            results.append({
                'ticker': price_data[i]['ticker'],
                'date': price_data[i]['date'],
                'name': self.name,
                'value': round(williams_r, 2),
                'metadata': {'period': self.period}
            })
        
        return results

