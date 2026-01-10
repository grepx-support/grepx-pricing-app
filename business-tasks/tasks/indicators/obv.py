"""On-Balance Volume (OBV) indicator."""
from typing import List, Dict, Any
from tasks.indicators.base import Indicator


class OBV(Indicator):
    """On-Balance Volume indicator."""
    
    def __init__(self):
        """Initialize OBV indicator."""
        super().__init__("OBV")
    
    def calculate(self, price_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate On-Balance Volume.
        
        Args:
            price_data: List of price records
        
        Returns:
            List of indicator records
        """
        if len(price_data) < 2:
            return []
        
        close_prices = self._get_close_prices(price_data)
        volumes = self._get_volumes(price_data)
        
        results = []
        obv = 0
        
        results.append({
            'ticker': price_data[0]['ticker'],
            'date': price_data[0]['date'],
            'name': self.name,
            'value': obv,
            'metadata': {}
        })
        
        for i in range(1, len(price_data)):
            if close_prices[i] > close_prices[i-1]:
                obv += volumes[i]
            elif close_prices[i] < close_prices[i-1]:
                obv -= volumes[i]
            
            results.append({
                'ticker': price_data[i]['ticker'],
                'date': price_data[i]['date'],
                'name': self.name,
                'value': obv,
                'metadata': {}
            })
        
        return results

