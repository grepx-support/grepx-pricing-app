"""Average Directional Index (ADX) indicator."""
from typing import List, Dict, Any
from tasks.indicators.base import Indicator


class ADX(Indicator):
    """Average Directional Index indicator."""
    
    def __init__(self, period: int = 14):
        """
        Initialize ADX indicator.
        
        Args:
            period: Number of periods for ADX calculation
        """
        super().__init__(f"ADX_{period}")
        self.period = period
    
    def calculate(self, price_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate ADX, +DI, and -DI.
        
        Args:
            price_data: List of price records
        
        Returns:
            List of indicator records
        """
        if len(price_data) < self.period * 2 + 1:
            return []
        
        high_prices = self._get_high_prices(price_data)
        low_prices = self._get_low_prices(price_data)
        close_prices = self._get_close_prices(price_data)
        
        tr_list = []
        plus_dm = []
        minus_dm = []
        
        for i in range(1, len(price_data)):
            high = high_prices[i]
            low = low_prices[i]
            prev_high = high_prices[i-1]
            prev_low = low_prices[i-1]
            prev_close = close_prices[i-1]
            
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_list.append(tr)
            
            up_move = high - prev_high
            down_move = prev_low - low
            
            if up_move > down_move and up_move > 0:
                plus_dm.append(up_move)
            else:
                plus_dm.append(0)
            
            if down_move > up_move and down_move > 0:
                minus_dm.append(down_move)
            else:
                minus_dm.append(0)
        
        atr = sum(tr_list[:self.period]) / self.period
        smooth_plus_dm = sum(plus_dm[:self.period]) / self.period
        smooth_minus_dm = sum(minus_dm[:self.period]) / self.period
        
        results = []
        
        for i in range(self.period, len(tr_list)):
            atr = (atr * (self.period - 1) + tr_list[i]) / self.period
            smooth_plus_dm = (smooth_plus_dm * (self.period - 1) + plus_dm[i]) / self.period
            smooth_minus_dm = (smooth_minus_dm * (self.period - 1) + minus_dm[i]) / self.period
            
            plus_di = 100 * (smooth_plus_dm / atr) if atr > 0 else 0
            minus_di = 100 * (smooth_minus_dm / atr) if atr > 0 else 0
            
            di_sum = plus_di + minus_di
            dx = 100 * abs(plus_di - minus_di) / di_sum if di_sum > 0 else 0
            
            if i == self.period:
                adx = dx
            else:
                adx = (adx * (self.period - 1) + dx) / self.period
            
            results.append({
                'ticker': price_data[i + 1]['ticker'],
                'date': price_data[i + 1]['date'],
                'name': self.name,
                'value': round(adx, 2),
                'metadata': {
                    'adx': round(adx, 2),
                    'plus_di': round(plus_di, 2),
                    'minus_di': round(minus_di, 2),
                    'period': self.period
                }
            })
        
        return results

