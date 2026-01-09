"""Stochastic Oscillator indicator."""
from typing import List, Dict, Any
from tasks.indicators.base import Indicator


class Stochastic(Indicator):
    """Stochastic Oscillator indicator."""
    
    def __init__(self, period: int = 14, smooth_k: int = 3, smooth_d: int = 3):
        """
        Initialize Stochastic indicator.
        
        Args:
            period: Number of periods for %K calculation
            smooth_k: Smoothing period for %K
            smooth_d: Smoothing period for %D (signal line)
        """
        super().__init__(f"STOCH_{period}_{smooth_k}_{smooth_d}")
        self.period = period
        self.smooth_k = smooth_k
        self.smooth_d = smooth_d
    
    def calculate(self, price_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate Stochastic Oscillator (%K and %D).
        
        Args:
            price_data: List of price records
        
        Returns:
            List of indicator records
        """
        if len(price_data) < self.period + self.smooth_k + self.smooth_d:
            return []
        
        close_prices = self._get_close_prices(price_data)
        high_prices = self._get_high_prices(price_data)
        low_prices = self._get_low_prices(price_data)
        
        raw_k = []
        for i in range(self.period - 1, len(price_data)):
            window_high = max(high_prices[i - self.period + 1:i + 1])
            window_low = min(low_prices[i - self.period + 1:i + 1])
            
            if window_high == window_low:
                k = 50.0
            else:
                k = 100 * (close_prices[i] - window_low) / (window_high - window_low)
            raw_k.append(k)
        
        smooth_k_values = []
        for i in range(self.smooth_k - 1, len(raw_k)):
            k_window = raw_k[i - self.smooth_k + 1:i + 1]
            smooth_k_values.append(sum(k_window) / self.smooth_k)
        
        results = []
        for i in range(self.smooth_d - 1, len(smooth_k_values)):
            d_window = smooth_k_values[i - self.smooth_d + 1:i + 1]
            d_value = sum(d_window) / self.smooth_d
            
            data_idx = self.period + self.smooth_k + i - 1
            
            results.append({
                'ticker': price_data[data_idx]['ticker'],
                'date': price_data[data_idx]['date'],
                'name': self.name,
                'value': round(smooth_k_values[i], 2),
                'metadata': {
                    'k': round(smooth_k_values[i], 2),
                    'd': round(d_value, 2),
                    'period': self.period,
                    'smooth_k': self.smooth_k,
                    'smooth_d': self.smooth_d
                }
            })
        
        return results

