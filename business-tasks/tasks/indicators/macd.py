"""Moving Average Convergence Divergence (MACD) indicator."""
from typing import List, Dict, Any
from tasks.indicators.base import Indicator


class MACD(Indicator):
    """Moving Average Convergence Divergence indicator."""
    
    def __init__(self, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9):
        """
        Initialize MACD indicator.
        
        Args:
            fast_period: Fast EMA period
            slow_period: Slow EMA period
            signal_period: Signal line EMA period
        """
        super().__init__(f"MACD_{fast_period}_{slow_period}_{signal_period}")
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period
    
    def _calculate_ema(self, prices: List[float], period: int) -> List[float]:
        """Calculate EMA for given prices."""
        if len(prices) < period:
            return []
        
        multiplier = 2 / (period + 1)
        ema_values = []
        
        ema = sum(prices[:period]) / period
        ema_values.append(ema)
        
        for i in range(period, len(prices)):
            ema = (prices[i] - ema) * multiplier + ema
            ema_values.append(ema)
        
        return ema_values
    
    def calculate(self, price_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate MACD, Signal, and Histogram.
        
        Args:
            price_data: List of price records
        
        Returns:
            List of indicator records
        """
        if len(price_data) < self.slow_period + self.signal_period:
            return []
        
        close_prices = self._get_close_prices(price_data)
        
        fast_ema = self._calculate_ema(close_prices, self.fast_period)
        slow_ema = self._calculate_ema(close_prices, self.slow_period)
        
        offset = self.slow_period - self.fast_period
        fast_ema = fast_ema[offset:]
        
        macd_line = [fast_ema[i] - slow_ema[i] for i in range(len(slow_ema))]
        
        signal_line = self._calculate_ema(macd_line, self.signal_period)
        
        results = []
        start_idx = self.slow_period + self.signal_period - 1
        
        for i in range(len(signal_line)):
            data_idx = start_idx + i
            macd_idx = self.signal_period - 1 + i
            
            histogram = macd_line[macd_idx] - signal_line[i]
            
            results.append({
                'ticker': price_data[data_idx]['ticker'],
                'date': price_data[data_idx]['date'],
                'name': self.name,
                'value': round(macd_line[macd_idx], 4),
                'metadata': {
                    'macd': round(macd_line[macd_idx], 4),
                    'signal': round(signal_line[i], 4),
                    'histogram': round(histogram, 4),
                    'fast_period': self.fast_period,
                    'slow_period': self.slow_period,
                    'signal_period': self.signal_period
                }
            })
        
        return results

