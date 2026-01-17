"""Base class for technical indicators."""
from abc import ABC, abstractmethod
from typing import List, Dict, Any


class Indicator(ABC):
    """Abstract base class for technical indicators."""
    
    def __init__(self, name: str):
        """
        Initialize indicator.
        
        Args:
            name: Indicator name
        """
        self.name = name
    
    @abstractmethod
    def calculate(self, price_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate indicator values.
        
        Args:
            price_data: List of price records with keys:
                - ticker, date, open, high, low, close, volume
        
        Returns:
            List of indicator records with keys:
                - ticker, date, name, value, metadata
        """
        pass
    
    def _get_close_prices(self, price_data: List[Dict[str, Any]]) -> List[float]:
        """Extract closing prices from price data."""
        return [float(record['close']) for record in price_data]
    
    def _get_high_prices(self, price_data: List[Dict[str, Any]]) -> List[float]:
        """Extract high prices from price data."""
        return [float(record['high']) for record in price_data]
    
    def _get_low_prices(self, price_data: List[Dict[str, Any]]) -> List[float]:
        """Extract low prices from price data."""
        return [float(record['low']) for record in price_data]
    
    def _get_volumes(self, price_data: List[Dict[str, Any]]) -> List[int]:
        """Extract volumes from price data."""
        return [int(record['volume']) for record in price_data]

