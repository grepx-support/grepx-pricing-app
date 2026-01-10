"""Data service for fetching and processing financial data using providers."""
from typing import Dict, List, Optional, Any
import pandas as pd
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'shared', 'grepx-shared-models', 'src'))

from tasks.providers import ProviderFactory
from grepx_models import ProviderConfig

logger = logging.getLogger(__name__)


class DataService:
    """Service class for managing data retrieval using various providers."""
    
    def __init__(self, provider_config: Optional[ProviderConfig] = None, **provider_kwargs):
        """
        Initialize the data service with a specific provider.
        
        Args:
            provider_config: ProviderConfig instance (loads from DB if not provided)
            **provider_kwargs: Additional arguments for the provider
        """
        if provider_config is None:
            from business_tasks.config_loader import load_provider_config
            provider_config = load_provider_config()
        
        self.provider_config = provider_config
        self.provider_name = provider_config.provider_type
        
        # Merge extra_params with provider_kwargs
        merged_kwargs = {**provider_config.extra_params, **provider_kwargs}
        self.provider = ProviderFactory.get_provider(self.provider_name, **merged_kwargs)
        logger.info(f"DataService initialized with {self.provider_name} provider")
    
    def get_historical_data(
        self,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        interval: str = "1d"
    ) -> pd.DataFrame:
        """
        Get historical data for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            interval: Data interval (1d, 1h, etc.)
        
        Returns:
            DataFrame with historical data
        """
        try:
            logger.info(f"Fetching historical data for {ticker}")
            data = self.provider.download_historical_data(
                ticker, start_date, end_date, interval
            )
            return data
        except Exception as e:
            logger.error(f"Error fetching historical data for {ticker}: {e}")
            raise
    
    async def get_historical_data_async(
        self,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        interval: str = "1d"
    ) -> pd.DataFrame:
        """
        Asynchronously get historical data for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            interval: Data interval (1d, 1h, etc.)
        
        Returns:
            DataFrame with historical data
        """
        try:
            logger.info(f"Async fetching historical data for {ticker}")
            data = await self.provider.download_historical_data_async(
                ticker, start_date, end_date, interval
            )
            return data
        except Exception as e:
            logger.error(f"Error async fetching historical data for {ticker}: {e}")
            raise
    
    def get_close_prices(
        self,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        interval: str = "1d"
    ) -> pd.Series:
        """
        Get close prices for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            interval: Data interval (1d, 1h, etc.)
        
        Returns:
            Series with close prices
        """
        try:
            logger.info(f"Fetching close prices for {ticker}")
            prices = self.provider.download_close_prices(
                ticker, start_date, end_date, interval
            )
            return prices
        except Exception as e:
            logger.error(f"Error fetching close prices for {ticker}: {e}")
            raise
    
    def get_multiple_symbols(
        self,
        symbols: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        interval: str = "1d"
    ) -> Dict[str, pd.DataFrame]:
        """
        Get historical data for multiple symbols.
        
        Args:
            symbols: List of stock ticker symbols
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            interval: Data interval (1d, 1h, etc.)
        
        Returns:
            Dictionary mapping ticker to DataFrame
        """
        try:
            logger.info(f"Fetching data for {len(symbols)} symbols")
            data = self.provider.download_multiple_symbols(
                symbols, start_date, end_date, interval
            )
            return data
        except Exception as e:
            logger.error(f"Error fetching multiple symbols: {e}")
            raise
    
    async def get_multiple_symbols_async(
        self,
        symbols: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        interval: str = "1d"
    ) -> Dict[str, pd.DataFrame]:
        """
        Asynchronously get historical data for multiple symbols.
        
        Args:
            symbols: List of stock ticker symbols
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            interval: Data interval (1d, 1h, etc.)
        
        Returns:
            Dictionary mapping ticker to DataFrame
        """
        try:
            logger.info(f"Async fetching data for {len(symbols)} symbols")
            data = await self.provider.download_multiple_symbols_async(
                symbols, start_date, end_date, interval
            )
            return data
        except Exception as e:
            logger.error(f"Error async fetching multiple symbols: {e}")
            raise
    
    def calculate_returns(self, data: pd.DataFrame) -> pd.Series:
        """
        Calculate returns from price data.
        
        Args:
            data: DataFrame with price data (must have 'Close' column)
        
        Returns:
            Series with returns
        """
        if 'Close' not in data.columns:
            raise ValueError("DataFrame must have 'Close' column")
        
        returns = data['Close'].pct_change()
        return returns
    
    def calculate_volatility(self, data: pd.DataFrame, window: int = 20) -> pd.Series:
        """
        Calculate rolling volatility from price data.
        
        Args:
            data: DataFrame with price data (must have 'Close' column)
            window: Rolling window size
        
        Returns:
            Series with volatility
        """
        returns = self.calculate_returns(data)
        volatility = returns.rolling(window=window).std()
        return volatility
    
    def get_summary_statistics(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Get summary statistics for price data.
        
        Args:
            data: DataFrame with price data
        
        Returns:
            Dictionary with summary statistics
        """
        if data is None or data.empty:
            return {}
        
        stats = {
            'count': len(data),
            'start_date': str(data.index[0]) if len(data) > 0 else None,
            'end_date': str(data.index[-1]) if len(data) > 0 else None,
        }
        
        if 'Close' in data.columns:
            stats.update({
                'close_mean': float(data['Close'].mean()),
                'close_std': float(data['Close'].std()),
                'close_min': float(data['Close'].min()),
                'close_max': float(data['Close'].max()),
            })
        
        if 'Volume' in data.columns:
            stats.update({
                'volume_mean': float(data['Volume'].mean()),
                'volume_total': int(data['Volume'].sum()),
            })
        
        return stats

