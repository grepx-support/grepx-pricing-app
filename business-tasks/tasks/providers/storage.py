"""Storage utilities for providers - save data to database."""
from typing import Dict, Any, Optional
import logging
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from database.factory import get_database
from .provider_factory import ProviderFactory

logger = logging.getLogger(__name__)


def _get_default_db_config() -> Dict[str, Any]:
    """Get default database configuration."""
    return {
        'type': 'mongodb',
        'connection_string': 'mongodb://admin:password123@localhost:27017/stock_analysis?authSource=admin',
        'database_name': 'stock_analysis'
    }


def download_and_store(
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    period: Optional[str] = None,
    db_config: Optional[Dict[str, Any]] = None,
    provider_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Download ticker data using the provider factory and store in database.
    
    Args:
        ticker: Stock ticker symbol
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        period: Period to download (uses config default if not provided)
        db_config: Database configuration
        provider_name: Name of the data provider (uses config default if not provided)
    
    Returns:
        Result dictionary with ticker and record count
    """
    if db_config is None:
        db_config = _get_default_db_config()
    
    if provider_name is None or period is None:
        from .config_loader import load_task_config
        config = load_task_config('data_download_default')
        if config:
            if provider_name is None:
                provider_name = config.get('provider_type', 'yahoo')
            if period is None:
                period = config.get('metadata', {}).get('default_period', '1y')
        else:
            provider_name = provider_name or 'yahoo'
            period = period or '1y'
    
    try:
        logger.info(f"Downloading data for {ticker} using {provider_name} provider")
        provider = ProviderFactory.get_provider(provider_name)
        
        # Download data using provider
        df = provider.download_historical_data(ticker, start_date, end_date)
        
        if df is None or df.empty:
            return {'ticker': ticker, 'count': 0, 'status': 'no_data'}
        
        # Convert DataFrame to records format
        df = df.reset_index()
        records = []
        for _, row in df.iterrows():
            record = {
                'ticker': ticker,
                'date': row['Date'].strftime('%Y-%m-%d') if hasattr(row['Date'], 'strftime') else str(row['Date'])[:10],
                'open': float(row['Open']) if 'Open' in row else None,
                'high': float(row['High']) if 'High' in row else None,
                'low': float(row['Low']) if 'Low' in row else None,
                'close': float(row['Close']) if 'Close' in row else None,
                'volume': int(row['Volume']) if 'Volume' in row else None,
            }
            records.append(record)
        
        if not records:
            return {'ticker': ticker, 'count': 0, 'status': 'no_data'}
        
        logger.info(f"Storing {len(records)} records for {ticker}")
        db = get_database(db_config)
        db.connect()
        
        try:
            db.insert_many('price_data', records)
        finally:
            db.disconnect()
        
        return {
            'ticker': ticker,
            'count': len(records),
            'status': 'success',
            'start_date': records[0]['date'],
            'end_date': records[-1]['date']
        }
    
    except Exception as e:
        logger.error(f"Error in download_and_store for {ticker}: {e}")
        return {
            'ticker': ticker,
            'count': 0,
            'status': 'error',
            'error': str(e)
        }


def download_and_store_multiple(
    tickers: list,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    period: Optional[str] = None,
    db_config: Optional[Dict[str, Any]] = None,
    provider_name: Optional[str] = None
) -> Dict[str, Dict[str, Any]]:
    """
    Download and store data for multiple tickers.
    
    Args:
        tickers: List of stock ticker symbols
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        period: Period to download (uses config default if not provided)
        db_config: Database configuration
        provider_name: Name of the data provider (uses config default if not provided)
    
    Returns:
        Dictionary mapping ticker to result dictionary
    """
    results = {}
    
    for ticker in tickers:
        result = download_and_store(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
            period=period,
            db_config=db_config,
            provider_name=provider_name
        )
        results[ticker] = result
    
    return results

