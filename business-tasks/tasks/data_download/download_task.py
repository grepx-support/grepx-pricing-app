"""Data download task for storing price data in database."""
from typing import Dict, Any, Optional
import logging
from tasks.providers import ProviderFactory
from database.factory import get_database
from config.task_config import DEFAULT_DB_CONFIG

logger = logging.getLogger(__name__)


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
        db_config = DEFAULT_DB_CONFIG
    
    if provider_name is None or period is None:
        from business_tasks.config_loader import load_data_download_config
        config = load_data_download_config()
        if provider_name is None:
            provider_name = config.provider.provider_type
        if period is None:
            period = config.default_period
    
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

