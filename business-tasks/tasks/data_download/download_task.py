"""Data download task for storing price data in database."""
from typing import Dict, Any, Optional
import logging
from tasks.data_download.yahoo_finance_downloader import download_ticker_data
from database.factory import get_database
from config.task_config import DEFAULT_DB_CONFIG

logger = logging.getLogger(__name__)


def download_and_store(
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    period: str = "1y",
    db_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Download ticker data from Yahoo Finance and store in database.
    
    Args:
        ticker: Stock ticker symbol
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        period: Period to download (default: 1y)
        db_config: Database configuration
    
    Returns:
        Result dictionary with ticker and record count
    """
    if db_config is None:
        db_config = DEFAULT_DB_CONFIG
    
    try:
        logger.info(f"Downloading data for {ticker}")
        records = download_ticker_data(ticker, start_date, end_date, period)
        
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

