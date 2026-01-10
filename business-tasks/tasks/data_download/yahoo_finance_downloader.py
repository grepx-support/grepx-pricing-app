"""Yahoo Finance data downloader."""
import yfinance as yf
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


def download_ticker_data(
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    period: str = "1y"
) -> List[Dict[str, Any]]:
    """
    Download historical price data from Yahoo Finance.
    
    Args:
        ticker: Stock ticker symbol (e.g., 'AAPL', 'MSFT')
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
        period: Period to download if dates not specified
                Valid periods: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max
    
    Returns:
        List of price records as dictionaries
    """
    try:
        logger.info(f"Downloading data for {ticker}")
        
        if start_date and end_date:
            df = yf.download(ticker, start=start_date, end=end_date, progress=False)
        else:
            df = yf.download(ticker, period=period, progress=False)
        
        if df.empty:
            logger.warning(f"No data found for {ticker}")
            return []
        
        df = df.reset_index()
        
        records = []
        for _, row in df.iterrows():
            record = {
                'ticker': ticker,
                'date': row['Date'].strftime('%Y-%m-%d') if hasattr(row['Date'], 'strftime') else str(row['Date'])[:10],
                'open': float(row['Open']),
                'high': float(row['High']),
                'low': float(row['Low']),
                'close': float(row['Close']),
                'volume': int(row['Volume']),
                'adj_close': float(row['Adj Close']) if 'Adj Close' in row else float(row['Close'])
            }
            records.append(record)
        
        logger.info(f"Downloaded {len(records)} records for {ticker}")
        return records
    
    except Exception as e:
        logger.error(f"Error downloading data for {ticker}: {e}")
        raise


def download_multiple_tickers(
    tickers: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    period: str = "1y"
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Download data for multiple tickers.
    
    Args:
        tickers: List of stock ticker symbols
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
        period: Period to download if dates not specified
    
    Returns:
        Dictionary mapping ticker to list of price records
    """
    results = {}
    
    for ticker in tickers:
        try:
            records = download_ticker_data(ticker, start_date, end_date, period)
            results[ticker] = records
        except Exception as e:
            logger.error(f"Failed to download {ticker}: {e}")
            results[ticker] = []
    
    return results

