"""Download close prices from Yahoo Finance and store to database."""
import asyncio
import aiohttp
import logging
import yfinance as yf
from typing import Dict, Any, List
from datetime import datetime

from .base import DownloadTask
from .. import config

logger = logging.getLogger(__name__)


class DownloadFromYahooClosePrice(DownloadTask):
    """Download close prices from Yahoo Finance."""

    def __init__(self):
        super().__init__(
            task_name="download_from_yahoo_close_price",
            collection_name="close_price",
            storage_name=config.TICKER_STORAGE
        )

    async def _get_active_tickers(self) -> List[str]:
        """Fetch active tickers from database via API."""
        payload = {
            "storage_name": self.storage_name,
            "model_class_name": "active_stocks",
            "filters": None,
            "limit": None,
            "offset": None
        }

        try:
            logger.info(f"Querying API: {self.api_url}/query")
            logger.info(f"Payload: {payload}")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{self.api_url}/query", json=payload) as response:
                    status = response.status
                    
                    if status != 200:
                        text = await response.text()
                        logger.error(f"API error {status}: {text}")
                        raise Exception(f"API error {status}: {text}")
                    
                    # API returns QueryResponse: {"data": [...], "count": n}
                    result = await response.json()
                    logger.info(f"API Response: {result}")
                    
                    # Get data directly (no "success" field)
                    data = result.get("data", [])
                    
                    if not data:
                        logger.warning("No active stocks found in database")
                        return []
                    
                    # Extract tickers
                    tickers = [record.get("ticker") for record in data if record.get("ticker")]
                    logger.info(f"Fetched {len(tickers)} tickers: {tickers}")
                    return tickers
                    
        except Exception as e:
            logger.error(f"Failed to fetch active tickers: {e}", exc_info=True)
            raise
    def get_active_tickers(self) -> List[str]:
        """Synchronous wrapper for getting active tickers."""
        return asyncio.run(self._get_active_tickers())

    def fetch_data(self, tickers: List[str] = None, date: str = None, partition_key: str = None) -> List[Dict[str, Any]]:
        """
        Fetch close prices from Yahoo Finance.

        Args:
            tickers: List of tickers (default: fetch from active_stocks)
            date: Date string (default: today)
            partition_key: Partition key from Dagster (used as target date)

        Returns:
            List of close price records
        """
        # Use partition_key as the target date if provided
        target_date = partition_key or date
        if target_date is None:
            target_date = datetime.now().strftime("%Y-%m-%d")

        # Get tickers from active_stocks if not provided
        if tickers is None:
            tickers = self.get_active_tickers()
            logger.info(f"Fetched {len(tickers)} tickers from active_stocks")

        if not tickers:
            logger.warning("No tickers to fetch")
            return []

        records = []

        # Parse target date and calculate date range for fetching
        # We fetch a small range to ensure we get the target date's data
        target_dt = datetime.strptime(target_date, "%Y-%m-%d")
        # Fetch from target date to day after to ensure we get the target date
        start_date = target_date
        # Add one day to end_date to include target_date in yfinance results
        from datetime import timedelta
        end_dt = target_dt + timedelta(days=1)
        end_date = end_dt.strftime("%Y-%m-%d")

        logger.info(f"Fetching close prices for date: {target_date}")

        for ticker in tickers:
            try:
                # Fetch data from Yahoo Finance for the specific date range
                stock = yf.Ticker(ticker)
                hist = stock.history(start=start_date, end=end_date)

                if hist.empty:
                    logger.warning(f"No data for ticker {ticker} on date {target_date}")
                    continue

                # Store ALL rows returned (should be just the target date)
                for idx, row in hist.iterrows():
                    record_date = idx.strftime("%Y-%m-%d")
                    records.append({
                        "ticker": ticker,
                        "date": record_date,
                        "price": float(row['Close']),
                        "source": "yahoo"
                    })
                    logger.debug(f"Fetched {ticker} for {record_date}: {row['Close']:.2f}")

            except Exception as e:
                logger.error(f"Failed to fetch {ticker}: {e}")
                continue

        logger.info(f"Fetched close prices for {len(records)} ticker-date combinations")
        return records


# Task instance
_task_instance = DownloadFromYahooClosePrice()


def download_from_yahoo_close_price(tickers: List[str] = None, date: str = None, partition_key: str = None, **kwargs) -> Dict[str, Any]:
    """
    Download close prices from Yahoo Finance and store to database.

    Args:
        tickers: List of tickers (default: fetch from active_stocks)
        date: Date string (default: today)
        partition_key: Partition key from Dagster (used as target date)
        **kwargs: Additional arguments (ignored, for Dagster compatibility)

    Returns:
        Result dictionary with status and counts
    """
    return _task_instance.execute(tickers=tickers, date=date, partition_key=partition_key)
