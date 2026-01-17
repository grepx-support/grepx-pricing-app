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

    def fetch_data(self, tickers: List[str] = None, date: str = None, period: str = "1d") -> List[Dict[str, Any]]:
        """
        Fetch close prices from Yahoo Finance.

        Args:
            tickers: List of tickers (default: fetch from active_stocks)
            date: Date string (default: today)
            period: Yahoo Finance period (default: 1d)

        Returns:
            List of close price records
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        # Get tickers from active_stocks if not provided
        if tickers is None:
            tickers = self.get_active_tickers()
            logger.info(f"Fetched {len(tickers)} tickers from active_stocks")

        if not tickers:
            logger.warning("No tickers to fetch")
            return []

        records = []

        for ticker in tickers:
            try:
                # Fetch data from Yahoo Finance
                stock = yf.Ticker(ticker)
                hist = stock.history(period=period)

                if hist.empty:
                    logger.warning(f"No data for ticker: {ticker}")
                    continue

                # Get the latest close price
                latest = hist.iloc[-1]
                record_date = hist.index[-1].strftime("%Y-%m-%d")

                records.append({
                    "ticker": ticker,
                    "date": record_date,
                    "price": float(latest['Close']),
                    "source": "yahoo"
                })

                logger.debug(f"Fetched {ticker}: {latest['Close']:.2f}")

            except Exception as e:
                logger.error(f"Failed to fetch {ticker}: {e}")
                continue

        logger.info(f"Fetched close prices for {len(records)} tickers")
        return records


# Task instance
_task_instance = DownloadFromYahooClosePrice()


def download_from_yahoo_close_price(tickers: List[str] = None, date: str = None, period: str = "1d", **kwargs) -> Dict[str, Any]:
    """
    Download close prices from Yahoo Finance and store to database.

    Args:
        tickers: List of tickers (default: fetch from active_stocks)
        date: Date string (default: today)
        period: Yahoo Finance period (default: 1d)

    Returns:
        Result dictionary with status and counts
    """
    return _task_instance.execute(tickers=tickers, date=date, period=period)
