"""Download financial data from providers and store in database."""
import logging
import asyncio
from typing import Dict, Any, List
from .helpers import fetch_tickers_from_db, get_providers, save_to_mongodb

logger = logging.getLogger(__name__)


def download_financial_data(year: int, quarter: int, data_types: List[str] = None, source: str = None, **kwargs) -> Dict[str, Any]:
    """Download financial data for all tickers"""
    if data_types is None:
        data_types = ["balance_sheet", "income_statement", "cash_flow"]

    tickers = asyncio.run(fetch_tickers_from_db())
    if not tickers:
        raise ValueError("No tickers found in database")

    providers = get_providers()
    downloaded_count = 0

    for ticker in tickers:
        try:
            data = {}
            if "balance_sheet" in data_types:
                data["balance_sheet"] = providers["balance_sheet"].download_balance_sheet(ticker)
            if "income_statement" in data_types:
                data["income_statement"] = providers["balance_sheet"].download_income_statement(ticker)
            if "cash_flow" in data_types:
                data["cash_flow"] = providers["cash_flow"].download_cash_flow(ticker)

            if all(df.empty for df in data.values()):
                continue

            asyncio.run(save_to_mongodb(ticker, year, quarter, data, source))
            downloaded_count += 1
        except Exception as e:
            logger.error(f"Failed for {ticker}: {str(e)}")

    return {"status": "success", "tickers_downloaded": downloaded_count}
