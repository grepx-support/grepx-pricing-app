"""Yahoo Finance provider for cash flow statement data."""
import asyncio
from typing import Dict, List

import pandas as pd
import yfinance as yf

from .yahoo_provider import YahooProvider


class YahooProviderCashFlow(YahooProvider):
    """Yahoo Finance provider for cash flow statement data."""

    def download_cash_flow(self, ticker: str) -> pd.DataFrame:
        """Download the cash flow statement data for a given ticker using Yahoo Finance."""
        ticker_obj = yf.Ticker(ticker)
        cash_flow_data = ticker_obj.cash_flow
        return cash_flow_data.T

    async def download_cash_flow_async(self, ticker: str) -> pd.DataFrame:
        """Asynchronously download the cash flow statement data for a given ticker using Yahoo Finance."""
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(None, self.download_cash_flow, ticker)
        return data

    def download_multiple_cash_flows(self, symbols: List[str]) -> Dict[str, pd.DataFrame]:
        """Download cash flow data for multiple symbols using Yahoo Finance."""
        cash_flows = {}
        for ticker in symbols:
            cash_flows[ticker] = self.download_cash_flow(ticker)
        return cash_flows

    async def download_multiple_cash_flows_async(self, symbols: List[str]) -> Dict[str, pd.DataFrame]:
        """Asynchronously download cash flow data for multiple symbols using Yahoo Finance."""
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(
            None, self.download_multiple_cash_flows, symbols
        )
        return data
