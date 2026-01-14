"""Helper functions for fundamental analysis tasks."""
import logging
import sys
import os
import asyncio
import aiohttp
import pandas as pd
from typing import Dict, Any, List
from .. import config

logger = logging.getLogger(__name__)


# ============ Download Helpers ============

async def fetch_tickers_from_db() -> list:
    """Fetch all tickers from database"""
    try:
        async with aiohttp.ClientSession() as session:
            payload = {
                "storage_name": config.TICKER_STORAGE,
                "model_class_name": "tickers",
                "filters": {}
            }
            async with session.post(f"{config.API_URL}/query", json=payload) as response:
                if response.status == 200:
                    result = await response.json()
                    tickers = set()
                    for doc in result.get("data", []):
                        ticker = doc.get("tickers")
                        if ticker:
                            tickers.add(ticker)
                    return sorted(list(tickers))
    except Exception as e:
        logger.error(f"Query error: {str(e)}", exc_info=True)
    return []


def get_providers():
    """Get financial data providers"""
    parent_dir = os.path.join(os.path.dirname(__file__), '../..')
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    from providers.provider_factory import ProviderFactory
    return {
        "balance_sheet": ProviderFactory.get_provider("yahoo_for_balance_sheet"),
        "cash_flow": ProviderFactory.get_provider("yahoo_for_cash_flow")
    }


async def save_to_mongodb(ticker: str, year: int, quarter: int, data: Dict[str, Any], source: str = None):
    """Save financial data to MongoDB in ticker-specific collection"""
    try:
        async with aiohttp.ClientSession() as session:
            serialized_data = {}
            for key, value in data.items():
                serialized_data[key] = value.to_dict(orient='records') if isinstance(value, pd.DataFrame) else value

            payload_data = {
                "ticker": ticker,
                "year": year,
                "quarter": quarter,
                **serialized_data
            }
            if source:
                payload_data["source"] = source

            # Use ticker-specific collection like aapl_financial_statements
            collection_name = f"{ticker.lower()}_financial_statements"
            payload = {
                "storage_name": config.FINANCIAL_STORAGE,
                "model_class_name": collection_name,
                "data": payload_data
            }
            async with session.post(f"{config.API_URL}/write", json=payload) as response:
                if response.status == 200:
                    logger.info(f"Saved data for {ticker} in collection {collection_name}")
    except Exception as e:
        logger.error(f"Error saving {ticker}: {str(e)}", exc_info=True)


# ============ Extract Helpers ============

async def fetch_raw_statements(ticker: str, year: int, quarter: int):
    """Fetch raw financial statements from ticker-specific collection"""
    try:
        async with aiohttp.ClientSession() as session:
            collection_name = f"{ticker.lower()}_financial_statements"
            payload = {
                "storage_name": config.FINANCIAL_STORAGE,
                "model_class_name": collection_name,
                "filters": {"year": year, "quarter": quarter}
            }
            async with session.post(f"{config.API_URL}/query/one", json=payload) as response:
                if response.status == 200:
                    result = await response.json()
                    return result.get("data", {})
    except Exception as e:
        logger.error(f"Error fetching raw data for {ticker}: {str(e)}", exc_info=True)
    return {}


def extract_value(data_list, field_name, default=0):
    """Extract value from data list"""
    if not data_list or len(data_list) == 0:
        return default
    first = data_list[0] if isinstance(data_list, list) else data_list
    return first.get(field_name, default) if isinstance(first, dict) else default


async def get_all_tickers_with_data(year: int, quarter: int) -> List[str]:
    """Get all tickers that have financial data for given period by checking all ticker collections"""
    # Get list of all tickers first
    try:
        async with aiohttp.ClientSession() as session:
            payload = {
                "storage_name": config.TICKER_STORAGE,
                "model_class_name": "tickers",
                "filters": {}
            }
            async with session.post(f"{config.API_URL}/query", json=payload) as response:
                if response.status != 200:
                    return []

                result = await response.json()
                tickers = []
                for doc in result.get("data", []):
                    ticker = doc.get("tickers")
                    if ticker:
                        tickers.append(ticker)

                # Check which tickers have financial data for the given period
                tickers_with_data = []
                for ticker in tickers:
                    collection_name = f"{ticker.lower()}_financial_statements"
                    check_payload = {
                        "storage_name": config.FINANCIAL_STORAGE,
                        "model_class_name": collection_name,
                        "filters": {"year": year, "quarter": quarter}
                    }
                    async with session.post(f"{config.API_URL}/query", json=check_payload) as check_response:
                        if check_response.status == 200:
                            check_result = await check_response.json()
                            if check_result.get("data"):
                                tickers_with_data.append(ticker)

                return sorted(tickers_with_data)
    except Exception as e:
        logger.error(f"Error fetching tickers: {str(e)}", exc_info=True)
    return []
