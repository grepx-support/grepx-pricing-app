"""Load tickers from CSV file and store in database."""
from typing import Dict, Any
from .utils import TickerLoader


def load_tickers(csv_file_path: str, date: str, collection_name: str = "tickers") -> Dict[str, Any]:
    """Load tickers from CSV file and store to database"""
    return TickerLoader.load_tickers(csv_file_path, date, collection_name)
