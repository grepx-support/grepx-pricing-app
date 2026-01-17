"""Download active stocks from tickers.csv and store to database."""
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime

from .base import DownloadTask
from .. import config

logger = logging.getLogger(__name__)


class DownloadActiveStocks(DownloadTask):
    """Download active stocks from CSV file."""

    def __init__(self):
        super().__init__(
            task_name="download_active_stocks",
            collection_name="active_stocks",
            storage_name=config.TICKER_STORAGE
        )

    def fetch_data(self, csv_file_path: str = None, date: str = None, partition_key: str = None, **kwargs) -> List[Dict[str, Any]]:
        """
        Read tickers from CSV file.

        Args:
            csv_file_path: Path to CSV file (default: data/tickers.csv)
            date: Date string (default: today)
            partition_key: Partition key from Dagster (used as date if date not provided)

        Returns:
            List of ticker records
        """
        if date is None and partition_key is not None:
            date = partition_key
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        # Resolve CSV path
        if csv_file_path is None:
            csv_file_path = "data/tickers.csv"

        csv_path = Path(csv_file_path)
        if not csv_path.is_absolute():
            # Try project root
            project_root = Path(__file__).parent.parent.parent.parent
            csv_path = project_root / csv_file_path
            if not csv_path.exists():
                csv_path = project_root / "data" / Path(csv_file_path).name

        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_file_path}")

        # Read CSV
        df = pd.read_csv(str(csv_path))

        # Auto-detect ticker column
        ticker_col = None
        for col in ['ticker', 'symbol', 'Ticker', 'Symbol']:
            if col in df.columns:
                ticker_col = col
                break

        if ticker_col is None:
            raise ValueError(f"No ticker column found. Available columns: {list(df.columns)}")

        # Build records
        records = []
        for _, row in df.iterrows():
            ticker = str(row[ticker_col]).strip().upper()
            records.append({
                "ticker": ticker,
                "date": date,
                "source": "csv"
            })

        logger.info(f"Loaded {len(records)} tickers from {csv_path}")
        return records


# Task instance
_task_instance = DownloadActiveStocks()


def download_active_stocks(csv_file_path: str = None, date: str = None, partition_key: str = None, **kwargs) -> Dict[str, Any]:
    """
    Download active stocks from CSV and store to database.

    Args:
        csv_file_path: Path to CSV file (default: data/tickers.csv)
        date: Date string (default: today)
        partition_key: Partition key from Dagster (used as date if date not provided)
        **kwargs: Additional arguments

    Returns:
        Result dictionary with status and counts
    """
    # Use partition_key as date if date is not provided
    if date is None and partition_key is not None:
        date = partition_key
    
    return _task_instance.execute(csv_file_path=csv_file_path, date=date)