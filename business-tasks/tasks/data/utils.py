"""Utilities for data loading tasks - CSV reading and MongoDB storage."""
import asyncio
import pandas as pd
import aiohttp
import logging
from typing import Dict, Any
from pathlib import Path
from .. import config

logger = logging.getLogger(__name__)


class CSVReader:
    """Handles reading and validating CSV files"""

    @staticmethod
    def read_csv(csv_file_path: str) -> tuple:
        """Read CSV file and return dataframe and ticker column name

        Handles relative paths by trying multiple resolution strategies:
        1. Path as given (absolute or relative to current working directory)
        2. Path relative to project root
        3. Filename in project data/ folder
        """
        csv_path = Path(csv_file_path)
        paths_tried = [str(csv_path)]

        # Try 1: Path as given
        if not csv_path.exists():
            # Try 2: Resolve from project root
            project_root = Path(__file__).parent.parent.parent.parent
            csv_path = project_root / csv_file_path
            paths_tried.append(str(csv_path))

            # Try 3: If still not found, try data folder with just the filename
            if not csv_path.exists():
                csv_path = project_root / "data" / Path(csv_file_path).name
                paths_tried.append(str(csv_path))

        try:
            df = pd.read_csv(str(csv_path))
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found. Tried: {' | '.join(paths_tried)}")
        except Exception as e:
            raise Exception(f"Error reading CSV from {csv_path}: {str(e)}")

        # Auto-detect ticker column
        ticker_col = 'ticker' if 'ticker' in df.columns else 'symbol'
        if ticker_col not in df.columns:
            raise ValueError(f"Missing '{ticker_col}' column")

        logger.info(f"Loaded {len(df)} rows with ticker column: {ticker_col}")
        return df, ticker_col


class TickerStorage:
    """Handles storing tickers to MongoDB"""

    @staticmethod
    async def store_ticker(data: Dict[str, Any], collection_name: str, ticker: str) -> str:
        """Store a single ticker to MongoDB with upsert to prevent duplicates"""
        payload = {
            "storage_name": config.TICKER_STORAGE,
            "model_class_name": collection_name,
            "data": data,
            "filter_fields": {"tickers": ticker}
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{config.API_URL}/upsert", json=payload) as response:
                    if response.status != 200:
                        raise Exception(f"API error {response.status}: {await response.text()}")
                    result = await response.json()
                    if not result.get("success"):
                        raise Exception(result.get("error"))
                    logger.info(f"Stored ticker {ticker} to collection {collection_name}")
                    return str(result.get("id", "unknown"))
        except Exception as e:
            logger.error(f"Database store failed for {ticker}: {str(e)}")
            raise


class TickerLoader:
    """Orchestrates loading tickers from CSV to database"""

    @staticmethod
    def load_tickers(csv_file_path: str, date: str, collection_name: str = "tickers") -> Dict[str, Any]:
        """Load tickers from CSV file and store to database"""
        try:
            df, ticker_col = CSVReader.read_csv(csv_file_path)
        except (FileNotFoundError, ValueError, Exception) as e:
            return {"status": "failed", "error": str(e)}

        stored_count = 0
        for row in df.iterrows():
            try:
                ticker = str(row[1][ticker_col]).strip().upper()
                document = {"date": date, "tickers": ticker}
                asyncio.run(TickerStorage.store_ticker(document, collection_name, ticker))
                stored_count += 1
            except Exception as e:
                logger.error(f"Failed to store {row[1][ticker_col]}: {str(e)}")

        total_tickers = len(df)
        logger.info(f"Summary - Total: {total_tickers}, Stored: {stored_count}")

        return {
            "status": "success",
            "tickers_stored": stored_count,
            "total_tickers": total_tickers,
            "collection_name": collection_name
        }
