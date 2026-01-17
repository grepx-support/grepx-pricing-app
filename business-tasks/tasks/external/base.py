"""Base class for external download tasks."""
import asyncio
import aiohttp
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List
from datetime import datetime

from .. import config

logger = logging.getLogger(__name__)


class DownloadTask(ABC):
    """Abstract base class for download tasks."""

    def __init__(self, task_name: str, collection_name: str, storage_name: str = None):
        """
        Initialize download task.

        Args:
            task_name: Name of the task
            collection_name: MongoDB collection name to store data
            storage_name: Storage name from storage_master (defaults to TICKER_STORAGE)
        """
        self.task_name = task_name
        self.collection_name = collection_name
        self.storage_name = storage_name or config.TICKER_STORAGE
        self.api_url = config.API_URL

    @abstractmethod
    def fetch_data(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Fetch data from external source.

        Returns:
            List of data records to store
        """
        pass

    async def _store_record(self, data: Dict[str, Any], filter_fields: Dict[str, Any]) -> str:
        """Store a single record to MongoDB via API."""
        payload = {
            "storage_name": self.storage_name,
            "model_class_name": self.collection_name,
            "data": data,
            "filter_fields": filter_fields
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{self.api_url}/upsert", json=payload) as response:
                    if response.status != 200:
                        raise Exception(f"API error {response.status}: {await response.text()}")
                    result = await response.json()
                    if not result.get("success"):
                        raise Exception(result.get("error"))
                    return str(result.get("id", "unknown"))
        except Exception as e:
            logger.error(f"Failed to store record: {e}")
            raise

    def store_record(self, data: Dict[str, Any], filter_fields: Dict[str, Any]) -> str:
        """Synchronous wrapper for storing a record."""
        return asyncio.run(self._store_record(data, filter_fields))

    async def _store_batch(self, records: List[Dict[str, Any]], filter_fields: List[str]) -> Dict[str, Any]:
        """Store multiple records to MongoDB via API."""
        stored = 0
        failed = 0

        for record in records:
            try:
                # Build filter fields dict from the list of field names
                filter_dict = {field: record.get(field) for field in filter_fields if field in record}
                await self._store_record(record, filter_dict)
                stored += 1
            except Exception as e:
                logger.error(f"Failed to store record: {e}")
                failed += 1

        return {"stored": stored, "failed": failed}

    def store_batch(self, records: List[Dict[str, Any]], filter_fields: List[str]) -> Dict[str, Any]:
        """Synchronous wrapper for storing batch of records."""
        return asyncio.run(self._store_batch(records, filter_fields))

    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute the download task.

        Returns:
            Result dictionary with status and counts
        """
        start_time = datetime.now()

        try:
            records = self.fetch_data(**kwargs)
            logger.info(f"{self.task_name}: Fetched {len(records)} records")

            if not records:
                return {
                    "status": "success",
                    "task_name": self.task_name,
                    "records_fetched": 0,
                    "records_stored": 0,
                    "duration_seconds": (datetime.now() - start_time).total_seconds()
                }

            # Get the filter fields from first record
            filter_fields = self._get_filter_fields(records[0])
            result = self.store_batch(records, filter_fields)

            return {
                "status": "success",
                "task_name": self.task_name,
                "records_fetched": len(records),
                "records_stored": result["stored"],
                "records_failed": result["failed"],
                "duration_seconds": (datetime.now() - start_time).total_seconds()
            }
        except Exception as e:
            logger.error(f"{self.task_name} failed: {e}")
            return {
                "status": "failed",
                "task_name": self.task_name,
                "error": str(e),
                "duration_seconds": (datetime.now() - start_time).total_seconds()
            }

    def _get_filter_fields(self, record: Dict[str, Any]) -> List[str]:
        """Get the filter fields for upsert operations."""
        # Use both ticker and date as composite key to prevent overwriting
        if "ticker" in record and "date" in record:
            return ["ticker", "date"]
        elif "ticker" in record:
            return ["ticker"]
        elif "tickers" in record:
            return ["tickers"]
        return [list(record.keys())[0]]