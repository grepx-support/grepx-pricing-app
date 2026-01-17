"""Base classes for internal tasks (calculate, read, write)."""
import asyncio
import aiohttp
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime

from .. import config

logger = logging.getLogger(__name__)


class InternalTask(ABC):
    """Abstract base class for internal tasks."""

    def __init__(self, task_name: str, collection_name: str, storage_name: str = None):
        """
        Initialize internal task.

        Args:
            task_name: Name of the task
            collection_name: MongoDB collection name
            storage_name: Storage name from storage_master (defaults to METRICS_STORAGE)
        """
        self.task_name = task_name
        self.collection_name = collection_name
        self.storage_name = storage_name or config.METRICS_STORAGE
        self.api_url = config.API_URL

    async def _query_data(self, query: Dict[str, Any] = None, projection: Dict[str, Any] = None,
                          collection: str = None, storage: str = None) -> List[Dict[str, Any]]:
        """Query data from MongoDB via API."""
        payload = {
            "storage_name": storage or self.storage_name,
            "model_class_name": collection or self.collection_name,
            "query": query or {},
            "projection": projection or {}
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{self.api_url}/query", json=payload) as response:
                    if response.status != 200:
                        raise Exception(f"API error {response.status}: {await response.text()}")
                    result = await response.json()
                    if not result.get("success"):
                        raise Exception(result.get("error"))
                    return result.get("data", [])
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise

    def query_data(self, query: Dict[str, Any] = None, projection: Dict[str, Any] = None,
                   collection: str = None, storage: str = None) -> List[Dict[str, Any]]:
        """Synchronous wrapper for querying data."""
        return asyncio.run(self._query_data(query, projection, collection, storage))

    async def _store_record(self, data: Dict[str, Any], filter_fields: Dict[str, Any],
                            collection: str = None, storage: str = None) -> str:
        """Store a single record to MongoDB via API."""
        payload = {
            "storage_name": storage or self.storage_name,
            "model_class_name": collection or self.collection_name,
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
            logger.error(f"Store failed: {e}")
            raise

    def store_record(self, data: Dict[str, Any], filter_fields: Dict[str, Any],
                     collection: str = None, storage: str = None) -> str:
        """Synchronous wrapper for storing a record."""
        return asyncio.run(self._store_record(data, filter_fields, collection, storage))


class CalculateTask(InternalTask):
    """Base class for calculate tasks."""

    @abstractmethod
    def calculate(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Perform calculation.

        Returns:
            List of calculated records to store
        """
        pass

    def execute(self, **kwargs) -> Dict[str, Any]:
        """Execute the calculate task."""
        start_time = datetime.now()

        try:
            records = self.calculate(**kwargs)
            logger.info(f"{self.task_name}: Calculated {len(records)} records")

            if not records:
                return {
                    "status": "success",
                    "task_name": self.task_name,
                    "records_calculated": 0,
                    "records_stored": 0,
                    "duration_seconds": (datetime.now() - start_time).total_seconds()
                }

            # Store records
            stored = 0
            for record in records:
                try:
                    filter_fields = self._get_filter_fields(record)
                    self.store_record(record, filter_fields)
                    stored += 1
                except Exception as e:
                    logger.error(f"Failed to store record: {e}")

            return {
                "status": "success",
                "task_name": self.task_name,
                "records_calculated": len(records),
                "records_stored": stored,
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

    def _get_filter_fields(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Get filter fields for upsert operations."""
        filter_fields = {}
        if "ticker" in record:
            filter_fields["ticker"] = record["ticker"]
        if "date" in record:
            filter_fields["date"] = record["date"]
        return filter_fields if filter_fields else {"_id": record.get("_id")}


class ReadTask(InternalTask):
    """Base class for read tasks."""

    @abstractmethod
    def read(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Read data from database.

        Returns:
            List of records
        """
        pass

    def execute(self, **kwargs) -> Dict[str, Any]:
        """Execute the read task."""
        start_time = datetime.now()

        try:
            records = self.read(**kwargs)
            logger.info(f"{self.task_name}: Read {len(records)} records")

            return {
                "status": "success",
                "task_name": self.task_name,
                "records_read": len(records),
                "data": records,
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


class WriteTask(InternalTask):
    """Base class for write tasks."""

    @abstractmethod
    def write(self, data: List[Dict[str, Any]], **kwargs) -> int:
        """
        Write data to database.

        Args:
            data: List of records to write

        Returns:
            Number of records written
        """
        pass

    def execute(self, data: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """Execute the write task."""
        start_time = datetime.now()

        try:
            written = self.write(data, **kwargs)
            logger.info(f"{self.task_name}: Wrote {written} records")

            return {
                "status": "success",
                "task_name": self.task_name,
                "records_written": written,
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
