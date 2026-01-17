"""
DatabaseServerClient - Unified client for GrepX Database Server

Single client with both async and sync methods, with polars integration for easy data handling.
"""

import httpx
import asyncio
from typing import Dict, List, Any, Optional, Union
try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False


class DatabaseServerClient:
    """
    Unified client for GrepX Database Server with async/sync methods and polars integration.
    
    Usage:
        # Async usage
        async with DatabaseServerClient() as client:
            await client.insert_async("tickers", "companies", {"ticker": "AAPL"})
            df = await client.query_df_async("tickers", "companies", {"sector": "Tech"})
        
        # Sync usage
        with DatabaseServerClient() as client:
            client.insert("tickers", "companies", {"ticker": "AAPL"})
            df = client.query_df("tickers", "companies", {"sector": "Tech"})
    """
    
    def __init__(self, base_url: str = "http://localhost:8000", timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._async_client: Optional[httpx.AsyncClient] = None
        self._sync_client: Optional[httpx.Client] = None
    
    @property
    def async_client(self) -> httpx.AsyncClient:
        if self._async_client is None:
            self._async_client = httpx.AsyncClient(timeout=self.timeout)
        return self._async_client
    
    @property
    def sync_client(self) -> httpx.Client:
        if self._sync_client is None:
            self._sync_client = httpx.Client(timeout=self.timeout)
        return self._sync_client
    
    # ========== HEALTH CHECK ==========
    
    async def health_async(self) -> Dict[str, str]:
        """Check server health (async)"""
        response = await self.async_client.get(f"{self.base_url}/health")
        response.raise_for_status()
        return response.json()
    
    def health(self) -> Dict[str, str]:
        """Check server health (sync)"""
        response = self.sync_client.get(f"{self.base_url}/health")
        response.raise_for_status()
        return response.json()
    
    # ========== INSERT OPERATIONS ==========
    
    async def insert_async(
        self, 
        storage_name: str, 
        collection: str, 
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Insert single record (async)"""
        payload = {
            "storage_name": storage_name,
            "model_class_name": collection,
            "data": data
        }
        response = await self.async_client.post(f"{self.base_url}/write", json=payload)
        response.raise_for_status()
        return response.json()
    
    def insert(
        self, 
        storage_name: str, 
        collection: str, 
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Insert single record (sync)"""
        payload = {
            "storage_name": storage_name,
            "model_class_name": collection,
            "data": data
        }
        response = self.sync_client.post(f"{self.base_url}/write", json=payload)
        response.raise_for_status()
        return response.json()
    
    # ========== QUERY OPERATIONS ==========
    
    async def query_async(
        self,
        storage_name: str,
        collection: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Query multiple records (async)"""
        payload = {
            "storage_name": storage_name,
            "model_class_name": collection,
            "filters": filters or {},
            "limit": limit,
            "offset": offset
        }
        response = await self.async_client.post(f"{self.base_url}/query", json=payload)
        response.raise_for_status()
        return response.json().get("data", [])
    
    def query(
        self,
        storage_name: str,
        collection: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Query multiple records (sync)"""
        payload = {
            "storage_name": storage_name,
            "model_class_name": collection,
            "filters": filters or {},
            "limit": limit,
            "offset": offset
        }
        response = self.sync_client.post(f"{self.base_url}/query", json=payload)
        response.raise_for_status()
        return response.json().get("data", [])
    
    async def query_one_async(
        self,
        storage_name: str,
        collection: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Query single record (async)"""
        payload = {
            "storage_name": storage_name,
            "model_class_name": collection,
            "filters": filters or {}
        }
        response = await self.async_client.post(f"{self.base_url}/query/one", json=payload)
        response.raise_for_status()
        return response.json().get("data")
    
    def query_one(
        self,
        storage_name: str,
        collection: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Query single record (sync)"""
        payload = {
            "storage_name": storage_name,
            "model_class_name": collection,
            "filters": filters or {}
        }
        response = self.sync_client.post(f"{self.base_url}/query/one", json=payload)
        response.raise_for_status()
        return response.json().get("data")
    
    # ========== POLARS DATAFRAME QUERY ==========
    
    async def query_df_async(
        self,
        storage_name: str,
        collection: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> "pl.DataFrame":
        """Query and return results as polars DataFrame (async)"""
        if not POLARS_AVAILABLE:
            raise ImportError("polars not installed. Install with: pip install polars")
        
        data = await self.query_async(storage_name, collection, filters, limit, offset)
        return pl.DataFrame(data) if data else pl.DataFrame()
    
    def query_df(
        self,
        storage_name: str,
        collection: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> "pl.DataFrame":
        """Query and return results as polars DataFrame (sync)"""
        if not POLARS_AVAILABLE:
            raise ImportError("polars not installed. Install with: pip install polars")
        
        data = self.query(storage_name, collection, filters, limit, offset)
        return pl.DataFrame(data) if data else pl.DataFrame()
    
    # ========== UPSERT OPERATIONS ==========
    
    async def upsert_async(
        self,
        storage_name: str,
        collection: str,
        data: Dict[str, Any],
        filter_fields: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Insert or update record (async)"""
        payload = {
            "storage_name": storage_name,
            "model_class_name": collection,
            "data": data,
            "filter_fields": filter_fields
        }
        response = await self.async_client.post(f"{self.base_url}/upsert", json=payload)
        response.raise_for_status()
        return response.json()
    
    def upsert(
        self,
        storage_name: str,
        collection: str,
        data: Dict[str, Any],
        filter_fields: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Insert or update record (sync)"""
        payload = {
            "storage_name": storage_name,
            "model_class_name": collection,
            "data": data,
            "filter_fields": filter_fields
        }
        response = self.sync_client.post(f"{self.base_url}/upsert", json=payload)
        response.raise_for_status()
        return response.json()
    
    async def bulk_upsert_async(
        self,
        storage_name: str,
        collection: str,
        records: Union[List[Dict[str, Any]], "pl.DataFrame"],
        key_fields: List[str],
        batch_size: int = 100
    ) -> Dict[str, Any]:
        """Bulk upsert records (async) - accepts list or polars DataFrame"""
        if POLARS_AVAILABLE and isinstance(records, pl.DataFrame):
            records = records.to_dicts()
        
        payload = {
            "storage_name": storage_name,
            "model_class_name": collection,
            "records": records,
            "key_fields": key_fields,
            "batch_size": batch_size
        }
        response = await self.async_client.post(f"{self.base_url}/bulk-upsert", json=payload)
        response.raise_for_status()
        return response.json()
    
    def bulk_upsert(
        self,
        storage_name: str,
        collection: str,
        records: Union[List[Dict[str, Any]], "pl.DataFrame"],
        key_fields: List[str],
        batch_size: int = 100
    ) -> Dict[str, Any]:
        """Bulk upsert records (sync) - accepts list or polars DataFrame"""
        if POLARS_AVAILABLE and isinstance(records, pl.DataFrame):
            records = records.to_dicts()
        
        payload = {
            "storage_name": storage_name,
            "model_class_name": collection,
            "records": records,
            "key_fields": key_fields,
            "batch_size": batch_size
        }
        response = self.sync_client.post(f"{self.base_url}/bulk-upsert", json=payload)
        response.raise_for_status()
        return response.json()
    
    # ========== COUNT OPERATIONS ==========
    
    async def count_async(
        self,
        storage_name: str,
        collection: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> int:
        """Count records (async)"""
        payload = {
            "storage_name": storage_name,
            "model_class_name": collection,
            "filters": filters or {}
        }
        response = await self.async_client.post(f"{self.base_url}/count", json=payload)
        response.raise_for_status()
        return response.json().get("count", 0)
    
    def count(
        self,
        storage_name: str,
        collection: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> int:
        """Count records (sync)"""
        payload = {
            "storage_name": storage_name,
            "model_class_name": collection,
            "filters": filters or {}
        }
        response = self.sync_client.post(f"{self.base_url}/count", json=payload)
        response.raise_for_status()
        return response.json().get("count", 0)
    
    # ========== CLEANUP ==========
    
    async def close_async(self):
        """Close async client"""
        if self._async_client:
            await self._async_client.aclose()
    
    def close(self):
        """Close sync client"""
        if self._sync_client:
            self._sync_client.close()
    
    def close_all(self):
        """Close both clients"""
        if self._sync_client:
            self._sync_client.close()
        if self._async_client:
            asyncio.run(self._async_client.aclose())
    
    # ========== CONTEXT MANAGERS ==========
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close_async()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
