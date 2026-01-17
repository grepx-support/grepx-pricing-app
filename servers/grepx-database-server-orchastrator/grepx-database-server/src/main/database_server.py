"""Database Server - Minimal implementation using libraries"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any

from backends.discovery import discover_backends
from dbManager import BaseMasterDBManager
from config.config_loader import ConfigLoader
from storage_management import StorageManager


class DatabaseServer:
    """Database server using grepx-orm and pydantic"""

    def __init__(self, config_path: str = None):
        self.config = ConfigLoader(config_path)
        self.master_db: Optional[BaseMasterDBManager] = None
        self.storage: Optional[StorageManager] = None
        self.logger = logging.getLogger(__name__)

        # Setup logging
        log_config = self.config.get_logging_config()
        logging.basicConfig(
            level=getattr(logging, log_config.get('level', 'INFO')),
            format=log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )

    async def initialize(self):
        """Initialize server"""
        self.logger.info("=" * 60)
        self.logger.info("DATABASE SERVER STARTING")
        self.logger.info("=" * 60)

        discover_backends()

        # Get project root
        import os
        project_root = os.environ.get('PROJECT_ROOT') or str(Path.cwd())
        self.storage = StorageManager(project_root)

        # Connect master DB
        master_config = self.config.get_master_db_config()
        self.master_db = BaseMasterDBManager(master_config)
        await self.master_db.connect()
        self.logger.info(f"✓ Master: {master_config.get('type')}")

        # Load and connect storages
        storages = await self.master_db.get_all_active_storages()
        self.logger.info(f"Loading {len(storages)} storage(s)...")

        for storage_dict in storages:
            try:
                self.logger.info(f"Connecting to DATABASE: {storage_dict}")
                await self.storage.connect(storage_dict)
            except Exception as e:
                name = storage_dict.get('storage_name', 'unknown')
                self.logger.error(f"✗ {name}: {e}")

        connected = self.storage.list_storages()
        self.logger.info(f"✓ Ready: {', '.join(connected) if connected else 'none'}")
        self.logger.info("=" * 60)

    # Read operations - delegate to grepx-orm
    async def select_data(self, storage_name: str, model_class,
                          filters: Optional[Dict] = None,
                          limit: Optional[int] = None,
                          offset: Optional[int] = None) -> List:
        from core.query import Query
        backend = self.storage.get(storage_name)
        if not backend:
            raise ValueError(f"Storage not found: {storage_name}")

        query = Query(model_class)
        if filters:
            for k, v in filters.items():
                query = query.filter(**{k: v})
        if limit:
            query = query.limit(limit)
        if offset:
            query = query.offset(offset)

        return await backend.query_all(query)

    async def select_one(self, storage_name: str, model_class, filters: Dict) -> Optional[Any]:
        from core.query import Query
        backend = self.storage.get(storage_name)
        if not backend:
            raise ValueError(f"Storage not found: {storage_name}")

        query = Query(model_class)
        for k, v in filters.items():
            query = query.filter(**{k: v})

        return await backend.query_first(query)

    async def count_data(self, storage_name: str, model_class,
                         filters: Optional[Dict] = None) -> int:
        from core.query import Query
        backend = self.storage.get(storage_name)
        if not backend:
            raise ValueError(f"Storage not found: {storage_name}")

        query = Query(model_class)
        if filters:
            for k, v in filters.items():
                query = query.filter(**{k: v})

        return await backend.query_count(query)

    # Write operations - delegate to grepx-orm
    async def write_data(self, storage_name: str, model) -> Any:
        backend = self.storage.get(storage_name)
        if not backend:
            raise ValueError(f"Storage not found: {storage_name}")
        return await backend.insert(model)

    async def update_data(self, storage_name: str, model) -> None:
        backend = self.storage.get(storage_name)
        if not backend:
            raise ValueError(f"Storage not found: {storage_name}")
        await backend.update(model)

    async def delete_data(self, storage_name: str, model) -> None:
        backend = self.storage.get(storage_name)
        if not backend:
            raise ValueError(f"Storage not found: {storage_name}")
        await backend.delete(model)

    async def upsert_data(self, storage_name: str, model, filter_fields: Dict) -> bool:
        backend = self.storage.get(storage_name)
        if not backend:
            raise ValueError(f"Storage not found: {storage_name}")
        return await backend.upsert(model, filter_fields)

    async def bulk_write_data(self, storage_name: str, models: List) -> List:
        backend = self.storage.get(storage_name)
        if not backend:
            raise ValueError(f"Storage not found: {storage_name}")

        results = []
        for model in models:
            results.append(await backend.insert(model))
        return results

    async def bulk_upsert_data(self, storage_name: str, model_class,
                               records: List[Dict], key_fields: List[str],
                               batch_size: int = 100) -> int:
        backend = self.storage.get(storage_name)
        if not backend:
            raise ValueError(f"Storage not found: {storage_name}")

        return await backend.bulk_upsert(model_class, records, key_fields, batch_size)

    async def shutdown(self):
        """Shutdown server"""
        self.logger.info("Shutting down...")

        if self.storage:
            await self.storage.disconnect_all()

        if self.master_db:
            await self.master_db.disconnect()

        self.logger.info("✓ Shutdown complete")
