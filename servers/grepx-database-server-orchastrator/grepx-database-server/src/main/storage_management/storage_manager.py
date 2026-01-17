"""Simple storage manager using pydantic for configuration"""

import logging
from pathlib import Path
from typing import Dict, Optional

from backends.registry import get_backend_class
from backends.base import DatabaseBackend
from .storage_config import StorageConfig


class StorageManager:
    """Manages all database connections"""

    def __init__(self, project_root: Optional[str] = None):
        self.logger = logging.getLogger(__name__)
        self.project_root = Path(project_root or Path.cwd()).resolve()
        self.backends: Dict[str, DatabaseBackend] = {}

    async def connect(self, storage_dict: Dict) -> None:
        """Connect to a storage"""
        config = StorageConfig(**storage_dict)
        name = config.storage_name

        self.logger.info(f"Connecting: {name} ({config})")

        # Get connection params
        params = config.get_connection_params()

        # Resolve file paths
        if config.is_file_based():
            params = self._resolve_file_path(params, name)

        self.logger.debug(f"Params: {config.safe_params()}")

        # Create and connect backend
        backend_class = get_backend_class(config.storage_type.lower())
        backend = backend_class()
        await backend.connect(**params)

        # Test connection for MongoDB (to catch auth errors early)
        if config.storage_type.lower() == 'mongodb':
            await self._test_mongodb_connection(backend, name)

        self.backends[name] = backend
        self.logger.info(f"✓ {name}")

    async def _test_mongodb_connection(self, backend, name: str) -> None:
        """Test MongoDB connection to catch auth errors early"""
        try:
            # Try to list collections to verify auth works
            await backend.database.list_collection_names()
        except Exception as e:
            error_msg = str(e)
            if 'Authentication failed' in error_msg or 'AuthenticationFailed' in error_msg:
                raise ValueError(
                    f"MongoDB authentication failed for '{name}'. "
                    f"Check username/password in storage_master table."
                )
            raise

    def _resolve_file_path(self, params: Dict, storage_name: str) -> Dict:
        """Resolve file paths to absolute"""
        file_path_str = None

        # Extract from connection_string
        if 'connection_string' in params:
            conn_str = params['connection_string']
            if conn_str.startswith('sqlite:///'):
                file_path_str = conn_str[10:]

        # Or from file_path
        if not file_path_str and 'file_path' in params:
            file_path_str = params['file_path']

        if not file_path_str:
            return params

        # Resolve to absolute
        file_path = Path(file_path_str)
        if not file_path.is_absolute():
            file_path = (self.project_root / file_path).resolve()

        # Create directory
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Log
        exists = file_path.exists()
        self.logger.info(
            f"{'✓' if exists else '→'} {file_path.name} "
            f"({file_path.stat().st_size:,} bytes)" if exists else "(new)"
        )

        # Update params with POSIX path for SQLite
        if 'connection_string' in params:
            params['connection_string'] = f"sqlite:///{file_path.as_posix()}"
        if 'file_path' in params:
            params['file_path'] = str(file_path)

        return params

    def get(self, storage_name: str) -> Optional[DatabaseBackend]:
        """Get backend by name"""
        return self.backends.get(storage_name)

    def list_storages(self):
        """Get all storage names"""
        return list(self.backends.keys())

    async def disconnect_all(self) -> None:
        """Close all connections"""
        for name, backend in list(self.backends.items()):
            try:
                await backend.disconnect()
                self.logger.info(f"Closed: {name}")
            except Exception as e:
                self.logger.error(f"Error closing {name}: {e}")
        self.backends.clear()
