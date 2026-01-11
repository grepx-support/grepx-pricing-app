"""Session management with context managers and dependency injection"""

from typing import Optional, Dict, Any, List, Type
from .connection import create_backend_from_connection_string
from .query import Query
from .base import Model
from backends.base import DatabaseBackend
from backends.discovery import discover_backends
from backends.registry import get_backend_class


class Session:
    """Database session with context manager support"""

    _current: Optional['Session'] = None

    def __init__(self, backend: DatabaseBackend, connection_params: Dict[str, Any]):
        self.backend = backend
        self.connection_params = connection_params
        self._connected = False

    @classmethod
    def get_current(cls) -> 'Session':
        """Get the current active session"""
        if cls._current is None:
            raise RuntimeError("No active session. Use session context manager.")
        return cls._current

    @classmethod
    def from_backend_name(cls, backend_name: str, **connection_params):
        """Create session from backend name and parameters"""
        discover_backends()
        backend_class = get_backend_class(backend_name)
        backend = backend_class()
        return cls(backend, connection_params)

    @classmethod
    def from_connection_string(cls, connection_string: str):
        """Create session from connection string"""
        backend, params = create_backend_from_connection_string(connection_string)
        return cls(backend, params)

    async def __aenter__(self):
        """Async context manager entry"""
        await self.backend.connect(**self.connection_params)
        self._connected = True
        Session._current = self
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.backend.disconnect()
        self._connected = False
        Session._current = None

    # Delegated methods
    async def create_table(self, model_class):
        """Create a table/collection for the model"""
        return await self.backend.create_table(model_class)

    async def drop_table(self, model_class):
        """Drop a table/collection for the model"""
        return await self.backend.drop_table(model_class)

    async def add(self, model):
        """Add a model instance to the database"""
        return await self.backend.insert(model)

    async def update(self, model):
        """Update a model instance in the database"""
        await self.backend.update(model)

    async def delete(self, model):
        """Delete a model instance from the database"""
        await self.backend.delete(model)

    async def query_all(self, query: Query):
        """Execute query and return all results"""
        return await self.backend.query_all(query)

    async def query_first(self, query: Query):
        """Execute query and return first result"""
        return await self.backend.query_first(query)

    async def query_count(self, query: Query):
        """Execute query and return count"""
        return await self.backend.query_count(query)

    # Upsert operations (delegate to backend native implementations)
    async def upsert(self, model: Model, filter_fields: Dict[str, Any]) -> bool:
        """
        Upsert a single record using database-native implementation.

        This is atomic and much more efficient than query-then-insert/update pattern.
        Automatically delegates to the appropriate backend implementation:
        - MongoDB: Native $set operator with upsert=True
        - PostgreSQL: INSERT ... ON CONFLICT ... UPDATE
        - SQLite: INSERT ... ON CONFLICT ... UPDATE with transactions

        Args:
            model: Model instance to upsert
            filter_fields: Dict of fields to match for upsert (e.g., {'symbol': 'AAPL', 'date': '2025-01-01'})

        Returns:
            True if successfully upserted, False otherwise

        Example:
            model = StockPriceModel(symbol='AAPL', date='2025-01-01', open=150.0, ...)
            success = await session.upsert(model, {'symbol': 'AAPL', 'date': '2025-01-01'})
        """
        return await self.backend.upsert(model, filter_fields)

    async def bulk_upsert(self, model_class: Type[Model], records: List[Dict],
                          key_fields: List[str], batch_size: int = 100) -> int:
        """
        Bulk upsert multiple records using database-native implementation.

        Much more efficient than individual upsert calls. Automatically delegates to
        the appropriate backend implementation with optimizations for each database.

        Args:
            model_class: Model class for the records
            records: List of record dictionaries to upsert
            key_fields: Fields used for matching existing records (composite key)
            batch_size: Number of records to process in each batch (default: 100)

        Returns:
            Total number of records successfully upserted

        Example:
            records = [
                {"symbol": "AAPL", "date": "2025-01-01", "open": 150.0, ...},
                {"symbol": "AAPL", "date": "2025-01-02", "open": 151.0, ...},
            ]
            count = await session.bulk_upsert(
                StockPriceModel, records,
                key_fields=['symbol', 'date'],
                batch_size=100
            )
        """
        return await self.backend.bulk_upsert(model_class, records, key_fields, batch_size)

    # Transaction support
    async def begin(self):
        """Begin a transaction"""
        if hasattr(self.backend, 'begin_transaction'):
            await self.backend.begin_transaction()

    async def commit(self):
        """Commit a transaction"""
        if hasattr(self.backend, 'commit_transaction'):
            await self.backend.commit_transaction()

    async def rollback(self):
        """Rollback a transaction"""
        if hasattr(self.backend, 'rollback_transaction'):
            await self.backend.rollback_transaction()


def create_session(backend_type: str, connection_string: str) -> Session:
    """Create a session from backend type and connection string (legacy support)"""
    return Session.from_connection_string(connection_string)
