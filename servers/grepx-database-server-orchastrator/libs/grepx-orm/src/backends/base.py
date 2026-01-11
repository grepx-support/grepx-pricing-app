"""Abstract base class for all database backends"""

from abc import ABC, abstractmethod
from typing import List, Optional, Any, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from core.base import Model
    from core.query import Query


class DatabaseBackend(ABC):
    """Abstract base class for all database backends"""
    
    @property
    @abstractmethod
    def backend_name(self) -> str:
        """Return the name of this backend"""
        pass
    
    @property
    @abstractmethod
    def dialect(self):
        """Return the dialect for this backend"""
        pass
    
    @abstractmethod
    async def connect(self, **connection_params) -> None:
        """Connect to the database"""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the database"""
        pass
    
    @abstractmethod
    async def create_table(self, model_class: Type['Model']) -> None:
        """Create a table/collection for the model"""
        pass
    
    @abstractmethod
    async def drop_table(self, model_class: Type['Model']) -> None:
        """Drop a table/collection for the model"""
        pass
    
    @abstractmethod
    async def insert(self, model: 'Model') -> Any:
        """Insert a model instance and return the primary key"""
        pass
    
    @abstractmethod
    async def update(self, model: 'Model') -> None:
        """Update a model instance"""
        pass
    
    @abstractmethod
    async def delete(self, model: 'Model') -> None:
        """Delete a model instance"""
        pass
    
    @abstractmethod
    async def query_all(self, query: 'Query') -> List['Model']:
        """Execute query and return all results"""
        pass
    
    @abstractmethod
    async def query_first(self, query: 'Query') -> Optional['Model']:
        """Execute query and return first result"""
        pass
    
    @abstractmethod
    async def query_count(self, query: 'Query') -> int:
        """Execute query and return count"""
        pass

    @abstractmethod
    async def upsert(self, model: 'Model', filter_fields: dict) -> bool:
        """
        Upsert a single record - insert if not exists, update if exists.

        Uses database-native upsert optimizations:
        - MongoDB: Uses $set operator with upsert=True
        - PostgreSQL: Uses INSERT...ON CONFLICT...UPDATE
        - SQLite: Uses INSERT...ON CONFLICT

        Args:
            model: Model instance to upsert
            filter_fields: Dict of fields to match for upsert (e.g., {'symbol': 'AAPL', 'date': '2025-01-01'})

        Returns:
            True if successfully upserted, False otherwise
        """
        pass

    @abstractmethod
    async def bulk_upsert(self, model_class: Type['Model'], records: List[dict], key_fields: List[str], batch_size: int = 100) -> int:
        """
        Bulk upsert multiple records with database-native optimizations.

        Args:
            model_class: Model class for the records
            records: List of record dictionaries to upsert
            key_fields: Fields used for matching existing records (composite key)
            batch_size: Number of records to process in each batch (default: 100)

        Returns:
            Total number of records successfully upserted
        """
        pass

    # Optional hooks for backend-specific optimizations
    async def begin_transaction(self):
        """Begin a transaction (optional)"""
        pass

    async def commit_transaction(self):
        """Commit a transaction (optional)"""
        pass

    async def rollback_transaction(self):
        """Rollback a transaction (optional)"""
        pass

