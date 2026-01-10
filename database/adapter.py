"""Database adapter interface."""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional


class DatabaseAdapter(ABC):
    """Abstract base class for database adapters."""
    
    @abstractmethod
    def connect(self):
        """Establish database connection."""
        pass
    
    @abstractmethod
    def disconnect(self):
        """Close database connection."""
        pass
    
    @abstractmethod
    def insert_many(self, table: str, records: List[Dict[str, Any]]):
        """
        Insert multiple records into a table.
        
        Args:
            table: Table name
            records: List of records to insert
        """
        pass
    
    @abstractmethod
    def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results.
        
        Args:
            sql: SQL query string
            params: Optional query parameters
            
        Returns:
            List of records as dictionaries
        """
        pass
    
    @abstractmethod
    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None):
        """
        Execute a SQL statement without returning results.
        
        Args:
            sql: SQL statement
            params: Optional statement parameters
        """
        pass

