"""Query builder for constructing database queries"""

from typing import List, Optional, Type

from .base import Model


class Query:
    """Query builder class for constructing and executing queries"""
    
    def __init__(self, model_class: Type[Model]):
        self.model_class = model_class
        self._filters = []
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
        self._order_by: List[str] = []
    
    def filter(self, **kwargs) -> 'Query':
        """Add equality filters to the query"""
        for key, value in kwargs.items():
            self._filters.append((key, '=', value))
        return self
    
    def limit(self, limit: int) -> 'Query':
        """Set the maximum number of results"""
        self._limit = limit
        return self
    
    def offset(self, offset: int) -> 'Query':
        """Set the offset for pagination"""
        self._offset = offset
        return self
    
    def order_by(self, *fields: str) -> 'Query':
        """Add ordering to the query. Prefix with '-' for descending order"""
        self._order_by.extend(fields)
        return self
    
    async def all(self) -> List[Model]:
        """Execute query and return all results"""
        from .session import Session
        session = Session.get_current()
        return await session.query_all(self)
    
    async def first(self) -> Optional[Model]:
        """Execute query and return the first result"""
        from .session import Session
        session = Session.get_current()
        return await session.query_first(self)
    
    async def count(self) -> int:
        """Execute query and return the count of results"""
        from .session import Session
        session = Session.get_current()
        return await session.query_count(self)
