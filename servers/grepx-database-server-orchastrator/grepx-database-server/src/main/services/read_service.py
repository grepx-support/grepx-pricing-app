import setup_paths

from typing import List, Dict, Any, Optional
from backends.base import DatabaseBackend

class ReadService:
    def __init__(self):
        self._sessions: Dict[str, DatabaseBackend] = {}
    
    def register_backend(self, storage_name: str, backend: DatabaseBackend):
        self._sessions[storage_name] = backend
    
    def get_backend(self, storage_name: str) -> Optional[DatabaseBackend]:
        return self._sessions.get(storage_name)
    
    async def execute_query(self, storage_name: str, model_class, filters: Optional[Dict[str, Any]] = None) -> List[Any]:
        backend = self.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No read backend available for storage: {storage_name}")
        
        from core.query import Query
        query = Query(model_class)
        
        if filters:
            for key, value in filters.items():
                query = query.filter(**{key: value})
        
        return await backend.query_all(query)
    
    async def fetch_one(self, storage_name: str, model_class, filters: Dict[str, Any]) -> Optional[Any]:
        backend = self.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No read backend available for storage: {storage_name}")
        
        from core.query import Query
        query = Query(model_class)
        
        for key, value in filters.items():
            query = query.filter(**{key: value})
        
        return await backend.query_first(query)
    
    async def fetch_all(self, storage_name: str, model_class, filters: Optional[Dict[str, Any]] = None, limit: Optional[int] = None, offset: Optional[int] = None) -> List[Any]:
        backend = self.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No read backend available for storage: {storage_name}")
        
        from core.query import Query
        query = Query(model_class)
        
        if filters:
            for key, value in filters.items():
                query = query.filter(**{key: value})
        
        if limit:
            query = query.limit(limit)
        
        if offset:
            query = query.offset(offset)
        
        return await backend.query_all(query)
    
    async def count(self, storage_name: str, model_class, filters: Optional[Dict[str, Any]] = None) -> int:
        backend = self.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No read backend available for storage: {storage_name}")
        
        from core.query import Query
        query = Query(model_class)
        
        if filters:
            for key, value in filters.items():
                query = query.filter(**{key: value})
        
        return await backend.query_count(query)
