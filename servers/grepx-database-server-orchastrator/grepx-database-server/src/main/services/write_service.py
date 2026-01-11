import setup_paths

from typing import List, Dict, Any, Optional
from backends.base import DatabaseBackend

class WriteService:
    def __init__(self):
        self._sessions: Dict[str, DatabaseBackend] = {}
    
    def register_backend(self, storage_name: str, backend: DatabaseBackend):
        self._sessions[storage_name] = backend
    
    def get_backend(self, storage_name: str) -> Optional[DatabaseBackend]:
        return self._sessions.get(storage_name)
    
    async def insert(self, storage_name: str, model) -> Any:
        backend = self.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No write backend available for storage: {storage_name}")
        
        return await backend.insert(model)
    
    async def update(self, storage_name: str, model) -> None:
        backend = self.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No write backend available for storage: {storage_name}")
        
        await backend.update(model)
    
    async def delete(self, storage_name: str, model) -> None:
        backend = self.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No write backend available for storage: {storage_name}")
        
        await backend.delete(model)
    
    async def upsert(self, storage_name: str, model, filter_fields: Dict[str, Any]) -> bool:
        backend = self.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No write backend available for storage: {storage_name}")
        
        return await backend.upsert(model, filter_fields)
    
    async def bulk_insert(self, storage_name: str, models: List[Any]) -> List[Any]:
        backend = self.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No write backend available for storage: {storage_name}")
        
        results = []
        for model in models:
            result = await backend.insert(model)
            results.append(result)
        
        return results
    
    async def bulk_upsert(self, storage_name: str, model_class, records: List[Dict[str, Any]], key_fields: List[str], batch_size: int = 100) -> int:
        backend = self.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No write backend available for storage: {storage_name}")
        
        return await backend.bulk_upsert(model_class, records, key_fields, batch_size)
    
    async def transaction_context(self, storage_name: str):
        backend = self.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No write backend available for storage: {storage_name}")
        
        return TransactionContext(backend)

class TransactionContext:
    def __init__(self, backend):
        self.backend = backend
    
    async def __aenter__(self):
        await self.backend.begin_transaction()
        return self.backend
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            await self.backend.rollback_transaction()
        else:
            await self.backend.commit_transaction()
