import setup_paths

import logging
from typing import Dict, List
from config.config_loader import ConfigLoader
from dbManager import BaseMasterDBManager
from services.read_service import ReadService
from services.write_service import WriteService
from backends.discovery import discover_backends
from backends.registry import get_backend_class

class DatabaseServer:
    def __init__(self, config_path: str = None):
        self.config = ConfigLoader(config_path)
        self.master_db: BaseMasterDBManager = None
        self.read_service = ReadService()
        self.write_service = WriteService()
        self.logger = logging.getLogger(self.__class__.__name__)
        
        log_config = self.config.get_logging_config()
        logging.basicConfig(
            level=getattr(logging, log_config.get('level', 'INFO')),
            format=log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
    
    async def initialize(self):
        self.logger.info("Initializing Database Server...")
        
        discover_backends()
        
        master_config = self.config.get_master_db_config()
        self.master_db = BaseMasterDBManager(master_config)
        await self.master_db.connect()
        
        self.logger.info(f"Master DB connected (Type: {master_config.get('type')})")
        
        storages = await self.master_db.get_all_active_storages()
        self.logger.info(f"Found {len(storages)} active storage configurations")
        
        for storage in storages:
            await self._register_storage_connections(storage)
        
        self.logger.info("Database Server initialized successfully")
    
    async def _register_storage_connections(self, storage: Dict):
        try:
            storage_name = storage.get('storage_name')
            storage_type = storage.get('storage_type')
            
            self.logger.info(f"Registering storage: {storage_name} (Type: {storage_type})")
            
            backend_class = get_backend_class(storage_type)
            connection_params = self._build_connection_params(storage)
            
            read_backend = backend_class()
            await read_backend.connect(**connection_params)
            self.read_service.register_backend(storage_name, read_backend)
            
            write_backend = backend_class()
            await write_backend.connect(**connection_params)
            self.write_service.register_backend(storage_name, write_backend)
            
            self.logger.info(f"Storage '{storage_name}' registered successfully with separate read/write connections")
        
        except Exception as e:
            self.logger.error(f"Failed to register storage '{storage.get('storage_name')}': {e}", exc_info=True)
    
    def _build_connection_params(self, storage: Dict) -> Dict:
        params = {}
        
        connection_string = storage.get('connection_string')
        if connection_string:
            params['connection_string'] = connection_string
        else:
            host = storage.get('host')
            port = storage.get('port')
            database_name = storage.get('database_name')
            username = storage.get('username')
            password = storage.get('password')
            file_path = storage.get('file_path')
            
            if host:
                params['host'] = host
            if port:
                params['port'] = port
            if database_name:
                params['database'] = database_name
            if username:
                params['username'] = username
            if password:
                params['password'] = password
            if file_path:
                params['file_path'] = file_path
        
        connection_params = storage.get('connection_params')
        if connection_params:
            params.update(connection_params)
        
        return params
    
    async def select_data(self, storage_name: str, model_class, filters: Dict = None, limit: int = None, offset: int = None) -> List:
        backend = self.read_service.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No connection pool found for storage: {storage_name}")
        
        return await self.read_service.fetch_all(storage_name, model_class, filters, limit, offset)
    
    async def select_one(self, storage_name: str, model_class, filters: Dict):
        backend = self.read_service.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No connection pool found for storage: {storage_name}")
        
        return await self.read_service.fetch_one(storage_name, model_class, filters)
    
    async def write_data(self, storage_name: str, model):
        backend = self.write_service.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No connection pool found for storage: {storage_name}")
        
        return await self.write_service.insert(storage_name, model)
    
    async def update_data(self, storage_name: str, model) -> None:
        backend = self.write_service.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No connection pool found for storage: {storage_name}")
        
        await self.write_service.update(storage_name, model)
    
    async def delete_data(self, storage_name: str, model) -> None:
        backend = self.write_service.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No connection pool found for storage: {storage_name}")
        
        await self.write_service.delete(storage_name, model)
    
    async def upsert_data(self, storage_name: str, model, filter_fields: Dict) -> bool:
        backend = self.write_service.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No connection pool found for storage: {storage_name}")
        
        return await self.write_service.upsert(storage_name, model, filter_fields)
    
    async def bulk_write_data(self, storage_name: str, models: List) -> List:
        backend = self.write_service.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No connection pool found for storage: {storage_name}")
        
        return await self.write_service.bulk_insert(storage_name, models)
    
    async def bulk_upsert_data(self, storage_name: str, model_class, records: List[Dict], key_fields: List[str], batch_size: int = 100) -> int:
        backend = self.write_service.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No connection pool found for storage: {storage_name}")
        
        return await self.write_service.bulk_upsert(storage_name, model_class, records, key_fields, batch_size)
    
    async def count_data(self, storage_name: str, model_class, filters: Dict = None) -> int:
        backend = self.read_service.get_backend(storage_name)
        if not backend:
            raise ValueError(f"No connection pool found for storage: {storage_name}")
        
        return await self.read_service.count(storage_name, model_class, filters)
    
    async def shutdown(self):
        self.logger.info("Shutting down Database Server...")
        
        for storage_name in list(self.read_service._sessions.keys()):
            backend = self.read_service.get_backend(storage_name)
            if backend:
                await backend.disconnect()
        
        for storage_name in list(self.write_service._sessions.keys()):
            backend = self.write_service.get_backend(storage_name)
            if backend:
                await backend.disconnect()
        
        if self.master_db:
            await self.master_db.disconnect()
        
        self.logger.info("Database Server shut down successfully")
