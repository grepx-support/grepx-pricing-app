"""Storage configuration models using pydantic"""

from pathlib import Path
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator


class StorageConfig(BaseModel):
    """Storage configuration with automatic validation"""
    
    storage_name: str
    storage_type: str
    
    # Network params
    host: Optional[str] = None
    port: Optional[int] = None
    database_name: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    
    # File params
    file_path: Optional[str] = None
    connection_string: Optional[str] = None
    
    # Additional
    connection_params: Optional[Dict[str, Any]] = None
    active_flag: bool = True
    
    @field_validator('file_path')
    @classmethod
    def validate_file_path(cls, v):
        """Clean null values"""
        return None if v == 'null' else v
    
    @field_validator('host', 'username', 'password', 'database_name')
    @classmethod
    def validate_strings(cls, v):
        """Clean null values"""
        return None if v == 'null' else v
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters as dict"""
        params = {}
        
        if self.connection_string:
            params['connection_string'] = self.connection_string
            if self.file_path:
                params['file_path'] = self.file_path
            return params
        
        if self.host:
            params['host'] = self.host
        if self.port:
            params['port'] = self.port
        if self.database_name:
            params['database'] = self.database_name
        if self.username:
            params['username'] = self.username
        if self.password:
            params['password'] = self.password
        if self.file_path:
            params['file_path'] = self.file_path
        
        if self.connection_params:
            params.update(self.connection_params)
        
        return params
    
    def is_file_based(self) -> bool:
        """Check if file-based storage"""
        return self.storage_type.lower() in ['sqlite', 'duckdb']
    
    def safe_params(self) -> Dict[str, Any]:
        """Get params with password hidden"""
        params = self.get_connection_params()
        if 'password' in params:
            params['password'] = '***'
        if 'connection_string' in params and '@' in params['connection_string']:
            parts = params['connection_string'].split('@')
            if len(parts) == 2:
                user = parts[0].split(':')[0]
                params['connection_string'] = f"{user}:***@{parts[1]}"
        return params
