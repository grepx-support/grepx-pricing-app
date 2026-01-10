"""Storage master table model for database configurations."""
from sqlalchemy import Column, String, Integer, Boolean, DateTime, JSON, Text, Enum as SQLEnum
from datetime import datetime
from .base import Base
import enum


class StorageType(enum.Enum):
    """Supported storage/database types."""
    MONGODB = "mongodb"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    DUCKDB = "duckdb"
    CSV = "csv"
    REDIS = "redis"


class StorageMaster(Base):
    """Master table for all storage/database configurations."""
    
    __tablename__ = 'storage_master'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    storage_name = Column(String(255), nullable=False, unique=True, index=True, comment='Unique name for this storage configuration')
    storage_type = Column(SQLEnum(StorageType), nullable=False, comment='Type of storage/database')
    host = Column(String(255), nullable=True, comment='Database host address')
    port = Column(Integer, nullable=True, comment='Database port number')
    database_name = Column(String(255), nullable=True, comment='Database/schema name')
    username = Column(String(255), nullable=True, comment='Database username')
    password = Column(String(255), nullable=True, comment='Database password (encrypted)')
    connection_string = Column(Text, nullable=True, comment='Full connection string (if applicable)')
    file_path = Column(String(500), nullable=True, comment='File path for file-based storage (CSV, SQLite, etc.)')
    auth_source = Column(String(100), nullable=True, comment='Authentication source (MongoDB)')
    ssl_enabled = Column(Boolean, default=False, comment='Whether SSL/TLS is enabled')
    connection_params = Column(JSON, nullable=True, comment='Additional connection parameters as JSON')
    credentials = Column(JSON, nullable=True, comment='Additional credentials as JSON (encrypted)')
    storage_metadata = Column(JSON, nullable=True, comment='Additional metadata for storage configuration')
    is_default = Column(Boolean, default=False, comment='Whether this is the default storage')
    active_flag = Column(Boolean, default=True, nullable=False)
    max_connections = Column(Integer, default=10, comment='Maximum number of connections in pool')
    timeout_seconds = Column(Integer, default=30, comment='Connection timeout in seconds')
    description = Column(Text, nullable=True, comment='Description of this storage configuration')
    created_date = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_by = Column(String(100), nullable=True)
    updated_by = Column(String(100), nullable=True)
    
    def __repr__(self):
        return f"<StorageMaster(id={self.id}, storage_name='{self.storage_name}', storage_type='{self.storage_type.value}', active={self.active_flag})>"
    
    def to_dict(self):
        """Convert model to dictionary."""
        return {
            'id': self.id,
            'storage_name': self.storage_name,
            'storage_type': self.storage_type.value if self.storage_type else None,
            'host': self.host,
            'port': self.port,
            'database_name': self.database_name,
            'username': self.username,
            'password': '***HIDDEN***',  # Never expose password in dict
            'connection_string': self.connection_string,
            'file_path': self.file_path,
            'auth_source': self.auth_source,
            'ssl_enabled': self.ssl_enabled,
            'connection_params': self.connection_params,
            'credentials': '***HIDDEN***',  # Never expose credentials in dict
            'storage_metadata': self.storage_metadata,
            'is_default': self.is_default,
            'active_flag': self.active_flag,
            'max_connections': self.max_connections,
            'timeout_seconds': self.timeout_seconds,
            'description': self.description,
            'created_date': self.created_date.isoformat() if self.created_date else None,
            'updated_date': self.updated_date.isoformat() if self.updated_date else None,
            'created_by': self.created_by,
            'updated_by': self.updated_by
        }
    
    def get_connection_config(self):
        """Get connection configuration dictionary for use with database factory."""
        config = {
            'type': self.storage_type.value if self.storage_type else None,
            'database_name': self.database_name,
        }
        
        if self.connection_string:
            config['connection_string'] = self.connection_string
        else:
            # Build connection details
            if self.host:
                config['host'] = self.host
            if self.port:
                config['port'] = self.port
            if self.username:
                config['username'] = self.username
            if self.password:
                config['password'] = self.password
            if self.auth_source:
                config['auth_source'] = self.auth_source
        
        if self.file_path:
            config['file_path'] = self.file_path
        
        if self.ssl_enabled:
            config['ssl'] = True
        
        if self.connection_params:
            config.update(self.connection_params)
        
        return config

