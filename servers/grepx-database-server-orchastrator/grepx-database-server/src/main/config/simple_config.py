"""Configuration using pydantic-settings"""

from pathlib import Path
from typing import Optional, Dict, Any
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MasterDBConfig(BaseSettings):
    """Master database configuration"""
    type: str = 'sqlite'
    connection_string: Optional[str] = None
    enabled: bool = True
    
    model_config = SettingsConfigDict(
        env_prefix='MASTER_DB_',
        env_file='.env',
        case_sensitive=False
    )


class ServerConfig(BaseSettings):
    """Server configuration"""
    host: str = '0.0.0.0'
    port: int = 8000
    debug: bool = False
    workers: int = 1
    
    model_config = SettingsConfigDict(
        env_prefix='SERVER_',
        env_file='.env',
        case_sensitive=False
    )


class LoggingConfig(BaseSettings):
    """Logging configuration"""
    level: str = 'INFO'
    format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    model_config = SettingsConfigDict(
        env_prefix='LOG_',
        env_file='.env',
        case_sensitive=False
    )


class Config:
    """Main configuration - uses pydantic-settings for env override"""
    
    def __init__(self, config_path: Optional[str] = None):
        # Load from file if provided
        self._yaml_config = {}
        if config_path:
            self._load_yaml(config_path)
        
        # Pydantic settings (auto-loads from env)
        self.master_db = MasterDBConfig()
        self.server = ServerConfig()
        self.logging = LoggingConfig()
        
        # Override with GREPX_MASTER_DB_URL if set
        import os
        if url := os.getenv('GREPX_MASTER_DB_URL'):
            self.master_db.connection_string = url
            if url.startswith('sqlite'):
                self.master_db.type = 'sqlite'
            elif url.startswith('postgresql'):
                self.master_db.type = 'postgresql'
            elif url.startswith('mongodb'):
                self.master_db.type = 'mongodb'
    
    def _load_yaml(self, path: str):
        """Load YAML config as fallback"""
        try:
            import yaml
            with open(path, 'r') as f:
                self._yaml_config = yaml.safe_load(f) or {}
        except Exception:
            pass
    
    def get_master_db_config(self) -> Dict[str, Any]:
        """Get master DB config as dict"""
        return {
            'type': self.master_db.type,
            'connection_string': self.master_db.connection_string,
            'enabled': self.master_db.enabled
        }
    
    def get_server_config(self) -> Dict[str, Any]:
        """Get server config as dict"""
        return {
            'host': self.server.host,
            'port': self.server.port,
            'debug': self.server.debug,
            'workers': self.server.workers
        }
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging config as dict"""
        return {
            'level': self.logging.level,
            'format': self.logging.format
        }
