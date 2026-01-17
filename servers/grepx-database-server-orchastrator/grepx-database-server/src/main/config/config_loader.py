import setup_paths

import os
from pathlib import Path
from typing import Dict, Any


class ConfigLoader:
    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "resources" / "config.yaml"

        self.config_path = Path(config_path)
        self.config: Dict[str, Any] = {}
        self._load()
        self._override_from_env()

    def _load(self):
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        import yaml
        with open(self.config_path, 'r') as f:
            self.config = yaml.safe_load(f)

    def _override_from_env(self):
        # Priority 1: Use GREPX_MASTER_DB_URL if set (standard across all servers)
        if os.getenv('GREPX_MASTER_DB_URL'):
            if 'master_db' not in self.config:
                self.config['master_db'] = {}
            connection_string = os.getenv('GREPX_MASTER_DB_URL')
            self.config['master_db']['connection_string'] = connection_string
            self.config['master_db']['enabled'] = True
            # Detect type from connection string
            if connection_string.startswith('sqlite'):
                self.config['master_db']['type'] = 'sqlite'
            elif connection_string.startswith('postgresql'):
                self.config['master_db']['type'] = 'postgresql'
            elif connection_string.startswith('mongodb'):
                self.config['master_db']['type'] = 'mongodb'
            print(f"Using GREPX_MASTER_DB_URL: {connection_string}")

        if os.getenv('MASTER_DB_ENABLED') is not None:
            enabled = os.getenv('MASTER_DB_ENABLED', 'true').lower() == 'true'
            if 'master_db' not in self.config:
                self.config['master_db'] = {}
            self.config['master_db']['enabled'] = enabled

        if os.getenv('MASTER_DB_TYPE'):
            if 'master_db' not in self.config:
                self.config['master_db'] = {}
            self.config['master_db']['type'] = os.getenv('MASTER_DB_TYPE')

        if os.getenv('MASTER_DB_HOST'):
            if 'master_db' not in self.config:
                self.config['master_db'] = {}
            host = os.getenv('MASTER_DB_HOST')
            port = os.getenv('MASTER_DB_PORT', '5432')
            user = os.getenv('MASTER_DB_USER', 'postgres')
            password = os.getenv('MASTER_DB_PASSWORD', 'postgres')
            dbname = os.getenv('MASTER_DB_NAME', 'grepx_master')
            self.config['master_db']['connection_string'] = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

        if os.getenv('MASTER_DB_SQLITE_ENABLED') is not None:
            enabled = os.getenv('MASTER_DB_SQLITE_ENABLED', 'false').lower() == 'true'
            if 'master_db_sqlite' not in self.config:
                self.config['master_db_sqlite'] = {}
            self.config['master_db_sqlite']['enabled'] = enabled

        if os.getenv('MASTER_DB_SQLITE_PATH'):
            if 'master_db_sqlite' not in self.config:
                self.config['master_db_sqlite'] = {}
            sqlite_path = os.getenv('MASTER_DB_SQLITE_PATH')
            self.config['master_db_sqlite']['type'] = 'sqlite'
            self.config['master_db_sqlite']['connection_string'] = f"sqlite:///{sqlite_path}"

        if os.getenv('MASTER_DB_MONGODB_ENABLED') is not None:
            enabled = os.getenv('MASTER_DB_MONGODB_ENABLED', 'false').lower() == 'true'
            if 'master_db_mongodb' not in self.config:
                self.config['master_db_mongodb'] = {}
            self.config['master_db_mongodb']['enabled'] = enabled

        if os.getenv('MASTER_DB_MONGODB_HOST'):
            if 'master_db_mongodb' not in self.config:
                self.config['master_db_mongodb'] = {}
            host = os.getenv('MASTER_DB_MONGODB_HOST')
            port = os.getenv('MASTER_DB_MONGODB_PORT', '27017')
            user = os.getenv('MASTER_DB_MONGODB_USER', '')
            password = os.getenv('MASTER_DB_MONGODB_PASSWORD', '')
            dbname = os.getenv('MASTER_DB_MONGODB_NAME', 'grepx_master')

            if user and password:
                connection_string = f"mongodb://{user}:{password}@{host}:{port}/{dbname}"
            else:
                connection_string = f"mongodb://{host}:{port}/{dbname}"

            self.config['master_db_mongodb']['type'] = 'mongodb'
            self.config['master_db_mongodb']['connection_string'] = connection_string

        if os.getenv('SERVER_HOST'):
            if 'server' not in self.config:
                self.config['server'] = {}
            self.config['server']['host'] = os.getenv('SERVER_HOST')

        if os.getenv('SERVER_PORT'):
            if 'server' not in self.config:
                self.config['server'] = {}
            self.config['server']['port'] = int(os.getenv('SERVER_PORT'))

        if os.getenv('SERVER_DEBUG') is not None:
            if 'server' not in self.config:
                self.config['server'] = {}
            self.config['server']['debug'] = os.getenv('SERVER_DEBUG', 'false').lower() == 'true'

        if os.getenv('SERVER_WORKERS'):
            if 'server' not in self.config:
                self.config['server'] = {}
            self.config['server']['workers'] = int(os.getenv('SERVER_WORKERS'))

        if os.getenv('LOG_LEVEL'):
            if 'logging' not in self.config:
                self.config['logging'] = {}
            self.config['logging']['level'] = os.getenv('LOG_LEVEL')

        if os.getenv('LOG_FORMAT'):
            if 'logging' not in self.config:
                self.config['logging'] = {}
            self.config['logging']['format'] = os.getenv('LOG_FORMAT')

        if os.getenv('CONNECTION_POOL_DEFAULT_SIZE'):
            if 'connection_pools' not in self.config:
                self.config['connection_pools'] = {}
            self.config['connection_pools']['default_pool_size'] = int(os.getenv('CONNECTION_POOL_DEFAULT_SIZE'))

        if os.getenv('CONNECTION_POOL_READ_SIZE'):
            if 'connection_pools' not in self.config:
                self.config['connection_pools'] = {}
            self.config['connection_pools']['read_pool_size'] = int(os.getenv('CONNECTION_POOL_READ_SIZE'))

        if os.getenv('CONNECTION_POOL_WRITE_SIZE'):
            if 'connection_pools' not in self.config:
                self.config['connection_pools'] = {}
            self.config['connection_pools']['write_pool_size'] = int(os.getenv('CONNECTION_POOL_WRITE_SIZE'))

    def get(self, key: str, default=None):
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        return value

    def get_master_db_config(self) -> Dict[str, Any]:
        master_db = self.config.get('master_db', {})
        if master_db.get('enabled', True):
            return master_db

        master_db_sqlite = self.config.get('master_db_sqlite', {})
        if master_db_sqlite.get('enabled', False):
            return master_db_sqlite

        master_db_mongodb = self.config.get('master_db_mongodb', {})
        if master_db_mongodb.get('enabled', False):
            return master_db_mongodb

        return master_db

    def get_server_config(self) -> Dict[str, Any]:
        return self.config.get('server', {})

    def get_pool_config(self) -> Dict[str, Any]:
        return self.config.get('connection_pools', {})

    def get_logging_config(self) -> Dict[str, Any]:
        return self.config.get('logging', {})
