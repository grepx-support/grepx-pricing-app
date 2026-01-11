import setup_paths

import yaml
import logging
from pathlib import Path
from typing import List, Dict, Any
from core.session import Session
from .base_master_db_manager import StorageMaster

logger = logging.getLogger(__name__)


class StorageInitializer:
    """Initializes storage_master table from database.yaml configuration"""

    def __init__(self, master_db_session: Session, database_yaml_path: str = None):
        """
        Initialize storage initializer

        Args:
            master_db_session: Active master database session
            database_yaml_path: Path to database.yaml file (defaults to task-generator-server resources)
        """
        self.session = master_db_session

        if database_yaml_path is None:
            # Default to task-generator-server database.yaml
            database_yaml_path = Path(__file__).parent.parent.parent.parent.parent.parent / \
                "grepx-task-generator-server" / "src" / "main" / "resources" / "database.yaml"

        self.database_yaml_path = Path(database_yaml_path)
        logger.info(f"StorageInitializer using config: {self.database_yaml_path}")

    def load_database_yaml(self) -> List[Dict[str, Any]]:
        """Load storage configurations from database.yaml"""
        if not self.database_yaml_path.exists():
            logger.warning(f"Database config file not found: {self.database_yaml_path}")
            return []

        try:
            with open(self.database_yaml_path, 'r') as f:
                config = yaml.safe_load(f)

            storage_configs = config.get('storage_configurations', [])
            logger.info(f"Loaded {len(storage_configs)} storage configurations from database.yaml")
            return storage_configs

        except Exception as e:
            logger.error(f"Error loading database.yaml: {e}")
            return []

    async def populate_storage_master(self, storage_configs: List[Dict[str, Any]] = None):
        """
        Populate storage_master table with configurations from database.yaml

        Args:
            storage_configs: List of storage configurations. If None, loads from database.yaml
        """
        if storage_configs is None:
            storage_configs = self.load_database_yaml()

        if not storage_configs:
            logger.warning("No storage configurations to populate")
            return

        for config in storage_configs:
            try:
                storage_name = config.get('storage_name')

                # Check if storage already exists
                existing = await StorageMaster.query().filter(storage_name=storage_name).first()

                if existing:
                    # Update existing record
                    await self._update_storage_master(existing, config)
                    logger.info(f"Updated storage configuration: {storage_name}")
                else:
                    # Create new record
                    await self._create_storage_master(config)
                    logger.info(f"Created storage configuration: {storage_name}")

            except Exception as e:
                logger.error(f"Error processing storage config {config.get('storage_name')}: {e}")

    async def _create_storage_master(self, config: Dict[str, Any]):
        """Create new StorageMaster record"""
        storage = StorageMaster()
        storage.storage_name = config.get('storage_name')
        storage.storage_type = config.get('storage_type')
        storage.host = config.get('host')
        storage.port = config.get('port')
        storage.database_name = config.get('database_name')
        storage.username = config.get('username')
        storage.password = config.get('password')
        storage.connection_string = config.get('connection_string')
        storage.file_path = config.get('file_path')
        storage.auth_source = config.get('auth_source')
        storage.ssl_enabled = config.get('ssl_enabled', False)
        storage.connection_params = config.get('connection_params')
        storage.credentials = config.get('credentials')
        storage.storage_metadata = config.get('storage_metadata')
        storage.is_default = config.get('is_default', False)
        storage.active_flag = config.get('active_flag', True)
        storage.max_connections = config.get('max_connections', 10)
        storage.timeout_seconds = config.get('timeout_seconds', 30)
        storage.description = config.get('description')
        storage.created_date = config.get('created_date')
        storage.updated_date = config.get('update_date')
        storage.created_by = config.get('created_by')
        storage.updated_by = config.get('updated_by')

        await self.session.add(storage)
        await self.session.commit()

    async def _update_storage_master(self, existing: StorageMaster, config: Dict[str, Any]):
        """Update existing StorageMaster record"""
        existing.storage_type = config.get('storage_type')
        existing.host = config.get('host')
        existing.port = config.get('port')
        existing.database_name = config.get('database_name')
        existing.username = config.get('username')
        existing.password = config.get('password')
        existing.connection_string = config.get('connection_string')
        existing.file_path = config.get('file_path')
        existing.auth_source = config.get('auth_source')
        existing.ssl_enabled = config.get('ssl_enabled', False)
        existing.connection_params = config.get('connection_params')
        existing.credentials = config.get('credentials')
        existing.storage_metadata = config.get('storage_metadata')
        existing.is_default = config.get('is_default', False)
        existing.active_flag = config.get('active_flag', True)
        existing.max_connections = config.get('max_connections', 10)
        existing.timeout_seconds = config.get('timeout_seconds', 30)
        existing.description = config.get('description')
        existing.updated_date = config.get('update_date')
        existing.updated_by = config.get('updated_by')

        await self.session.commit()
