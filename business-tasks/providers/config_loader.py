"""Configuration loader for providers - loads from business_tasks_master table."""
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'shared', 'grepx-shared-models', 'src'))

from .provider_config import ProviderConfig, DataDownloadConfig, ProviderType
from business_tasks.database.factory import get_database
from grepx_models import BusinessTasksMaster, StorageMaster

logger = logging.getLogger(__name__)


def _get_db_config() -> dict:
    """Get database configuration from storage_master or use default."""
    try:
        default_config = {
            'type': 'mongodb',
            'connection_string': 'mongodb://admin:password123@localhost:27017/stock_analysis?authSource=admin',
            'database_name': 'stock_analysis'
        }
        
        db = get_database(default_config)
        db.connect()
        
        try:
            storage_data = db.find_one('storage_master', {'is_default': True, 'active_flag': True})
            
            if storage_data:
                logger.info("Using storage configuration from storage_master")
                return {
                    'type': storage_data.get('storage_type'),
                    'connection_string': storage_data.get('connection_string'),
                    'database_name': storage_data.get('database_name'),
                    'host': storage_data.get('host'),
                    'port': storage_data.get('port'),
                    'username': storage_data.get('username'),
                    'password': storage_data.get('password'),
                    'auth_source': storage_data.get('auth_source'),
                }
        finally:
            db.disconnect()
    except Exception as e:
        logger.warning(f"Could not load storage config from database: {e}")
    
    return {
        'type': 'mongodb',
        'connection_string': 'mongodb://admin:password123@localhost:27017/stock_analysis?authSource=admin',
        'database_name': 'stock_analysis'
    }


def get_storage_config(storage_name: str = None) -> dict:
    """Get storage configuration from storage_master table."""
    try:
        db = get_database(_get_db_config())
        db.connect()
        
        try:
            if storage_name:
                storage_data = db.find_one('storage_master', {'storage_name': storage_name, 'active_flag': True})
            else:
                storage_data = db.find_one('storage_master', {'is_default': True, 'active_flag': True})
            
            if storage_data:
                logger.info(f"Loaded storage config: {storage_data.get('storage_name')}")
                return {
                    'type': storage_data.get('storage_type'),
                    'connection_string': storage_data.get('connection_string'),
                    'database_name': storage_data.get('database_name'),
                    'host': storage_data.get('host'),
                    'port': storage_data.get('port'),
                    'username': storage_data.get('username'),
                    'password': storage_data.get('password'),
                    'file_path': storage_data.get('file_path'),
                    'auth_source': storage_data.get('auth_source'),
                    'ssl': storage_data.get('ssl_enabled'),
                }
        finally:
            db.disconnect()
    except Exception as e:
        logger.error(f"Could not load storage config: {e}")
    
    return _get_db_config()


def load_provider_config() -> ProviderConfig:
    """Load provider configuration from business_tasks_master table."""
    try:
        db = get_database(_get_db_config())
        db.connect()
        
        try:
            task_data = db.find_one('business_tasks_master', {'task_name': 'default_provider_config'})
            
            if task_data and task_data.get('task_metadata'):
                logger.info("Loaded provider config from business_tasks_master")
                metadata = task_data['task_metadata']
                return ProviderConfig(
                    provider_type=metadata.get('provider_type', ProviderType.YAHOO.value),
                    interval=metadata.get('interval', '1d'),
                    extra_params=metadata.get('extra_params', {})
                )
        finally:
            db.disconnect()
    except Exception as e:
        logger.warning(f"Could not load provider config from database: {e}")
    
    logger.info("Using default provider config")
    return ProviderConfig(
        provider_type=ProviderType.YAHOO.value,
        interval="1d",
        extra_params={}
    )


def load_task_config(task_name: str) -> dict:
    """Load task configuration from business_tasks_master table."""
    try:
        db = get_database(_get_db_config())
        db.connect()
        
        try:
            task_data = db.find_one('business_tasks_master', {'task_name': task_name, 'active_flag': True})
            
            if task_data:
                logger.info(f"Loaded task config for {task_name}")
                return task_data
            else:
                logger.warning(f"Task {task_name} not found in business_tasks_master")
                return None
        finally:
            db.disconnect()
    except Exception as e:
        logger.error(f"Could not load task config for {task_name}: {e}")
        return None


def save_task_config(task_config: dict) -> bool:
    """Save or update task configuration in business_tasks_master table."""
    try:
        db = get_database(_get_db_config())
        db.connect()
        
        try:
            task_name = task_config.get('task_name')
            if not task_name:
                logger.error("task_name is required in task_config")
                return False
            
            existing = db.find_one('business_tasks_master', {'task_name': task_name})
            
            if existing:
                db.update_one('business_tasks_master', 
                             {'task_name': task_name}, 
                             {'$set': task_config})
                logger.info(f"Updated task config for {task_name}")
            else:
                db.insert_one('business_tasks_master', task_config)
                logger.info(f"Created task config for {task_name}")
            
            return True
        finally:
            db.disconnect()
    except Exception as e:
        logger.error(f"Could not save task config: {e}")
        return False


def get_all_active_tasks() -> list:
    """Get all active tasks from business_tasks_master table."""
    try:
        db = get_database(_get_db_config())
        db.connect()
        
        try:
            tasks = db.find_many('business_tasks_master', {'active_flag': True})
            logger.info(f"Loaded {len(tasks)} active tasks")
            return tasks
        finally:
            db.disconnect()
    except Exception as e:
        logger.error(f"Could not load active tasks: {e}")
        return []

