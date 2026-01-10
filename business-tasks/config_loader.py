"""Configuration loader for business tasks - loads from database."""
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared', 'grepx-shared-models', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from grepx_models import ProviderConfig, DataDownloadConfig, ProviderType
from database.factory import get_database

logger = logging.getLogger(__name__)


def load_provider_config() -> ProviderConfig:
    """
    Load provider configuration from database.
    Falls back to default if not found in database.
    
    Returns:
        ProviderConfig instance
    """
    try:
        db = get_database({'type': 'mongodb', 
                          'connection_string': 'mongodb://admin:password123@localhost:27017/stock_analysis?authSource=admin',
                          'database_name': 'stock_analysis'})
        db.connect()
        
        try:
            config_data = db.find_one('provider_config', {})
            
            if config_data:
                logger.info("Loaded provider config from database")
                return ProviderConfig(
                    provider_type=config_data.get('provider_type', ProviderType.YAHOO.value),
                    interval=config_data.get('interval', '1d'),
                    extra_params=config_data.get('extra_params', {})
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


def load_data_download_config() -> DataDownloadConfig:
    """
    Load data download configuration from database.
    Falls back to default if not found in database.
    
    Returns:
        DataDownloadConfig instance
    """
    try:
        db = get_database({'type': 'mongodb', 
                          'connection_string': 'mongodb://admin:password123@localhost:27017/stock_analysis?authSource=admin',
                          'database_name': 'stock_analysis'})
        db.connect()
        
        try:
            config_data = db.find_one('data_download_config', {})
            
            if config_data:
                logger.info("Loaded data download config from database")
                
                provider_data = config_data.get('provider', {})
                provider_config = ProviderConfig(
                    provider_type=provider_data.get('provider_type', ProviderType.YAHOO.value),
                    interval=provider_data.get('interval', '1d'),
                    extra_params=provider_data.get('extra_params', {})
                )
                
                return DataDownloadConfig(
                    provider=provider_config,
                    default_period=config_data.get('default_period', '1y'),
                    default_tickers=config_data.get('default_tickers', ['AAPL', 'MSFT', 'GOOG']),
                    start_date=config_data.get('start_date'),
                    end_date=config_data.get('end_date'),
                    batch_size=config_data.get('batch_size', 10),
                    retry_attempts=config_data.get('retry_attempts', 3),
                    timeout_seconds=config_data.get('timeout_seconds', 300)
                )
        finally:
            db.disconnect()
    except Exception as e:
        logger.warning(f"Could not load data download config from database: {e}")
    
    logger.info("Using default data download config")
    return DataDownloadConfig(
        provider=ProviderConfig(provider_type=ProviderType.YAHOO.value, interval="1d"),
        default_period='1y',
        default_tickers=['AAPL', 'MSFT', 'GOOG'],
        batch_size=10,
        retry_attempts=3,
        timeout_seconds=300
    )


def save_provider_config(config: ProviderConfig) -> bool:
    """
    Save provider configuration to database.
    
    Args:
        config: ProviderConfig instance to save
        
    Returns:
        True if successful, False otherwise
    """
    try:
        db = get_database({'type': 'mongodb', 
                          'connection_string': 'mongodb://admin:password123@localhost:27017/stock_analysis?authSource=admin',
                          'database_name': 'stock_analysis'})
        db.connect()
        
        try:
            config_data = {
                'provider_type': config.provider_type,
                'interval': config.interval,
                'extra_params': config.extra_params
            }
            
            db.delete_many('provider_config', {})
            db.insert_one('provider_config', config_data)
            logger.info("Saved provider config to database")
            return True
        finally:
            db.disconnect()
    except Exception as e:
        logger.error(f"Could not save provider config to database: {e}")
        return False


def save_data_download_config(config: DataDownloadConfig) -> bool:
    """
    Save data download configuration to database.
    
    Args:
        config: DataDownloadConfig instance to save
        
    Returns:
        True if successful, False otherwise
    """
    try:
        db = get_database({'type': 'mongodb', 
                          'connection_string': 'mongodb://admin:password123@localhost:27017/stock_analysis?authSource=admin',
                          'database_name': 'stock_analysis'})
        db.connect()
        
        try:
            config_data = {
                'provider': {
                    'provider_type': config.provider.provider_type,
                    'interval': config.provider.interval,
                    'extra_params': config.provider.extra_params
                },
                'default_period': config.default_period,
                'default_tickers': config.default_tickers,
                'start_date': config.start_date,
                'end_date': config.end_date,
                'batch_size': config.batch_size,
                'retry_attempts': config.retry_attempts,
                'timeout_seconds': config.timeout_seconds
            }
            
            db.delete_many('data_download_config', {})
            db.insert_one('data_download_config', config_data)
            logger.info("Saved data download config to database")
            return True
        finally:
            db.disconnect()
    except Exception as e:
        logger.error(f"Could not save data download config to database: {e}")
        return False

