"""Financial data providers for downloading stock market data."""

from .base_provider import BaseProvider
from .yahoo_provider import YahooProvider
# Lazy import JugadProvider to avoid aiohttp dependency
# from .jugad_provider import JugadProvider
from .custom_provider import CustomProvider
from .yahoo_provider_balance_sheet import YahooProviderBalanceSheet
from .provider_factory import ProviderFactory
# Removed storage import (database.factory doesn't exist)
# from .storage import download_and_store, download_and_store_multiple
from .provider_config import ProviderType, ProviderConfig, DataDownloadConfig
# Lazy import config_loader to avoid database.factory dependency
# from .config_loader import (
#     load_provider_config,
#     load_task_config,
#     save_task_config,
#     get_all_active_tasks,
#     get_storage_config
# )

# Functions will be imported lazily when actually needed
def load_provider_config():
    """Lazy load provider config."""
    from .config_loader import load_provider_config as _load
    return _load()

def load_task_config(task_name: str):
    """Lazy load task config."""
    from .config_loader import load_task_config as _load
    return _load(task_name)

def save_task_config(task_config: dict):
    """Lazy save task config."""
    from .config_loader import save_task_config as _save
    return _save(task_config)

def get_all_active_tasks():
    """Lazy get all active tasks."""
    from .config_loader import get_all_active_tasks as _get
    return _get()

def get_storage_config(storage_name: str = None):
    """Lazy get storage config."""
    from .config_loader import get_storage_config as _get
    return _get(storage_name)

__all__ = [
    'BaseProvider',
    'YahooProvider',
    'JugadProvider',
    'CustomProvider',
    'YahooProviderBalanceSheet',
    'ProviderFactory',
    'download_and_store',
    'download_and_store_multiple',
    'ProviderType',
    'ProviderConfig',
    'DataDownloadConfig',
    'load_provider_config',
    'load_task_config',
    'save_task_config',
    'get_all_active_tasks',
    'get_storage_config',
]

