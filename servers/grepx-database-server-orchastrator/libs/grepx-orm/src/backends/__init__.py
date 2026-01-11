"""Database backends and registry system"""

from .base import DatabaseBackend
from .registry import (
    BackendRegistry, registry,
    register_backend, get_backend_class, list_available_backends
)
from .discovery import discover_backends

# IMPORT BACKEND MODULES - This makes the @register_backend decorators execute!
# The decorators run when the modules are imported, registering the backends

from . import sqlite
from . import postgresql
from . import mongodb

__all__ = [
    'DatabaseBackend',
    'BackendRegistry', 'registry',
    'register_backend', 'get_backend_class', 'list_available_backends',
    'discover_backends'
]
