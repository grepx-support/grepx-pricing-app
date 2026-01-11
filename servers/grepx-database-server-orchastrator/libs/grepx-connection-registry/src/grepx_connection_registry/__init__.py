"""Connection Registry Library.

A flexible connection management library with registry pattern.
"""

from .connection_protocol import Connection
from .connection_base import ConnectionBase
from .connection_factory import ConnectionFactory
from .connection_registry import ConnectionRegistry
from .connection_manager import ConnectionManager

__all__ = [
    "Connection",
    "ConnectionBase",
    "ConnectionFactory",
    "ConnectionRegistry",
    "ConnectionManager",
]

__version__ = "1.0.0"

