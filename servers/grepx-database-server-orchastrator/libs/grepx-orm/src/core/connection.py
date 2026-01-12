"""Connection string parsing and backend factory"""

from urllib.parse import urlparse, parse_qs
from typing import Dict, Any, Tuple

from backends.discovery import discover_backends
from backends.registry import get_backend_class


def parse_connection_string(connection_string: str) -> Tuple[str, Dict[str, Any]]:
    """
    Parse connection string and extract backend type and parameters

    Supports:
    - sqlite:///path/to/database.db
    - postgresql://user:pass@host:port/database
    - mongodb://user:pass@host:port/database
    - mongodb+srv://user:pass@host/database (MongoDB Atlas)
    - mysql://user:pass@host:port/database
    """
    parsed = urlparse(connection_string)

    # Extract backend type from scheme
    backend_type = parsed.scheme.lower()
    # Normalize MongoDB Atlas connection strings
    if backend_type == 'mongodb+srv':
        backend_type = 'mongodb'

    # Build connection parameters
    params = {
        'connection_string': connection_string,  # Always preserve the original connection string
        'host': parsed.hostname,
        'port': parsed.port,
        'username': parsed.username,
        'password': parsed.password,
        'database': parsed.path.lstrip('/') if parsed.path else None
    }

    # Parse query parameters
    if parsed.query:
        query_params = parse_qs(parsed.query)
        for key, value in query_params.items():
            params[key] = value[0] if len(value) == 1 else value

    # Special handling for SQLite
    if backend_type == 'sqlite':
        if parsed.netloc:
            params['database'] = f"{parsed.netloc}{parsed.path}"
        else:
            params['database'] = parsed.path

    return backend_type, params


def create_backend_from_connection_string(connection_string: str):
    """
    Create backend instance from connection string
    """
    discover_backends()

    backend_type, params = parse_connection_string(connection_string)

    try:
        backend_class = get_backend_class(backend_type)
        return backend_class(), params
    except ValueError:
        # Try to discover again in case backends were added dynamically
        try:
            discover_backends()
        except Exception:
            pass
        backend_class = get_backend_class(backend_type)
        return backend_class(), params
