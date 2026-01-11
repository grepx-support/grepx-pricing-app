# Connection Registry Library

A flexible connection management library with registry pattern for managing different types of connections (database, message queues, APIs, etc.).

## Features

- **Registry Pattern**: Register and manage different connection types dynamically
- **Factory Pattern**: Create connection instances based on configuration
- **Protocol Support**: Type-safe connection interface using Python Protocols
- **Base Classes**: Extensible base class for creating custom connections
- **Connection Caching**: Automatic connection instance caching and reuse
- **Zero Dependencies**: Uses only Python standard library

## Installation

```bash
# Install from local development
pip install -e libs/grepx-connection-registry

# Or add to requirements.txt
-e libs/grepx-connection-registry
```

## Quick Start

### Define a Custom Connection

```python
from grepx_connection_registry import ConnectionBase

class MyCustomConnection(ConnectionBase):
    def connect(self) -> None:
        # Your connection logic
        self._client = your_client()
    
    def disconnect(self) -> None:
        if self._client:
            self._client.close()
        self._client = None
```

### Register Connection Type

```python
from grepx_connection_registry import ConnectionFactory
from my_connections import MyCustomConnection

# Register the connection type
ConnectionFactory.register("my_connection_type", MyCustomConnection)
```

### Use Connection Manager

```python
from omegaconf import OmegaConf
from grepx_connection_registry import ConnectionManager

# Load configuration
config = OmegaConf.load("config.yaml")
config_dir = Path("resources")

# Create connection manager
manager = ConnectionManager(config, config_dir)

# Get a connection
conn = manager.get("primary_db")
client = conn.get_client()
```

## Architecture

- **ConnectionBase**: Abstract base class for all connections
- **ConnectionFactory**: Factory for creating connections by type
- **ConnectionRegistry**: Registry for caching and managing connection instances
- **ConnectionManager**: High-level interface for managing connections
- **Connection (Protocol)**: Type protocol defining the connection interface

## License

MIT

