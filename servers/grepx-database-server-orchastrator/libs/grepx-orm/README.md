# ORM Library

A database-agnostic ORM library with plugin architecture supporting SQL and NoSQL databases.

## Features

- **Database Agnostic**: Works with SQLite, PostgreSQL, MongoDB, and more
- **Plugin Architecture**: Easy to add new database backends
- **Async/Await Support**: Built for modern Python async applications
- **Type Safe**: Full type hints support
- **Zero Core Dependencies**: Only install the database drivers you need

## Installation

```bash
# Core library (no database dependencies)
pip install ormlib

# With specific database support
pip install ormlib[sqlite]
pip install ormlib[postgresql]
pip install ormlib[mongodb]

# Or install all backends
pip install ormlib[all]
```

## Quick Start

### Define a Model

```python
from ormlib import Model, IntegerField, StringField

class User(Model):
    id = IntegerField(primary_key=True)
    name = StringField(max_length=100)
    email = StringField(unique=True)
```

### SQLite Example

```python
import asyncio
from ormlib import Session

async def main():
    async with Session.from_connection_string('sqlite:///mydatabase.db') as session:
        # Create table
        await session.create_table(User)
        
        # Create user
        user = User(id=1, name="John Doe", email="john@example.com")
        await session.add(user)
        
        # Query users
        users = await User.query().filter(name="John Doe").all()
        print(users)

asyncio.run(main())
```

### PostgreSQL Example

```python
async def main():
    async with Session.from_connection_string(
        'postgresql://user:password@localhost/mydb'
    ) as session:
        await session.create_table(User)
        user = User(id=1, name="Jane Doe", email="jane@example.com")
        await session.add(user)
```

### MongoDB Example

```python
async def main():
    async with Session.from_connection_string(
        'mongodb://localhost:27017/mydb'
    ) as session:
        await session.create_table(User)
        user = User(id=1, name="Bob Smith", email="bob@example.com")
        await session.add(user)
```

## Architecture

The library uses a plugin architecture:

- **Core**: Model definitions, query builder, session management
- **Backends**: Database-specific implementations (SQLite, PostgreSQL, MongoDB, etc.)
- **Dialects**: SQL and NoSQL query generation
- **Registry**: Dynamic backend discovery and registration

## Adding Custom Backends

```python
from ormlib.backends import DatabaseBackend, register_backend

@register_backend('customdb')
class CustomDatabaseBackend(DatabaseBackend):
    # Implement all abstract methods
    pass
```

## License

MIT

