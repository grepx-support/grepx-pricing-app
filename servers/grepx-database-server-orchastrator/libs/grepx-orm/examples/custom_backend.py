"""Example of creating a custom database backend"""

from typing import List, Optional, Any, Dict, Type
from ormlib.backends import DatabaseBackend, register_backend
from ormlib.core.base import Model
from ormlib.core.query import Query
from ormlib.dialects.sql import SQLDialect

from src.core import IntegerField, StringField, Session


@register_backend('custom')
class CustomDatabaseBackend(DatabaseBackend):
    """Example custom backend implementation"""

    def __init__(self):
        self.connection = None
        self.dialect = SQLDialect()
        self._storage = {}  # In-memory storage for demo

    @property
    def backend_name(self) -> str:
        return "custom"

    async def connect(self, **connection_params) -> None:
        """Connect to the database"""
        # Implement connection logic
        self._storage = {}

    async def disconnect(self) -> None:
        """Disconnect from the database"""
        self._storage = {}

    async def create_table(self, model_class: Type[Model]) -> None:
        """Create a table/collection"""
        table_name = model_class.get_table_name()
        if table_name not in self._storage:
            self._storage[table_name] = []

    async def drop_table(self, model_class: Type[Model]) -> None:
        """Drop a table/collection"""
        table_name = model_class.get_table_name()
        if table_name in self._storage:
            del self._storage[table_name]

    async def insert(self, model: Model) -> Any:
        """Insert a model instance"""
        table_name = model.get_table_name()
        if table_name not in self._storage:
            self._storage[table_name] = []

        data = model.to_dict()
        self._storage[table_name].append(data)

        # Return primary key if exists
        pk_field = next((name for name, field in model._fields.items() if field.primary_key), None)
        if pk_field:
            return data.get(pk_field)
        return None

    async def update(self, model: Model) -> None:
        """Update a model instance"""
        table_name = model.get_table_name()
        data = model.to_dict()

        pk_field = next((name for name, field in model._fields.items() if field.primary_key), None)
        if not pk_field:
            raise ValueError("Model must have a primary key for updates")

        pk_value = data[pk_field]

        # Find and update the record
        for i, record in enumerate(self._storage.get(table_name, [])):
            if record.get(pk_field) == pk_value:
                self._storage[table_name][i] = data
                break

    async def delete(self, model: Model) -> None:
        """Delete a model instance"""
        table_name = model.get_table_name()
        data = model.to_dict()

        pk_field = next((name for name, field in model._fields.items() if field.primary_key), None)
        if not pk_field:
            raise ValueError("Model must have a primary key for deletion")

        pk_value = data[pk_field]

        # Find and remove the record
        records = self._storage.get(table_name, [])
        self._storage[table_name] = [
            r for r in records if r.get(pk_field) != pk_value
        ]

    async def query_all(self, query: Query) -> List[Model]:
        """Execute query and return all results"""
        table_name = query.model_class.get_table_name()
        records = self._storage.get(table_name, [])

        # Apply filters
        for field, operator, value in query._filters:
            if operator == '=':
                records = [r for r in records if r.get(field) == value]

        # Apply ordering
        if query._order_by:
            # Simple ordering implementation
            for field in reversed(query._order_by):
                reverse = field.startswith('-')
                field_name = field.lstrip('-')
                records.sort(key=lambda x: x.get(field_name, 0), reverse=reverse)

        # Apply limit and offset
        if query._offset:
            records = records[query._offset:]
        if query._limit:
            records = records[:query._limit]

        # Convert to model instances
        results = []
        for record in records:
            results.append(query.model_class(**record))

        return results

    async def query_first(self, query: Query) -> Optional[Model]:
        """Execute query and return first result"""
        query._limit = 1
        results = await self.query_all(query)
        return results[0] if results else None

    async def query_count(self, query: Query) -> int:
        """Execute query and return count"""
        table_name = query.model_class.get_table_name()
        records = self._storage.get(table_name, [])

        # Apply filters
        for field, operator, value in query._filters:
            if operator == '=':
                records = [r for r in records if r.get(field) == value]

        return len(records)


# Usage example
if __name__ == "__main__":
    import asyncio


    class User(Model):
        id = IntegerField(primary_key=True)
        name = StringField()
        email = StringField()


    async def main():
        async with Session.from_backend_name('custom') as session:
            await session.create_table(User)

            user = User(id=1, name="Test User", email="test@example.com")
            await session.add(user)

            users = await User.query().all()
            print(f"Users: {[u.to_dict() for u in users]}")


    asyncio.run(main())
