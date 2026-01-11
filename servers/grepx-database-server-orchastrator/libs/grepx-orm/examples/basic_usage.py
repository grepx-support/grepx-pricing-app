"""Basic usage examples for the ORM library"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core import Model, IntegerField, StringField, Session


class User(Model):
    id = IntegerField(primary_key=True)
    name = StringField(max_length=100)
    email = StringField(unique=True, max_length=255)


async def sqlite_example():
    """Example using SQLite"""
    async with Session.from_connection_string('sqlite:///example.db') as session:
        # Create table
        await session.create_table(User)

        # Create users
        user1 = User(id=1, name="Alice", email="alice@example.com")
        user2 = User(id=2, name="Bob", email="bob@example.com")

        await session.add(user1)
        await session.add(user2)

        # Query users
        users = await User.query().all()
        print(f"All users: {[u.to_dict() for u in users]}")

        # Filter query
        alice = await User.query().filter(name="Alice").first()
        print(f"Found Alice: {alice.to_dict() if alice else None}")

        # Update
        if alice:
            alice.name = "Alice Smith"
            await session.update(alice)

        # Count
        count = await User.query().count()
        print(f"Total users: {count}")


async def postgresql_example():
    """Example using PostgreSQL"""
    try:
        async with Session.from_connection_string(
                'postgresql://user:password@localhost/mydb'
        ) as session:
            await session.create_table(User)
            user = User(id=1, name="Charlie", email="charlie@example.com")
            await session.add(user)
            print("PostgreSQL example completed")
    except Exception as e:
        print(f"PostgreSQL example failed (database may not be available): {e}")


async def mongodb_example():
    """Example using MongoDB"""
    try:
        async with Session.from_connection_string(
                'mongodb://localhost:27017/mydb'
        ) as session:
            await session.create_table(User)
            user = User(id=1, name="Diana", email="diana@example.com")
            await session.add(user)
            print("MongoDB example completed")
    except Exception as e:
        print(f"MongoDB example failed (database may not be available): {e}")


if __name__ == "__main__":
    print("Running SQLite example...")
    asyncio.run(sqlite_example())

    print("\nRunning PostgreSQL example...")
    asyncio.run(postgresql_example())

    print("\nRunning MongoDB example...")
    asyncio.run(mongodb_example())
