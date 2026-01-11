"""MongoDB backend implementation"""

from typing import List, Optional, Any, Dict, Type

from motor.motor_asyncio import AsyncIOMotorClient
from .base import DatabaseBackend
from .registry import register_backend
from core.base import Model
from core.query import Query
from dialects.no_sql_dialect import NoSQLDialect


@register_backend('mongodb')
class MongoDBBackend(DatabaseBackend):
    """MongoDB database backend"""

    def __init__(self):
        if AsyncIOMotorClient is None:
            raise ImportError(
                "motor is required for MongoDB backend. "
                "Install it with: pip install motor"
            )
        self.client: Optional[AsyncIOMotorClient] = None
        self.database = None
        self._dialect = NoSQLDialect()

    @property
    def backend_name(self) -> str:
        return "mongodb"

    @property
    def dialect(self):
        """Return the dialect for this backend"""
        return self._dialect

    async def connect(self, **connection_params) -> None:
        """Connect to MongoDB database"""
        # Use provided connection string or build from parameters
        if 'connection_string' in connection_params:
            conn_str = connection_params['connection_string']
        else:
            conn_str = self._build_connection_string(connection_params)

        self.client = AsyncIOMotorClient(conn_str)
        # Use database_name from connection_params, fallback to 'database' key, or extract from connection string
        database_name = connection_params.get('database_name') or connection_params.get('database')

        # If still no database name, try to extract from connection string
        if not database_name and 'connection_string' in connection_params:
            # Extract database name from connection string (e.g., mongodb://host:port/dbname)
            import re
            match = re.search(r'mongodb://[^/]+/([^?]+)', conn_str)
            if match:
                database_name = match.group(1)

        # Final fallback if nothing found
        if not database_name:
            database_name = 'default_db'

        self.database = self.client[database_name]

    def _build_connection_string(self, params: Dict[str, Any]) -> str:
        """Build MongoDB connection string from parameters"""
        auth_part = ""
        if params.get('username') and params.get('password'):
            auth_part = f"{params['username']}:{params['password']}@"

        host = params.get('host', 'localhost')
        port = params.get('port', 27017)

        return f"mongodb://{auth_part}{host}:{port}/"

    async def disconnect(self) -> None:
        """Disconnect from MongoDB database"""
        if self.client:
            self.client.close()

    async def create_table(self, model_class: Type[Model]) -> None:
        """Create a collection for the model (MongoDB creates collections automatically)"""
        pass

    async def drop_table(self, model_class: Type[Model]) -> None:
        """Drop a collection for the model"""
        collection = self.database[model_class.get_table_name()]
        await collection.drop()

    async def insert(self, model: Model) -> Any:
        """Insert a model instance"""
        collection = self.database[model.get_table_name()]
        data = model.to_dict()

        # Convert to MongoDB format
        mongo_data = self.dialect.to_mongo_format(data)
        result = await collection.insert_one(mongo_data)

        # Store the MongoDB _id
        model._id = result.inserted_id
        return result.inserted_id

    async def update(self, model: Model) -> None:
        """Update a model instance"""
        collection = self.database[model.get_table_name()]
        data = model.to_dict()
        mongo_data = self.dialect.to_mongo_format(data)

        # Use _id for updates in MongoDB
        if hasattr(model, '_id') and model._id:
            await collection.update_one(
                {'_id': model._id},
                {'$set': mongo_data}
            )

    async def delete(self, model: Model) -> None:
        """Delete a model instance"""
        collection = self.database[model.get_table_name()]
        if hasattr(model, '_id') and model._id:
            await collection.delete_one({'_id': model._id})

    async def query_all(self, query: Query) -> List[Model]:
        """Execute query and return all results"""
        collection = self.database[query.model_class.get_table_name()]

        # Convert filters to MongoDB query
        mongo_query = self.dialect.build_mongo_query(query._filters)

        # Build cursor with options
        cursor = collection.find(mongo_query)

        if query._limit:
            cursor = cursor.limit(query._limit)
        if query._offset:
            cursor = cursor.skip(query._offset)
        if query._order_by:
            sort_spec = self.dialect.build_sort_spec(query._order_by)
            cursor = cursor.sort(sort_spec)

        results = []
        async for doc in cursor:
            # Convert from MongoDB format
            data = self.dialect.from_mongo_format(doc)
            results.append(query.model_class(**data))

        return results

    async def query_first(self, query: Query) -> Optional[Model]:
        """Execute query and return first result"""
        query._limit = 1
        results = await self.query_all(query)
        return results[0] if results else None

    async def query_count(self, query: Query) -> int:
        """Execute query and return count"""
        collection = self.database[query.model_class.get_table_name()]
        mongo_query = self.dialect.build_mongo_query(query._filters)
        return await collection.count_documents(mongo_query)

    async def upsert(self, model: Model, filter_fields: dict) -> bool:
        """
        Upsert a single record in MongoDB using native $set operator with upsert=True.

        This is much more efficient than query-then-insert/update pattern as it's
        atomic and avoids race conditions.

        Args:
            model: Model instance to upsert
            filter_fields: Dict of fields to match for upsert (e.g., {'symbol': 'AAPL'})

        Returns:
            True if successfully upserted, False otherwise
        """
        try:
            collection = self.database[model.get_table_name()]
            data = model.to_dict()
            mongo_data = self.dialect.to_mongo_format(data)

            # Build filter from filter_fields
            mongo_filter = {k: v for k, v in filter_fields.items()}

            # Use MongoDB's native upsert with $set operator
            result = await collection.update_one(
                mongo_filter,
                {'$set': mongo_data},
                upsert=True  # Atomic upsert operation
            )

            return True

        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"MongoDB upsert failed: {e}")
            return False

    async def bulk_upsert(self, model_class: Type[Model], records: List[dict], key_fields: List[str], batch_size: int = 100) -> int:
        """
        Bulk upsert multiple records in MongoDB using bulk_write operations.

        Much more efficient than individual upsert calls.

        Args:
            model_class: Model class for the records
            records: List of record dictionaries to upsert
            key_fields: Fields used for matching existing records
            batch_size: Number of records to process in each batch

        Returns:
            Total number of records successfully upserted
        """
        try:
            from pymongo import UpdateOne
        except ImportError:
            import logging
            logger = logging.getLogger(__name__)
            logger.error("pymongo is required for bulk operations")
            return 0

        try:
            collection = self.database[model_class.get_table_name()]
            count = 0

            # Process records in batches
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]

                # Build bulk operations
                operations = []
                for rec in batch:
                    # Build filter from key fields
                    filter_doc = {k: rec[k] for k in key_fields if k in rec}

                    # Convert record data to MongoDB format
                    update_data = self.dialect.to_mongo_format(rec)

                    operations.append(
                        UpdateOne(
                            filter_doc,
                            {'$set': update_data},
                            upsert=True  # Create if not exists
                        )
                    )

                # Execute bulk write
                result = await collection.bulk_write(operations)
                count += result.upserted_count + result.modified_count

            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"MongoDB bulk upsert completed: {count} records processed")

            return count

        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"MongoDB bulk upsert failed: {e}")
            return 0
