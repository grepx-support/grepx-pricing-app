"""PostgreSQL backend implementation"""

from typing import List, Optional, Any, Dict, Type

import asyncpg
from .base import DatabaseBackend
from .registry import register_backend
from core.base import Model
from core.query import Query
from dialects.sql_dialect import SQLDialect


@register_backend('postgresql')
@register_backend('postgres')  # Alias
class PostgreSQLBackend(DatabaseBackend):
    """PostgreSQL database backend"""

    def __init__(self):
        if asyncpg is None:
            raise ImportError(
                "asyncpg is required for PostgreSQL backend. "
                "Install it with: pip install asyncpg"
            )
        self.connection: Optional[asyncpg.Connection] = None
        self._dialect = SQLDialect()

    @property
    def backend_name(self) -> str:
        return "postgresql"

    @property
    def dialect(self):
        """Return the dialect for this backend"""
        return self._dialect

    async def connect(self, **connection_params) -> None:
        """Connect to PostgreSQL database"""
        # Check if connection_string is provided, otherwise use individual parameters
        if 'connection_string' in connection_params:
            # Use connection string directly
            conn_str = connection_params['connection_string']
            self.connection = await asyncpg.connect(conn_str)
        else:
            # Use individual parameters (safer for special characters in passwords)
            # Extract asyncpg-compatible parameters
            asyncpg_params = {}
            if connection_params.get('host'):
                asyncpg_params['host'] = connection_params['host']
            if connection_params.get('port'):
                asyncpg_params['port'] = connection_params['port']
            if connection_params.get('username'):
                asyncpg_params['user'] = connection_params['username']
            if connection_params.get('password'):
                asyncpg_params['password'] = connection_params['password']
            if connection_params.get('database'):
                asyncpg_params['database'] = connection_params['database']

            # Connect using keyword arguments (asyncpg handles this safely)
            self.connection = await asyncpg.connect(**asyncpg_params)

    def _build_connection_string(self, params: Dict[str, Any]) -> str:
        """Build PostgreSQL connection string from parameters"""
        parts = []
        if params.get('username'):
            parts.append(f"user={params['username']}")
        if params.get('password'):
            parts.append(f"password={params['password']}")
        if params.get('host'):
            parts.append(f"host={params['host']}")
        if params.get('port'):
            parts.append(f"port={params['port']}")
        if params.get('database'):
            parts.append(f"database={params['database']}")

        return ' '.join(parts)

    async def disconnect(self) -> None:
        """Disconnect from PostgreSQL database"""
        if self.connection:
            await self.connection.close()

    async def create_table(self, model_class: Type[Model]) -> None:
        """Create a table for the model"""
        sql = self.dialect.create_table_sql(model_class)
        await self.connection.execute(sql)

    async def drop_table(self, model_class: Type[Model]) -> None:
        """Drop a table for the model"""
        sql = self.dialect.drop_table_sql(model_class)
        await self.connection.execute(sql)

    async def insert(self, model: Model) -> Any:
        """Insert a model instance"""
        sql, params = self.dialect.insert_sql(model)
        # Replace ? with $1, $2, etc. for PostgreSQL
        sql = self._convert_placeholders(sql, len(params))
        result = await self.connection.fetchrow(sql, *params)

        # Get the primary key value
        pk_field = next((name for name, field in model._fields.items() if field.primary_key), None)
        if pk_field and result:
            return result[pk_field]
        return None

    def _convert_placeholders(self, sql: str, param_count: int) -> str:
        """Convert ? placeholders to PostgreSQL $1, $2, etc."""
        parts = sql.split('?')
        result = []
        for i, part in enumerate(parts):
            result.append(part)
            if i < len(parts) - 1:
                result.append(f'${i + 1}')
        return ''.join(result)

    async def update(self, model: Model) -> None:
        """Update a model instance"""
        sql, params = self.dialect.update_sql(model)
        sql = self._convert_placeholders(sql, len(params))
        await self.connection.execute(sql, *params)

    async def delete(self, model: Model) -> None:
        """Delete a model instance"""
        sql, params = self.dialect.delete_sql(model)
        sql = self._convert_placeholders(sql, len(params))
        await self.connection.execute(sql, *params)

    async def query_all(self, query: Query) -> List[Model]:
        """Execute query and return all results"""
        sql, params = self.dialect.select_sql(query)
        sql = self._convert_placeholders(sql, len(params))
        rows = await self.connection.fetch(sql, *params)

        results = []
        for row in rows:
            data = dict(row)
            results.append(query.model_class(**data))

        return results

    async def query_first(self, query: Query) -> Optional[Model]:
        """Execute query and return first result"""
        query._limit = 1
        results = await self.query_all(query)
        return results[0] if results else None

    async def query_count(self, query: Query) -> int:
        """Execute query and return count"""
        sql, params = self.dialect.count_sql(query)
        sql = self._convert_placeholders(sql, len(params))
        result = await self.connection.fetchval(sql, *params)
        return result or 0

    async def upsert(self, model: Model, filter_fields: dict) -> bool:
        """
        Upsert a single record in PostgreSQL using native INSERT...ON CONFLICT...UPDATE.

        This is atomic and much more efficient than query-then-insert/update pattern.

        Args:
            model: Model instance to upsert
            filter_fields: Dict of fields to match for upsert (e.g., {'symbol': 'AAPL'})

        Returns:
            True if successfully upserted, False otherwise
        """
        try:
            table_name = model.get_table_name()
            data = model.to_dict()
            columns = list(data.keys())
            values = list(data.values())

            # Quote table name and column names to handle special characters
            quoted_table = f'"{table_name}"'
            quoted_columns = [f'"{col}"' for col in columns]
            quoted_conflict_keys = [f'"{k}"' for k in filter_fields.keys()]

            # Build column list with placeholders
            placeholders = [f"${i+1}" for i in range(len(columns))]
            conflict_keys = ', '.join(quoted_conflict_keys)
            update_set = ', '.join([f'"{k}" = EXCLUDED."{k}"' for k in columns if k not in filter_fields])

            # PostgreSQL upsert syntax
            sql = f"""
                INSERT INTO {quoted_table} ({', '.join(quoted_columns)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT ({conflict_keys})
                DO UPDATE SET {update_set}
            """

            await self.connection.execute(sql, *values)
            return True

        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"PostgreSQL upsert failed: {e}")
            return False

    async def bulk_upsert(self, model_class: Type[Model], records: List[dict], key_fields: List[str], batch_size: int = 100) -> int:
        """
        Bulk upsert multiple records in PostgreSQL using INSERT...ON CONFLICT...UPDATE.

        Much more efficient than individual upsert calls.

        Args:
            model_class: Model class for the records
            records: List of record dictionaries to upsert
            key_fields: Fields used for matching existing records
            batch_size: Number of records to process in each batch

        Returns:
            Total number of records successfully upserted
        """
        if not records:
            return 0

        try:
            table_name = model_class.get_table_name()
            # Quote table name to handle special characters like ^
            quoted_table = f'"{table_name}"'
            # Quote key fields
            quoted_key_fields = [f'"{k}"' for k in key_fields]

            count = 0

            # Process records in batches
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]

                for record in batch:
                    try:
                        columns = list(record.keys())
                        values = list(record.values())

                        # Quote column names to handle special characters
                        quoted_columns = [f'"{col}"' for col in columns]

                        # Build placeholders
                        placeholders = [f"${j+1}" for j in range(len(columns))]
                        conflict_keys = ', '.join(quoted_key_fields)
                        update_set = ', '.join([f'"{k}" = EXCLUDED."{k}"' for k in columns if k not in key_fields])

                        # PostgreSQL upsert syntax
                        sql = f"""
                            INSERT INTO {quoted_table} ({', '.join(quoted_columns)})
                            VALUES ({', '.join(placeholders)})
                            ON CONFLICT ({conflict_keys})
                            DO UPDATE SET {update_set}
                        """

                        await self.connection.execute(sql, *values)
                        count += 1

                    except Exception as e:
                        import logging
                        logger = logging.getLogger(__name__)
                        logger.error(f"PostgreSQL upsert failed for record {record}: {e}")
                        # Continue with next record

            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"PostgreSQL bulk upsert: {count}/{len(records)} records processed")

            return count

        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"PostgreSQL bulk upsert failed: {e}")
            return 0
