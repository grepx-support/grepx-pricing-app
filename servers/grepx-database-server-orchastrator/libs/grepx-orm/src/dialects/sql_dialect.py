"""SQL dialect for relational databases"""

from typing import List, Tuple, Any, Type
from core import Model
from core import Query
from core import FieldType


class SQLDialect:
    """SQL dialect implementation for relational databases"""

    def create_table_sql(self, model_class: Type[Model]) -> str:
        """Generate CREATE TABLE SQL statement"""
        table_name = model_class.get_table_name()
        columns = []

        for field_name, field in model_class._fields.items():
            column_def = self._get_column_definition(field_name, field)
            columns.append(column_def)

        # Add primary key constraint if any
        pk_fields = [name for name, field in model_class._fields.items() if field.primary_key]
        if pk_fields:
            columns.append(f"PRIMARY KEY ({', '.join(pk_fields)})")

        return f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"

    def _get_column_definition(self, field_name: str, field) -> str:
        """Get SQL column definition for a field"""
        type_mapping = {
            FieldType.INTEGER: "INTEGER",
            FieldType.STRING: f"VARCHAR({getattr(field, 'max_length', 255)})",
            FieldType.TEXT: "TEXT",
            FieldType.BOOLEAN: "BOOLEAN",
            FieldType.FLOAT: "REAL",
            FieldType.DATETIME: "TIMESTAMP",
            FieldType.JSON: "JSON"
        }

        column_parts = [field_name, type_mapping[field.field_type]]

        if not field.nullable:
            column_parts.append("NOT NULL")
        if field.unique:
            column_parts.append("UNIQUE")
        if field.default is not None:
            column_parts.append(f"DEFAULT {self._format_default(field.default)}")

        return " ".join(column_parts)

    def _format_default(self, default: Any) -> str:
        """Format default value for SQL"""
        if isinstance(default, str):
            return f"'{default}'"
        return str(default)

    def drop_table_sql(self, model_class: Type[Model]) -> str:
        """Generate DROP TABLE SQL statement"""
        return f"DROP TABLE IF EXISTS {model_class.get_table_name()}"

    def insert_sql(self, model: Model) -> Tuple[str, List[Any]]:
        """Generate INSERT SQL statement"""
        table_name = model.get_table_name()
        data = model.to_dict()

        columns = list(data.keys())
        placeholders = ['?' for _ in columns]  # Use ? for parameterized queries
        values = list(data.values())

        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        return sql, values

    def update_sql(self, model: Model) -> Tuple[str, List[Any]]:
        """Generate UPDATE SQL statement"""
        table_name = model.get_table_name()
        data = model.to_dict()

        # Find primary key
        pk_field = next((name for name, field in model._fields.items() if field.primary_key), None)

        if not pk_field or pk_field not in data:
            raise ValueError("Model must have a primary key for updates")

        pk_value = data[pk_field]
        del data[pk_field]

        set_clause = ', '.join([f"{key} = ?" for key in data.keys()])
        sql = f"UPDATE {table_name} SET {set_clause} WHERE {pk_field} = ?"
        values = list(data.values()) + [pk_value]

        return sql, values

    def delete_sql(self, model: Model) -> Tuple[str, List[Any]]:
        """Generate DELETE SQL statement"""
        table_name = model.get_table_name()
        data = model.to_dict()

        # Find primary key
        pk_field = next((name for name, field in model._fields.items() if field.primary_key), None)

        if not pk_field or pk_field not in data:
            raise ValueError("Model must have a primary key for deletion")

        sql = f"DELETE FROM {table_name} WHERE {pk_field} = ?"
        return sql, [data[pk_field]]

    def select_sql(self, query: Query) -> Tuple[str, List[Any]]:
        """Generate SELECT SQL statement"""
        table_name = query.model_class.get_table_name()
        sql_parts = [f"SELECT * FROM {table_name}"]
        params = []

        # WHERE clause
        if query._filters:
            where_parts = []
            for field, operator, value in query._filters:
                where_parts.append(f"{field} {operator} ?")
                params.append(value)
            sql_parts.append(f"WHERE {' AND '.join(where_parts)}")

        # ORDER BY clause
        if query._order_by:
            sql_parts.append(f"ORDER BY {', '.join(query._order_by)}")

        # LIMIT and OFFSET
        if query._limit is not None:
            sql_parts.append(f"LIMIT {query._limit}")
        if query._offset is not None:
            sql_parts.append(f"OFFSET {query._offset}")

        return " ".join(sql_parts), params

    def count_sql(self, query: Query) -> Tuple[str, List[Any]]:
        """Generate COUNT SQL statement"""
        table_name = query.model_class.get_table_name()
        sql_parts = [f"SELECT COUNT(*) FROM {table_name}"]
        params = []

        if query._filters:
            where_parts = []
            for field, operator, value in query._filters:
                where_parts.append(f"{field} {operator} ?")
                params.append(value)
            sql_parts.append(f"WHERE {' AND '.join(where_parts)}")

        return " ".join(sql_parts), params
