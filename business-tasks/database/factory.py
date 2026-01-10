"""Database factory for creating database adapters."""
from typing import Dict, Any
from database.adapter import DatabaseAdapter
from database.csv_adapter import CSVAdapter
from database.duckdb_adapter import DuckDBAdapter


def get_database(config: Dict[str, Any]) -> DatabaseAdapter:
    """
    Factory function to create database adapter based on configuration.
    
    Args:
        config: Database configuration dictionary
            - type: 'csv', 'duckdb', or 'mongodb'
            - For CSV: storage_path
            - For DuckDB: database_path
            - For MongoDB: connection_string, database_name
    
    Returns:
        DatabaseAdapter instance
    
    Raises:
        ValueError: If database type is not supported
    """
    db_type = config.get('type', 'csv').lower()
    
    if db_type == 'csv':
        storage_path = config.get('storage_path', 'storage')
        return CSVAdapter(storage_path)
    elif db_type == 'duckdb':
        database_path = config.get('database_path', 'pricing_data.duckdb')
        return DuckDBAdapter(database_path)
    elif db_type == 'mongodb':
        try:
            from database.mongo_adapter import MongoAdapter
            return MongoAdapter(
                config['connection_string'],
                config['database_name']
            )
        except ImportError:
            raise ImportError("pymongo is required for MongoDB adapter")
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

