"""
StorageMaster Model

This model represents storage and database configurations in the master database.
It stores connection details for all databases/storages used by the system.

Note: This is an independent implementation. While grepx_models package has a similar
model, this keeps the database server self-contained without external dependencies.
"""

import setup_paths
from core import Model, IntegerField, StringField, BooleanField, DateTimeField, JSONField


class StorageMaster(Model):
    """
    StorageMaster model for storing database/storage configurations.
    
    This table acts as a registry of all storage connections available to the system,
    including MongoDB, PostgreSQL, SQLite, CSV files, etc.
    
    Attributes:
        id (int): Primary key, auto-incrementing unique identifier
        storage_name (str): Unique name for this storage configuration (e.g., 'tickers', 'financial_data')
        storage_type (str): Type of storage (mongodb, postgresql, sqlite, csv, redis, etc.)
        host (str): Database host address (for network databases)
        port (int): Database port number (for network databases)
        database_name (str): Name of the database/schema
        username (str): Database username for authentication
        password (str): Database password (should be encrypted in production)
        connection_string (str): Full connection string (if applicable)
        file_path (str): File path for file-based storage (SQLite, CSV, DuckDB)
        auth_source (str): Authentication source database (MongoDB specific)
        ssl_enabled (bool): Whether SSL/TLS is enabled for the connection
        connection_params (JSON): Additional connection parameters as key-value pairs
        credentials (JSON): Additional credentials as JSON (should be encrypted)
        storage_metadata (JSON): Additional metadata about the storage
        is_default (bool): Whether this is the default storage for its type
        active_flag (bool): Whether this storage configuration is active and should be used
        max_connections (int): Maximum number of connections in the connection pool
        timeout_seconds (int): Connection timeout in seconds
        description (str): Human-readable description of this storage's purpose
        created_date (datetime): Timestamp when this record was created
        updated_date (datetime): Timestamp when this record was last updated
        created_by (str): Username/identifier of who created this record
        updated_by (str): Username/identifier of who last updated this record
    
    Usage:
        # Query all active storages
        storages = await StorageMaster.query().filter(active_flag=True).all()
        
        # Get a specific storage by name
        storage = await StorageMaster.query().filter(storage_name='tickers').first()
        
        # Get default storage
        default = await StorageMaster.query().filter(is_default=True, active_flag=True).first()
    """
    
    # Primary key
    id = IntegerField(primary_key=True)
    
    # Storage identification
    storage_name = StringField(max_length=255)  # Unique identifier for this storage
    storage_type = StringField(max_length=50)   # mongodb, postgresql, sqlite, etc.
    
    # Connection details (for network databases)
    host = StringField(max_length=255)          # Database host address
    port = IntegerField()                       # Database port number
    database_name = StringField(max_length=255) # Database/schema name
    
    # Authentication
    username = StringField(max_length=255)      # Database username
    password = StringField(max_length=255)      # Database password (encrypt in production!)
    auth_source = StringField(max_length=100)   # Auth database (MongoDB)
    
    # Connection configuration
    connection_string = StringField(max_length=1000)  # Full connection string
    file_path = StringField(max_length=500)           # For file-based storage (SQLite, CSV)
    ssl_enabled = BooleanField()                      # SSL/TLS enabled
    connection_params = JSONField()                   # Additional connection parameters
    credentials = JSONField()                         # Additional credentials (encrypt!)
    
    # Metadata
    storage_metadata = JSONField()              # Additional metadata
    description = StringField(max_length=500)   # Purpose/description
    
    # Status and control flags
    is_default = BooleanField()                 # Is this the default storage?
    active_flag = BooleanField()                # Is this storage active/enabled?
    
    # Connection pool settings
    max_connections = IntegerField()            # Max connections in pool
    timeout_seconds = IntegerField()            # Connection timeout
    
    # Audit fields
    created_date = DateTimeField()              # When created
    updated_date = DateTimeField()              # When last updated
    created_by = StringField(max_length=100)    # Who created
    updated_by = StringField(max_length=100)    # Who last updated

    @classmethod
    def get_table_name(cls) -> str:
        """
        Override table name to match SQLAlchemy convention.
        
        Returns:
            str: The table name 'storage_master'
        """
        return 'storage_master'
