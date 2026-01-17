"""
Network Storage Handler - Handles network-based databases (MongoDB, PostgreSQL)

Single Responsibility: Validate network storage connection parameters
"""

import logging
from typing import Dict


class NetworkStorageHandler:
    """Handles network-based storage setup (MongoDB, PostgreSQL, etc.)"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def prepare_connection_params(
        self, 
        storage_name: str, 
        connection_params: Dict
    ) -> None:
        """
        Prepare connection parameters for network-based storage
        
        Validates required parameters
        
        Args:
            storage_name: Name of storage
            connection_params: Connection parameters (checked, not modified)
        """
        # For network databases, validate connection info exists
        has_connection_string = 'connection_string' in connection_params
        has_host = 'host' in connection_params
        
        if not has_connection_string and not has_host:
            self.logger.warning(
                f"{storage_name}: No connection_string or host found. "
                "Connection may fail."
            )
        
        # Log connection details (safe version)
        if has_connection_string:
            self.logger.debug(f"{storage_name}: Using connection_string")
        elif has_host:
            host = connection_params.get('host')
            port = connection_params.get('port', 'default')
            database = connection_params.get('database', 'N/A')
            self.logger.info(f"{storage_name}: {host}:{port}/{database}")
