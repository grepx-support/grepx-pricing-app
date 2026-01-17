"""
File Storage Handler - Handles file-based databases (SQLite, DuckDB)

Single Responsibility: Setup and validate file-based storage connections
"""

import logging
from pathlib import Path
from typing import Dict
from .path_resolver import PathResolver


class FileStorageHandler:
    """Handles file-based storage setup (SQLite, DuckDB, etc.)"""
    
    def __init__(self, path_resolver: PathResolver):
        self.path_resolver = path_resolver
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def prepare_connection_params(
        self, 
        storage_name: str, 
        connection_params: Dict
    ) -> None:
        """
        Prepare connection parameters for file-based storage
        
        Resolves paths, creates directories, updates connection string
        
        Args:
            storage_name: Name of storage
            connection_params: Connection parameters (modified in-place)
        """
        # Extract file path
        file_path_str = self._extract_file_path(connection_params)
        
        if not file_path_str:
            self.logger.warning(f"No file path for {storage_name}")
            return
        
        self.logger.debug(f"Original path: {file_path_str}")
        
        # Resolve to absolute path
        file_path = self.path_resolver.resolve_path(file_path_str)
        self.logger.info(f"Resolved: {file_path_str} -> {file_path}")
        
        # Create directory
        self.path_resolver.ensure_directory_exists(file_path)
        
        # Check file status
        self.path_resolver.check_file_exists(file_path)
        
        # Update connection params with normalized paths
        self._update_connection_params(connection_params, file_path)
    
    def _extract_file_path(self, params: Dict) -> str:
        """Extract file path from connection params"""
        
        # Check connection_string first
        if 'connection_string' in params:
            conn_str = params['connection_string']
            
            # Extract from sqlite:/// format
            if conn_str.startswith('sqlite:///'):
                return conn_str[10:]  # Remove 'sqlite:///'
        
        # Check file_path parameter
        if 'file_path' in params:
            return params['file_path']
        
        return None
    
    def _update_connection_params(self, params: Dict, file_path: Path) -> None:
        """Update connection params with resolved paths"""
        
        # Convert to POSIX for SQLite (forward slashes)
        file_path_posix = self.path_resolver.to_posix(file_path)
        
        # Update connection_string
        if 'connection_string' in params:
            params['connection_string'] = f"sqlite:///{file_path_posix}"
            self.logger.info(f"Connection string: sqlite:///{file_path_posix}")
        
        # Update file_path
        if 'file_path' in params:
            params['file_path'] = str(file_path)
            self.logger.debug(f"File path: {file_path}")
