"""
Resource Factory for creating dynamic Dagster resources
"""
from dagster import ConfigurableResource
from typing import Dict, Optional, Any
import pandas as pd
from pathlib import Path
from ..task_client import TaskClient


class DynamicResourceFactory:
    @staticmethod
    def create_resource(resource_config) -> ConfigurableResource:
        """Create a resource instance from database config"""
        # Handle both dict and object access
        if isinstance(resource_config, dict):
            resource_type = resource_config.get('resource_type')
            config = resource_config.get('config') or {}
        else:
            resource_type = resource_config.resource_type
            config = resource_config.config if resource_config.config else {}
        
        # Normalize resource type to lowercase for comparison
        resource_type_lower = resource_type.lower() if resource_type else ''
        
        if resource_type_lower in ['task_client', 'taskclient']:
            class TaskClientResource(ConfigurableResource):
                broker_url: str = "redis://localhost:6379/0"
                backend_url: Optional[str] = None
                task_timeout: Optional[int] = 60
                
                def get_client(self) -> TaskClient:
                    return TaskClient(broker_url=self.broker_url)
            
            return TaskClientResource(**config)
        
        elif resource_type_lower == 'database':
            class DatabaseResource(ConfigurableResource):
                connection_string: str
                
                def query(self, sql: str) -> pd.DataFrame:
                    import sqlite3
                    conn = sqlite3.connect(self.connection_string)
                    df = pd.read_sql_query(sql, conn)
                    conn.close()
                    return df
                
                def execute(self, sql: str):
                    import sqlite3
                    conn = sqlite3.connect(self.connection_string)
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    conn.commit()
                    conn.close()
            
            return DatabaseResource(**config)
        
        elif resource_type_lower == 'api':
            class APIResource(ConfigurableResource):
                base_url: str
                api_key: Optional[str] = None
                
                def get(self, endpoint: str) -> Dict[str, Any]:
                    import requests
                    headers = {}
                    if self.api_key:
                        headers['Authorization'] = f'Bearer {self.api_key}'
                    response = requests.get(f"{self.base_url}/{endpoint}", headers=headers)
                    return response.json()
                
                def post(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
                    import requests
                    headers = {}
                    if self.api_key:
                        headers['Authorization'] = f'Bearer {self.api_key}'
                    response = requests.post(f"{self.base_url}/{endpoint}", json=data, headers=headers)
                    return response.json()
            
            return APIResource(**config)
        
        elif resource_type_lower == 'file_system':
            class FileSystemResource(ConfigurableResource):
                base_path: str
                
                def read_file(self, filename: str) -> str:
                    path = Path(self.base_path) / filename
                    return path.read_text()
                
                def write_file(self, filename: str, content: str):
                    path = Path(self.base_path) / filename
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(content)
                
                def list_files(self) -> list[str]:
                    path = Path(self.base_path)
                    return [f.name for f in path.glob('*') if f.is_file()]
            
            return FileSystemResource(**config)
        
        else:
            # For generic/unknown resource types, just return a simple resource
            # We use dict (lowercase) instead of Dict to avoid type inference issues
            class GenericResource(ConfigurableResource):
                config_data: dict = {}
            
            return GenericResource(config_data=config)

