"""
Lightweight Database Manager for Prefect - Uses Raw SQL Queries
No ORM model dependencies - only reads from database
"""
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from typing import Dict, Any, List, Optional
import json
import os


class DatabaseManager:
    """Lightweight database manager - read-only, uses raw SQL"""

    def __init__(self, db_url: str = "sqlite:///./prefect_config.db"):
        """
        Initialize database manager
    
        Args:
            db_url: Database URL to connect to
        """
        # Normalize the database URL for cross-platform compatibility
        normalized_db_url = self._normalize_db_url(db_url)
            
        self.db_url = normalized_db_url
            
        # Handle the database URL
        engine_url = normalized_db_url
            
        # If this is the master database file, ensure path exists
        if 'grepx-master.db' in normalized_db_url:
            # Extract path part from URL
            if normalized_db_url.startswith('sqlite:///'):
                path_part = normalized_db_url[10:]  # Remove 'sqlite:///`
            elif normalized_db_url.startswith('file:///'):
                path_part = normalized_db_url[8:]  # Remove 'file:///`
            else:
                path_part = normalized_db_url
                
            # Check if the file exists at the given path
            import os.path
            if not os.path.exists(path_part):
                # If the path doesn't exist, try to find the database file in the expected location
                # The expected location is in the project root's data folder
                current_dir = os.getcwd()
                
# Try to find the database file by searching in common locations
                project_root_found = False
                actual_db_path = None
                
                # Get current working directory
                current_dir = os.getcwd()
                
                # Method 1: Search upward from current directory (same drive)
                search_dir = current_dir
                max_levels_up = 15
                
                for i in range(max_levels_up):
                    # Check if this directory contains the expected project structure
                    if (os.path.exists(os.path.join(search_dir, 'data', 'grepx-master.db')) and
                        os.path.exists(os.path.join(search_dir, 'servers', 'grepx-prefect-server'))):
                        # Found the project root
                        project_root_found = True
                        db_path = os.path.join(search_dir, 'data', 'grepx-master.db')
                        if os.path.exists(db_path):
                            actual_db_path = db_path
                        break
                    
                    # Move up one level
                    parent_dir = os.path.dirname(search_dir)
                    if parent_dir == search_dir:  # Reached filesystem root
                        break
                    search_dir = parent_dir
                
                # Method 2: If not found, try common project locations (cross-drive support)
                if not project_root_found:
                    # Common project root locations to check
                    common_locations = [
                        # Standard project location
                        'D:/GrepX/prefect_celery/grepx-pricing-app',
                        # Alternative locations
                        'C:/GrepX/prefect_celery/grepx-pricing-app',
                        # Try to construct from known patterns
                        os.path.join(os.path.expanduser('~'), 'GrepX', 'prefect_celery', 'grepx-pricing-app'),
                    ]
                    
                    for location in common_locations:
                        db_path = os.path.join(location, 'data', 'grepx-master.db')
                        if os.path.exists(db_path):
                            project_root_found = True
                            actual_db_path = db_path
                            break
                
                # If we haven't found the database file yet, try the original search logic
                if not project_root_found:
                    # Original search logic for normal cases
                    possible_paths = [
                        os.path.join(current_dir, '..', '..', '..', '..', 'data', 'grepx-master.db'),  # From servers/grepx-prefect-server/src/main/prefect_app
                        os.path.join(current_dir, '..', '..', '..', 'data', 'grepx-master.db'),  # From servers/grepx-prefect-server/src/main
                        os.path.join(current_dir, '..', '..', 'data', 'grepx-master.db'),     # From servers/grepx-prefect-server
                        os.path.join(current_dir, '..', 'data', 'grepx-master.db'),          # From project root
                    ]
                    
                    for p in possible_paths:
                        abs_p = os.path.abspath(p)
                        if os.path.exists(abs_p):
                            actual_db_path = abs_p
                            break
                    
                if actual_db_path:
                    engine_url = f'sqlite:///{actual_db_path.replace(os.sep, "/")}'
                else:
                    # If we can't find the file, raise an error
                    raise FileNotFoundError(f"Could not locate grepx-master.db database file. Searched: {possible_paths}")
            else:
                # Path exists, use it as is
                engine_url = normalized_db_url
        else:
            # For other URLs, use the normalized version
            engine_url = normalized_db_url
            
        self._engine = create_engine(engine_url, echo=False)
        self._Session = sessionmaker(bind=self._engine)
    
    def _normalize_db_url(self, db_url: str) -> str:
        """Normalize database URL for cross-platform compatibility"""
        if db_url.startswith('sqlite:///'):
            # Extract the path part after sqlite:/// and normalize it
            path_part = db_url[10:]  # Remove 'sqlite:///`
            # Convert backslashes to forward slashes
            normalized_path = path_part.replace('\\', '/').replace('\\', '/')
                    
            # Check if this is a Windows absolute path (e.g., C:/path/to/file)
            # We check if it starts with a drive letter followed by a colon
            is_drive_letter_colon = len(normalized_path) > 1 and normalized_path[1] == ':' and normalized_path[0].isalpha()
            is_drive_with_slash = len(normalized_path) > 2 and normalized_path[0].isalpha() and normalized_path[1] == ':' and normalized_path[2] == '/'
            if is_drive_letter_colon or is_drive_with_slash:
                # For Windows absolute paths, use the file:// format
                # Also ensure the path exists
                if os.path.exists(normalized_path):
                    return f'file:///{normalized_path}'
                else:
                    # If the path doesn't exist as-is, it might have extra prefixes
                    # Try to clean the path
                    clean_path = normalized_path
                    if clean_path.startswith('/'):
                        clean_path = clean_path[1:]
                    if os.path.exists(clean_path):
                        return f'file:///{clean_path}'
                    else:
                        # Path doesn't exist, return as is and let the path resolver handle it
                        return f'file:///{normalized_path}'
            else:
                # For relative paths or Unix paths, use the sqlite:// format
                return f'sqlite:///{normalized_path}'
        return db_url

    def get_session(self):
        """Get a new database session"""
        return self._Session()

    def _row_to_dict(self, row) -> Dict[str, Any]:
        """Convert SQLAlchemy row to dictionary"""
        return dict(row._mapping)

    def _parse_json_field(self, value):
        """Parse JSON field from database"""
        if value is None:
            return {}
        if isinstance(value, str):
            try:
                return json.loads(value)
            except:
                return {}
        return value

    def get_flows(self) -> List[Dict[str, Any]]:
        """Get all active Prefect flows - returns list of dicts"""
        with self.get_session() as session:
            result = session.execute(text("""
                SELECT id, name, description, flow_type, tags, is_active, created_at
                FROM prefect_flows
                WHERE is_active = 1
            """))
            flows = []
            for row in result:
                flow_dict = self._row_to_dict(row)
                flow_dict['tags'] = self._parse_json_field(flow_dict.get('tags'))
                flows.append(flow_dict)
            return flows

    def get_tasks(self, flow_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get all active Prefect tasks - returns list of dicts

        Args:
            flow_id: Optional flow ID to filter tasks by flow

        Returns:
            List of task dictionaries
        """
        with self.get_session() as session:
            if flow_id:
                result = session.execute(text("""
                    SELECT id, name, description, flow_id, celery_task_name,
                           task_args, task_kwargs, depends_on, tags, retry_config,
                           timeout_seconds, is_active, created_at
                    FROM prefect_tasks
                    WHERE is_active = 1 AND flow_id = :flow_id
                """), {"flow_id": flow_id})
            else:
                result = session.execute(text("""
                    SELECT id, name, description, flow_id, celery_task_name,
                           task_args, task_kwargs, depends_on, tags, retry_config,
                           timeout_seconds, is_active, created_at
                    FROM prefect_tasks
                    WHERE is_active = 1
                """))
            tasks = []
            for row in result:
                task_dict = self._row_to_dict(row)
                task_dict['task_args'] = self._parse_json_field(task_dict.get('task_args'))
                task_dict['task_kwargs'] = self._parse_json_field(task_dict.get('task_kwargs'))
                task_dict['depends_on'] = self._parse_json_field(task_dict.get('depends_on'))
                task_dict['tags'] = self._parse_json_field(task_dict.get('tags'))
                task_dict['retry_config'] = self._parse_json_field(task_dict.get('retry_config'))
                tasks.append(task_dict)
            return tasks

    def get_deployments(self, flow_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get all active Prefect deployments - returns list of dicts

        Args:
            flow_id: Optional flow ID to filter deployments by flow

        Returns:
            List of deployment dictionaries
        """
        with self.get_session() as session:
            if flow_id:
                result = session.execute(text("""
                    SELECT id, name, description, flow_id, work_pool_name,
                           work_queue_name, schedule_type, schedule_config,
                           parameters, tags, is_active, created_at
                    FROM prefect_deployments
                    WHERE is_active = 1 AND flow_id = :flow_id
                """), {"flow_id": flow_id})
            else:
                result = session.execute(text("""
                    SELECT id, name, description, flow_id, work_pool_name,
                           work_queue_name, schedule_type, schedule_config,
                           parameters, tags, is_active, created_at
                    FROM prefect_deployments
                    WHERE is_active = 1
                """))
            deployments = []
            for row in result:
                deployment_dict = self._row_to_dict(row)
                deployment_dict['schedule_config'] = self._parse_json_field(deployment_dict.get('schedule_config'))
                deployment_dict['parameters'] = self._parse_json_field(deployment_dict.get('parameters'))
                deployment_dict['tags'] = self._parse_json_field(deployment_dict.get('tags'))
                deployments.append(deployment_dict)
            return deployments

    def get_work_pools(self) -> List[Dict[str, Any]]:
        """Get all active Prefect work pools - returns list of dicts"""
        with self.get_session() as session:
            result = session.execute(text("""
                SELECT id, name, description, pool_type, config,
                       concurrency_limit, is_active, created_at
                FROM prefect_work_pools
                WHERE is_active = 1
            """))
            work_pools = []
            for row in result:
                pool_dict = self._row_to_dict(row)
                pool_dict['config'] = self._parse_json_field(pool_dict.get('config'))
                work_pools.append(pool_dict)
            return work_pools

    def get_flow_by_id(self, flow_id: int) -> Optional[Dict[str, Any]]:
        """Get a single flow by ID"""
        with self.get_session() as session:
            result = session.execute(text("""
                SELECT id, name, description, flow_type, tags, is_active, created_at
                FROM prefect_flows
                WHERE id = :flow_id
            """), {"flow_id": flow_id})
            row = result.first()
            if row:
                flow_dict = self._row_to_dict(row)
                flow_dict['tags'] = self._parse_json_field(flow_dict.get('tags'))
                return flow_dict
            return None

    def get_flow_by_name(self, flow_name: str) -> Optional[Dict[str, Any]]:
        """Get a single flow by name"""
        with self.get_session() as session:
            result = session.execute(text("""
                SELECT id, name, description, flow_type, tags, is_active, created_at
                FROM prefect_flows
                WHERE name = :flow_name
            """), {"flow_name": flow_name})
            row = result.first()
            if row:
                flow_dict = self._row_to_dict(row)
                flow_dict['tags'] = self._parse_json_field(flow_dict.get('tags'))
                return flow_dict
            return None
