"""
Lightweight Database Manager for Prefect - Uses Raw SQL Queries
No ORM model dependencies - only reads from database
"""
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from prefect.blocks.core import Block
from typing import Dict, Any, List, Optional
import json


class DatabaseManager(Block):
    """Lightweight database manager for Prefect - read-only, uses raw SQL"""
    
    _block_type_name = "Prefect Database Manager"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/3Nn3Bb8PbLn3yZWNHDYLZ/0fd3a9e5f8c89e4f0b62f017e6ff1ad6/prefect-logo-mark-gradient.svg"
    
    db_url: str = "sqlite:///./prefect_config_orm.db"

    def __init__(self, **kwargs):
        if 'db_url' not in kwargs:
            kwargs['db_url'] = "sqlite:///./prefect_config_orm.db"
        super().__init__(**kwargs)
        self._ensure_engine()

    def _ensure_engine(self):
        """Ensure engine is initialized"""
        if not hasattr(self, '_engine'):
            db_url = self.db_url
            object.__setattr__(self, '_engine', create_engine(db_url, echo=False))
            object.__setattr__(self, '_Session', sessionmaker(bind=self._engine))

    def get_session(self):
        """Get a new database session"""
        self._ensure_engine()
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

    def get_prefect_artifacts(self) -> List[Dict[str, Any]]:
        """Get all active Prefect artifacts - returns list of dicts"""
        with self.get_session() as session:
            result = session.execute(text("""
                SELECT id, name, description, group_name, artifact_type, 
                       dependencies, config, entrypoint, deployment_name, 
                       work_pool_name, parameters, partition_type, partition_config, 
                       is_active, created_at
                FROM prefect_artifacts 
                WHERE is_active = 1
            """))
            artifacts = []
            for row in result:
                artifact_dict = self._row_to_dict(row)
                # Parse JSON fields
                artifact_dict['dependencies'] = self._parse_json_field(artifact_dict.get('dependencies'))
                artifact_dict['config'] = self._parse_json_field(artifact_dict.get('config'))
                artifact_dict['parameters'] = self._parse_json_field(artifact_dict.get('parameters'))
                artifact_dict['partition_config'] = self._parse_json_field(artifact_dict.get('partition_config'))
                artifacts.append(artifact_dict)
            return artifacts

    def get_assets(self) -> List[Dict[str, Any]]:
        """Get all active Dagster-style assets - returns list of dicts"""
        with self.get_session() as session:
            result = session.execute(text("""
                SELECT id, name, description, group_name, asset_type, 
                       dependencies, config, celery_task_name, task_args, 
                       task_kwargs, partition_type, partition_config, 
                       is_active, created_at
                FROM assets 
                WHERE is_active = 1
            """))
            assets = []
            for row in result:
                asset_dict = self._row_to_dict(row)
                # Parse JSON fields
                asset_dict['dependencies'] = self._parse_json_field(asset_dict.get('dependencies'))
                asset_dict['config'] = self._parse_json_field(asset_dict.get('config'))
                asset_dict['task_args'] = self._parse_json_field(asset_dict.get('task_args'))
                asset_dict['task_kwargs'] = self._parse_json_field(asset_dict.get('task_kwargs'))
                asset_dict['partition_config'] = self._parse_json_field(asset_dict.get('partition_config'))
                assets.append(asset_dict)
            return assets

    def get_resources(self) -> List[Dict[str, Any]]:
        """Get all active resources - returns list of dicts"""
        with self.get_session() as session:
            result = session.execute(text("""
                SELECT id, name, resource_type, config, is_active, created_at
                FROM resources 
                WHERE is_active = 1
            """))
            resources = []
            for row in result:
                resource_dict = self._row_to_dict(row)
                resource_dict['config'] = self._parse_json_field(resource_dict.get('config'))
                resources.append(resource_dict)
            return resources

    def get_schedules(self) -> List[Dict[str, Any]]:
        """Get all active schedules - returns list of dicts"""
        with self.get_session() as session:
            result = session.execute(text("""
                SELECT id, name, cron_schedule, target_assets, config, 
                       is_active, created_at
                FROM schedules 
                WHERE is_active = 1
            """))
            schedules = []
            for row in result:
                schedule_dict = self._row_to_dict(row)
                schedule_dict['target_assets'] = self._parse_json_field(schedule_dict.get('target_assets'))
                schedule_dict['config'] = self._parse_json_field(schedule_dict.get('config'))
                schedules.append(schedule_dict)
            return schedules

    def get_sensors(self) -> List[Dict[str, Any]]:
        """Get all active sensors - returns list of dicts"""
        with self.get_session() as session:
            result = session.execute(text("""
                SELECT id, name, sensor_type, target_assets, config, 
                       minimum_interval_seconds, is_active, created_at
                FROM sensors 
                WHERE is_active = 1
            """))
            sensors = []
            for row in result:
                sensor_dict = self._row_to_dict(row)
                sensor_dict['target_assets'] = self._parse_json_field(sensor_dict.get('target_assets'))
                sensor_dict['config'] = self._parse_json_field(sensor_dict.get('config'))
                sensors.append(sensor_dict)
            return sensors

    def get_celery_tasks(self) -> List[Dict[str, Any]]:
        """Get all active Celery tasks - returns list of dicts"""
        with self.get_session() as session:
            result = session.execute(text("""
                SELECT id, name, module_path, function_name, description, 
                       tags, options, retry_policy, timeout, is_active, created_at
                FROM celery_tasks 
                WHERE is_active = 1
            """))
            tasks = []
            for row in result:
                task_dict = self._row_to_dict(row)
                # Parse JSON fields
                task_dict['tags'] = self._parse_json_field(task_dict.get('tags'))
                task_dict['options'] = self._parse_json_field(task_dict.get('options'))
                task_dict['retry_policy'] = self._parse_json_field(task_dict.get('retry_policy'))
                tasks.append(task_dict)
            return tasks