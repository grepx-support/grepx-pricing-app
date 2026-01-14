"""
Main Task Generator - Orchestrates Celery task, Dagster asset
"""
from typing import Dict, List, Any, Optional
from datetime import datetime
from .celery_task_generator import CeleryTaskGenerator
from .dagster_asset_generator import DagsterAssetGenerator


class TaskGenerator:
    """
    Main task generator that creates Celery tasks, Dagster assets
    """

    def __init__(self, db_manager):
        """
        Initialize task generator with database manager

        Args:
            db_manager: DatabaseManager instance for database operations
        """
        self.db_manager = db_manager
        self.celery_generator = CeleryTaskGenerator(db_manager)
        self.asset_generator = DagsterAssetGenerator(db_manager)
    
    def generate_from_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate tasks, assets from a configuration dictionary

        Args:
            config: Configuration dict with structure:
                {
                    'celery_tasks': [...],
                    'assets': [...],
                    'resources': [...],
                    'schedules': [...],
                    'sensors': [...]
                }

        Returns:
            Dict with counts of created items
        """
        results = {
            'celery_tasks': 0,
            'assets': 0,
            'resources': 0,
            'schedules': 0,
            'sensors': 0
        }

        # Generate Celery tasks
        if 'celery_tasks' in config:
            for task_config in config['celery_tasks']:
                if self.celery_generator.create_task(**task_config):
                    results['celery_tasks'] += 1

        # Generate Dagster assets
        if 'assets' in config:
            for asset_config in config['assets']:
                if self.asset_generator.create_asset(**asset_config):
                    results['assets'] += 1

        # Generate resources
        if 'resources' in config:
            for resource_config in config['resources']:
                if self._create_resource(resource_config):
                    results['resources'] += 1

        # Generate schedules
        if 'schedules' in config:
            for schedule_config in config['schedules']:
                if self._create_schedule(schedule_config):
                    results['schedules'] += 1

        # Generate sensors
        if 'sensors' in config:
            for sensor_config in config['sensors']:
                if self._create_sensor(sensor_config):
                    results['sensors'] += 1

     
        return results
    
    def _create_resource(self, config: Dict[str, Any]) -> bool:
        """Create a resource in the database"""
        from grepx_models import Resource
        
        with self.db_manager.get_session() as session:
            try:
                resource = Resource(
                    name=config['name'],
                    resource_type=config['resource_type'],
                    config=config.get('config', {}),
                    is_active=config.get('is_active', True),
                    created_at=datetime.now()
                )
                session.add(resource)
                session.commit()
                return True
            except Exception:
                session.rollback()
                return False
    
    def _create_schedule(self, config: Dict[str, Any]) -> bool:
        """Create a schedule in the database"""
        from grepx_models import Schedule
        
        with self.db_manager.get_session() as session:
            try:
                schedule = Schedule(
                    name=config['name'],
                    cron_schedule=config['cron_schedule'],
                    target_assets=config.get('target_assets', []),
                    config=config.get('config', {}),
                    is_active=config.get('is_active', True),
                    created_at=datetime.now()
                )
                session.add(schedule)
                session.commit()
                return True
            except Exception:
                session.rollback()
                return False
    
    def _create_sensor(self, config: Dict[str, Any]) -> bool:
        """Create a sensor in the database"""
        from grepx_models import Sensor
        
        with self.db_manager.get_session() as session:
            try:
                sensor = Sensor(
                    name=config['name'],
                    sensor_type=config['sensor_type'],
                    target_assets=config.get('target_assets', []),
                    config=config.get('config', {}),
                    minimum_interval_seconds=config.get('minimum_interval_seconds', 30),
                    is_active=config.get('is_active', True),
                    created_at=datetime.now()
                )
                session.add(sensor)
                session.commit()
                return True
            except Exception:
                session.rollback()
                return False
