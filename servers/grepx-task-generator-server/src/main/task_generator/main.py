#!/usr/bin/env python3
"""
Task Generator Main Script
==========================
Reads task-config.yaml, loads tasks/assets from specified file, and inserts into database
"""
import sys
from datetime import datetime
from pathlib import Path

import yaml

# Get project root (src/main/task_generator/main.py -> src/)
script_dir = Path(__file__).parent.parent.parent

# Add src/main to path so we can import task_generator
sys.path.insert(0, str(script_dir / "main"))

from task_generator.database import DatabaseManager
from grepx_models import CeleryTask, Asset, Resource, Schedule, Sensor


def load_config(config_path: Path) -> dict:
    """Load task-config.yaml"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def load_tasks_file(tasks_file_path: Path) -> dict:
    """Load tasks and assets from YAML file"""
    with open(tasks_file_path, 'r') as f:
        return yaml.safe_load(f)


def insert_celery_tasks(db_manager: DatabaseManager, tasks: list):
    """Insert Celery tasks into database"""
    count = 0
    with db_manager.get_session() as session:
        for task_data in tasks:
            try:
                # Check if exists
                existing = session.query(CeleryTask).filter_by(name=task_data['name']).first()
                if existing:
                    print(f"  Task '{task_data['name']}' already exists, skipping...")
                    continue
                
                task = CeleryTask(
                    name=task_data['name'],
                    module_path=task_data['module_path'],
                    function_name=task_data['function_name'],
                    description=task_data.get('description', ''),
                    tags=task_data.get('tags', []),
                    options=task_data.get('options', {}),
                    retry_policy=task_data.get('retry_policy', {}),
                    timeout=task_data.get('timeout', 300),
                    is_active=task_data.get('is_active', True),
                    created_at=datetime.now()
                )
                session.add(task)
                count += 1
                print(f"  [+] Created Celery task: {task_data['name']}")
            except Exception as e:
                print(f"  [-] Failed to create task '{task_data['name']}': {e}")
                session.rollback()
        
        session.commit()
    return count


def insert_assets(db_manager: DatabaseManager, assets: list):
    """Insert Dagster assets into database"""
    count = 0
    with db_manager.get_session() as session:
        for asset_data in assets:
            try:
                # Check if exists
                existing = session.query(Asset).filter_by(name=asset_data['name']).first()
                if existing:
                    print(f"  Asset '{asset_data['name']}' already exists, skipping...")
                    continue
                
                asset = Asset(
                    name=asset_data['name'],
                    description=asset_data.get('description', ''),
                    group_name=asset_data.get('group_name'),
                    asset_type=asset_data.get('asset_type'),
                    dependencies=asset_data.get('dependencies', []),
                    config=asset_data.get('config', {}),
                    celery_task_name=asset_data['celery_task_name'],
                    task_args=asset_data.get('task_args', []),
                    task_kwargs=asset_data.get('task_kwargs', {}),
                    partition_type=asset_data.get('partition_type'),
                    partition_config=asset_data.get('partition_config', {}),
                    is_active=asset_data.get('is_active', True),
                    created_at=datetime.now()
                )
                session.add(asset)
                count += 1
                print(f"  [+] Created Dagster asset: {asset_data['name']}")
            except Exception as e:
                print(f"  [-] Failed to create asset '{asset_data['name']}': {e}")
                session.rollback()
        
        session.commit()
    return count


def insert_resources(db_manager: DatabaseManager, resources: list):
    """Insert resources into database"""
    count = 0
    with db_manager.get_session() as session:
        for resource_data in resources:
            try:
                existing = session.query(Resource).filter_by(name=resource_data['name']).first()
                if existing:
                    print(f"  Resource '{resource_data['name']}' already exists, skipping...")
                    continue
                
                resource = Resource(
                    name=resource_data['name'],
                    resource_type=resource_data['resource_type'],
                    config=resource_data.get('config', {}),
                    is_active=resource_data.get('is_active', True),
                    created_at=datetime.now()
                )
                session.add(resource)
                count += 1
                print(f"  [+] Created resource: {resource_data['name']}")
            except Exception as e:
                print(f"  [-] Failed to create resource '{resource_data['name']}': {e}")
                session.rollback()
        
        session.commit()
    return count


def insert_schedules(db_manager: DatabaseManager, schedules: list):
    """Insert schedules into database"""
    count = 0
    with db_manager.get_session() as session:
        for schedule_data in schedules:
            try:
                existing = session.query(Schedule).filter_by(name=schedule_data['name']).first()
                if existing:
                    print(f"  Schedule '{schedule_data['name']}' already exists, skipping...")
                    continue
                
                schedule = Schedule(
                    name=schedule_data['name'],
                    cron_schedule=schedule_data['cron_schedule'],
                    target_assets=schedule_data.get('target_assets', []),
                    config=schedule_data.get('config', {}),
                    is_active=schedule_data.get('is_active', True),
                    created_at=datetime.now()
                )
                session.add(schedule)
                count += 1
                print(f"  [+] Created schedule: {schedule_data['name']}")
            except Exception as e:
                print(f"  [-] Failed to create schedule '{schedule_data['name']}': {e}")
                session.rollback()
        
        session.commit()
    return count


def insert_sensors(db_manager: DatabaseManager, sensors: list):
    """Insert sensors into database"""
    count = 0
    with db_manager.get_session() as session:
        for sensor_data in sensors:
            try:
                existing = session.query(Sensor).filter_by(name=sensor_data['name']).first()
                if existing:
                    print(f"  Sensor '{sensor_data['name']}' already exists, skipping...")
                    continue
                
                sensor = Sensor(
                    name=sensor_data['name'],
                    sensor_type=sensor_data['sensor_type'],
                    target_assets=sensor_data.get('target_assets', []),
                    config=sensor_data.get('config', {}),
                    minimum_interval_seconds=sensor_data.get('minimum_interval_seconds', 30),
                    is_active=sensor_data.get('is_active', True),
                    created_at=datetime.now()
                )
                session.add(sensor)
                count += 1
                print(f"  [+] Created sensor: {sensor_data['name']}")
            except Exception as e:
                print(f"  [-] Failed to create sensor '{sensor_data['name']}': {e}")
                session.rollback()
        
        session.commit()
    return count


def main():
    """Main execution"""
    # Get script directory (src/main/task_generator/main.py -> src/main/)
    main_dir = Path(__file__).parent.parent
    config_path = main_dir / "resources" / "task-config.yaml"
    
    if not config_path.exists():
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    
    # Load configuration
    print("Loading configuration...")
    config = load_config(config_path)
    
    # Get database URL and tasks file path
    db_url = config['database']['db_url']
    tasks_file = config.get('tasks_file', 'resources/tasks.yaml')
    # Resolve tasks file path relative to main directory
    if tasks_file.startswith('resources/'):
        tasks_file_path = main_dir / tasks_file
    else:
        tasks_file_path = main_dir / "resources" / tasks_file
    
    if not tasks_file_path.exists():
        print(f"ERROR: Tasks file not found: {tasks_file_path}")
        sys.exit(1)
    
    # Initialize database
    print(f"Connecting to database: {db_url}")
    db_manager = DatabaseManager(db_url=db_url)
    db_manager.initialize_schema()
    print("Database schema initialized")
    
    # Load tasks and assets
    print(f"Loading tasks from: {tasks_file_path}")
    tasks_data = load_tasks_file(tasks_file_path)
    
    # Insert into database
    print("\n=== Inserting into Database ===")
    
    results = {
        'celery_tasks': 0,
        'assets': 0,
        'resources': 0,
        'schedules': 0,
        'sensors': 0
    }
    
    if 'celery_tasks' in tasks_data:
        print("\nCelery Tasks:")
        results['celery_tasks'] = insert_celery_tasks(db_manager, tasks_data['celery_tasks'])
    
    if 'assets' in tasks_data:
        print("\nDagster Assets:")
        results['assets'] = insert_assets(db_manager, tasks_data['assets'])
    
    if 'resources' in tasks_data:
        print("\nResources:")
        results['resources'] = insert_resources(db_manager, tasks_data['resources'])
    
    if 'schedules' in tasks_data:
        print("\nSchedules:")
        results['schedules'] = insert_schedules(db_manager, tasks_data['schedules'])
    
    if 'sensors' in tasks_data:
        print("\nSensors:")
        results['sensors'] = insert_sensors(db_manager, tasks_data['sensors'])
    
    # Summary
    print("\n=== Summary ===")
    print(f"Celery Tasks: {results['celery_tasks']} created")
    print(f"Dagster Assets: {results['assets']} created")
    print(f"Resources: {results['resources']} created")
    print(f"Schedules: {results['schedules']} created")
    print(f"Sensors: {results['sensors']} created")
    print("==============================\n")


if __name__ == '__main__':
    main()
