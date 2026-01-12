#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Task Generator Main Script
==========================
Reads task-config.yaml, loads tasks/assets from specified file, and inserts into database
"""
import sys
import os
from datetime import datetime
from pathlib import Path

import yaml

# Fix encoding for Windows console
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Get project root (src/main/task_generator/main.py -> src/)
script_dir = Path(__file__).parent.parent.parent

# Add src/main to path so we can import task_generator
sys.path.insert(0, str(script_dir / "main"))

from task_generator.database import DatabaseManager
from grepx_models import CeleryTask, Asset, Resource, Schedule, Sensor, StorageMaster, StorageType
from grepx_models import PrefectFlow, PrefectTask, PrefectDeployment, PrefectWorkPool


def load_config(config_path: Path) -> dict:
    """Load task-config.yaml"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def load_tasks_file(tasks_file_path: Path) -> dict:
    """Load tasks and assets from YAML file"""
    with open(tasks_file_path, 'r') as f:
        return yaml.safe_load(f)


def populate_storage_master(db_manager: DatabaseManager, database_yaml_path: Path):
    """Populate storage_master table from database.yaml"""
    if not database_yaml_path.exists():
        print(f"  WARNING: database.yaml not found at {database_yaml_path}, skipping storage initialization")
        return 0

    try:
        with open(database_yaml_path, 'r') as f:
            db_config = yaml.safe_load(f)

        storage_configs = db_config.get('storage_configurations', [])
        if not storage_configs:
            print("  No storage configurations found in database.yaml")
            return 0

        count = 0
        with db_manager.get_session() as session:
            for config in storage_configs:
                try:
                    storage_name = config.get('storage_name')

                    # Check if exists
                    existing = session.query(StorageMaster).filter_by(storage_name=storage_name).first()

                    # Map storage type string to enum
                    storage_type_str = config.get('storage_type', '').lower()
                    storage_type_map = {
                        'mongodb': StorageType.MONGODB,
                        'postgresql': StorageType.POSTGRESQL,
                        'mysql': StorageType.MYSQL,
                        'sqlite': StorageType.SQLITE,
                        'duckdb': StorageType.DUCKDB,
                        'csv': StorageType.CSV,
                        'redis': StorageType.REDIS,
                    }
                    storage_type = storage_type_map.get(storage_type_str, StorageType.SQLITE)

                    if existing:
                        # Update existing
                        existing.storage_type = storage_type
                        existing.host = config.get('host')
                        existing.port = config.get('port')
                        existing.database_name = config.get('database_name')
                        existing.username = config.get('username')
                        existing.password = config.get('password')
                        existing.connection_string = config.get('connection_string')
                        existing.file_path = config.get('file_path')
                        existing.auth_source = config.get('auth_source')
                        existing.ssl_enabled = config.get('ssl_enabled', False)
                        existing.connection_params = config.get('connection_params')
                        existing.storage_metadata = config.get('storage_metadata')
                        existing.is_default = config.get('is_default', False)
                        existing.active_flag = config.get('active_flag', True)
                        existing.max_connections = config.get('max_connections', 10)
                        existing.timeout_seconds = config.get('timeout_seconds', 30)
                        existing.description = config.get('description')
                        print(f"  [~] Updated storage: {storage_name}")
                    else:
                        # Create new
                        storage = StorageMaster(
                            storage_name=storage_name,
                            storage_type=storage_type,
                            host=config.get('host'),
                            port=config.get('port'),
                            database_name=config.get('database_name'),
                            username=config.get('username'),
                            password=config.get('password'),
                            connection_string=config.get('connection_string'),
                            file_path=config.get('file_path'),
                            auth_source=config.get('auth_source'),
                            ssl_enabled=config.get('ssl_enabled', False),
                            connection_params=config.get('connection_params'),
                            storage_metadata=config.get('storage_metadata'),
                            is_default=config.get('is_default', False),
                            active_flag=config.get('active_flag', True),
                            max_connections=config.get('max_connections', 10),
                            timeout_seconds=config.get('timeout_seconds', 30),
                            description=config.get('description')
                        )
                        session.add(storage)
                        count += 1
                        print(f"  [+] Created storage: {storage_name}")

                except Exception as e:
                    print(f"  [-] Failed to process storage '{config.get('storage_name')}': {e}")
                    session.rollback()

            session.commit()

        return count

    except Exception as e:
        print(f"  [-] Error loading database.yaml: {e}")
        return 0


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


def insert_prefect_work_pools(db_manager: DatabaseManager, work_pools: list):
    """Insert Prefect work pools into database"""
    count = 0
    with db_manager.get_session() as session:
        for pool_data in work_pools:
            try:
                existing = session.query(PrefectWorkPool).filter_by(name=pool_data['name']).first()
                if existing:
                    print(f"  Work pool '{pool_data['name']}' already exists, skipping...")
                    continue

                work_pool = PrefectWorkPool(
                    name=pool_data['name'],
                    description=pool_data.get('description'),
                    pool_type=pool_data.get('pool_type', 'process'),
                    config=pool_data.get('config', {}),
                    concurrency_limit=pool_data.get('concurrency_limit', 10),
                    is_active=pool_data.get('is_active', True),
                    created_at=datetime.now()
                )
                session.add(work_pool)
                count += 1
                print(f"  [+] Created Prefect work pool: {pool_data['name']}")
            except Exception as e:
                print(f"  [-] Failed to create work pool '{pool_data['name']}': {e}")
                session.rollback()

        session.commit()
    return count


def insert_prefect_flows(db_manager: DatabaseManager, flows: list):
    """Insert Prefect flows into database"""
    count = 0
    with db_manager.get_session() as session:
        for flow_data in flows:
            try:
                existing = session.query(PrefectFlow).filter_by(name=flow_data['name']).first()
                if existing:
                    print(f"  Flow '{flow_data['name']}' already exists, skipping...")
                    continue

                flow = PrefectFlow(
                    name=flow_data['name'],
                    description=flow_data.get('description'),
                    flow_type=flow_data.get('flow_type'),
                    tags=flow_data.get('tags', []),
                    is_active=flow_data.get('is_active', True),
                    created_at=datetime.now()
                )
                session.add(flow)
                count += 1
                print(f"  [+] Created Prefect flow: {flow_data['name']}")
            except Exception as e:
                print(f"  [-] Failed to create flow '{flow_data['name']}': {e}")
                session.rollback()

        session.commit()
    return count


def insert_prefect_tasks(db_manager: DatabaseManager, tasks: list):
    """Insert Prefect tasks into database"""
    count = 0
    with db_manager.get_session() as session:
        for task_data in tasks:
            try:
                existing = session.query(PrefectTask).filter_by(name=task_data['name']).first()
                if existing:
                    print(f"  Task '{task_data['name']}' already exists, skipping...")
                    continue

                # Get flow_id from flow_name
                flow = session.query(PrefectFlow).filter_by(name=task_data['flow_name']).first()
                if not flow:
                    print(f"  [-] Flow '{task_data['flow_name']}' not found for task '{task_data['name']}', skipping...")
                    continue

                task = PrefectTask(
                    name=task_data['name'],
                    description=task_data.get('description'),
                    flow_id=flow.id,
                    celery_task_name=task_data['celery_task_name'],
                    task_args=task_data.get('task_args', []),
                    task_kwargs=task_data.get('task_kwargs', {}),
                    depends_on=task_data.get('depends_on', []),
                    tags=task_data.get('tags', []),
                    retry_config=task_data.get('retry_config', {}),
                    timeout_seconds=task_data.get('timeout_seconds', 300),
                    is_active=task_data.get('is_active', True),
                    created_at=datetime.now()
                )
                session.add(task)
                count += 1
                print(f"  [+] Created Prefect task: {task_data['name']}")
            except Exception as e:
                print(f"  [-] Failed to create task '{task_data['name']}': {e}")
                session.rollback()

        session.commit()
    return count


def insert_prefect_deployments(db_manager: DatabaseManager, deployments: list):
    """Insert Prefect deployments into database"""
    count = 0
    with db_manager.get_session() as session:
        for deployment_data in deployments:
            try:
                existing = session.query(PrefectDeployment).filter_by(name=deployment_data['name']).first()
                if existing:
                    print(f"  Deployment '{deployment_data['name']}' already exists, skipping...")
                    continue

                # Get flow_id from flow_name
                flow = session.query(PrefectFlow).filter_by(name=deployment_data['flow_name']).first()
                if not flow:
                    print(f"  [-] Flow '{deployment_data['flow_name']}' not found for deployment '{deployment_data['name']}', skipping...")
                    continue

                deployment = PrefectDeployment(
                    name=deployment_data['name'],
                    description=deployment_data.get('description'),
                    flow_id=flow.id,
                    work_pool_name=deployment_data['work_pool_name'],
                    work_queue_name=deployment_data.get('work_queue_name', 'default'),
                    schedule_type=deployment_data.get('schedule_type'),
                    schedule_config=deployment_data.get('schedule_config', {}),
                    parameters=deployment_data.get('parameters', {}),
                    tags=deployment_data.get('tags', []),
                    is_active=deployment_data.get('is_active', True),
                    created_at=datetime.now()
                )
                session.add(deployment)
                count += 1
                print(f"  [+] Created Prefect deployment: {deployment_data['name']}")
            except Exception as e:
                print(f"  [-] Failed to create deployment '{deployment_data['name']}': {e}")
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
    
    # Get database URL - prefer environment variable over config file
    db_url = os.getenv('GREPX_MASTER_DB_URL')
    if not db_url:
        # Fallback to config file if environment variable not set
        db_url = config.get('database', {}).get('db_url')
        if not db_url:
            print("ERROR: GREPX_MASTER_DB_URL not set in environment and not found in config")
            sys.exit(1)
    
    # Get tasks files (support both old single file and new multiple files format)
    tasks_files = config.get('tasks_files', [config.get('tasks_file', 'resources/pricing_tasks.yaml')])
    
    # Initialize database
    print(f"Connecting to database: {db_url}")
    db_manager = DatabaseManager(db_url=db_url)
    db_manager.initialize_schema()
    print("Database schema initialized")

    # Populate storage_master from database.yaml
    print("\nPopulating storage_master table from database.yaml...")
    database_yaml_path = main_dir / "resources" / "database.yaml"
    storage_count = populate_storage_master(db_manager, database_yaml_path)
    if storage_count > 0:
        print(f"[OK] Created {storage_count} storage configurations\n")
    
    # Process each tasks file
    total_results = {
        'celery_tasks': 0,
        'assets': 0,
        'resources': 0,
        'schedules': 0,
        'sensors': 0,
        'prefect_work_pools': 0,
        'prefect_flows': 0,
        'prefect_tasks': 0,
        'prefect_deployments': 0
    }
    
    for tasks_file in tasks_files:
        if not tasks_file:
            continue
        
        # Resolve tasks file path relative to main directory
        if tasks_file.startswith('resources/'):
            tasks_file_path = main_dir / tasks_file
        else:
            tasks_file_path = main_dir / "resources" / tasks_file
        
        if not tasks_file_path.exists():
            print(f"WARNING: Tasks file not found, skipping: {tasks_file_path}")
            continue
        
        # Load tasks and assets
        print(f"\n=== Processing: {tasks_file_path.name} ===")
        tasks_data = load_tasks_file(tasks_file_path)
        
        # Insert into database
        results = {
            'celery_tasks': 0,
            'assets': 0,
            'resources': 0,
            'schedules': 0,
            'sensors': 0,
            'prefect_work_pools': 0,
            'prefect_flows': 0,
            'prefect_tasks': 0,
            'prefect_deployments': 0
        }
        
        if 'celery_tasks' in tasks_data:
            print("\nInserting Celery tasks...")
            results['celery_tasks'] = insert_celery_tasks(db_manager, tasks_data['celery_tasks'])
        
        if 'dagster_assets' in tasks_data:
            print("\nInserting Dagster assets...")
            results['assets'] = insert_assets(db_manager, tasks_data['dagster_assets'])
        
        if 'dagster_resources' in tasks_data:
            print("\nInserting Dagster resources...")
            results['resources'] = insert_resources(db_manager, tasks_data['dagster_resources'])
        
        if 'dagster_schedules' in tasks_data:
            print("\nInserting Dagster schedules...")
            results['schedules'] = insert_schedules(db_manager, tasks_data['dagster_schedules'])
        
        if 'dagster_sensors' in tasks_data:
            print("\nInserting Dagster sensors...")
            results['sensors'] = insert_sensors(db_manager, tasks_data['dagster_sensors'])

        # Handle Prefect items (must be created in order: work_pools -> flows -> tasks -> deployments)
        if 'prefect_work_pools' in tasks_data:
            print("\nInserting Prefect work pools...")
            results['prefect_work_pools'] = insert_prefect_work_pools(db_manager, tasks_data['prefect_work_pools'])

        if 'prefect_flows' in tasks_data:
            print("\nInserting Prefect flows...")
            results['prefect_flows'] = insert_prefect_flows(db_manager, tasks_data['prefect_flows'])

        if 'prefect_tasks' in tasks_data:
            print("\nInserting Prefect tasks...")
            results['prefect_tasks'] = insert_prefect_tasks(db_manager, tasks_data['prefect_tasks'])

        if 'prefect_deployments' in tasks_data:
            print("\nInserting Prefect deployments...")
            results['prefect_deployments'] = insert_prefect_deployments(db_manager, tasks_data['prefect_deployments'])

        # Update totals
        for key in total_results:
            total_results[key] += results[key]
        
        print(f"\n[OK] Completed {tasks_file_path.name}: {sum(results.values())} items created")
    
    print("\n=== Summary ===")
    print(f"Total Celery tasks created: {total_results['celery_tasks']}")
    print(f"Total Dagster assets created: {total_results['assets']}")
    print(f"Total Dagster resources created: {total_results['resources']}")
    print(f"Total Dagster schedules created: {total_results['schedules']}")
    print(f"Total Dagster sensors created: {total_results['sensors']}")
    print(f"Total Prefect work pools created: {total_results['prefect_work_pools']}")
    print(f"Total Prefect flows created: {total_results['prefect_flows']}")
    print(f"Total Prefect tasks created: {total_results['prefect_tasks']}")
    print(f"Total Prefect deployments created: {total_results['prefect_deployments']}")
    print(f"\nGrand Total: {sum(total_results.values())} items created")
    print("\n[OK] Task generation complete!")
    
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
