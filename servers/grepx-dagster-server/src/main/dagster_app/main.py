"""
Celery-Integrated Dynamic Dagster Server
==========================================
Loads assets, schedules, sensors, and resources from database and registers with Dagster.
Database is assumed to be pre-populated by task-generator-server.
"""
from dagster import Definitions, FilesystemIOManager
from .builders import DynamicAssetBuilder, DynamicScheduleBuilder, DynamicSensorBuilder
from .factories import DynamicResourceFactory
from .task_client import TaskClient
from .config_loader import ConfigLoader

# Use local DatabaseManager - no dependency on task-generator-server models
# This uses raw SQL queries, no ORM models needed
from .database_manager import DatabaseManager


# ============================================================================
# LOAD CONFIGURATION
# ============================================================================
config_loader = ConfigLoader()
config = config_loader.load_config()

# Get database URL from config
db_url = config.get('database', {}).get('db_url', 'sqlite:///./dagster_config_orm.db')
dagster_home = config.get('dagster', {}).get('home', './.dagster_home')
storage_base_dir = config.get('io_manager', {}).get('base_dir', f'{dagster_home}/storage')
default_broker_url = config.get('celery', {}).get('broker_url', 'redis://localhost:6379/0')

# ============================================================================
# DATABASE INITIALIZATION
# ============================================================================
# Initialize database connection (assumes database is pre-populated)
# DatabaseManager only READS from database - does NOT create tables or assets
# Tables and data are created by task-generator-server when it populates the database
db_manager = DatabaseManager(db_url=db_url)
# Note: Database must be populated by running:
#       python libs/grepx-task-generator-server/src/main/task_generator/main.py

# ============================================================================
# LOAD RESOURCES FROM DATABASE
# ============================================================================
resources_data = db_manager.get_resources()

# Get task_client broker URL from database or use config default
task_client_broker_url = default_broker_url
for resource_data in resources_data:
    resource_type = resource_data.get('resource_type') if isinstance(resource_data, dict) else resource_data.resource_type
    if resource_type == 'task_client':
        config = resource_data.get('config') if isinstance(resource_data, dict) else resource_data.config
        task_client_broker_url = config.get('broker_url', task_client_broker_url) if config else task_client_broker_url
        break

# Create TaskClient for executing Celery tasks
task_client = TaskClient(broker_url=task_client_broker_url)

# ============================================================================
# BUILD DAGSTER COMPONENTS FROM DATABASE
# ============================================================================
# Build assets from database
asset_builder = DynamicAssetBuilder(db_manager, task_client)
dynamic_assets = asset_builder.build_assets()
print(f"Loaded {len(dynamic_assets)} assets from database")

# Build schedules from database
schedule_builder = DynamicScheduleBuilder(db_manager)
dynamic_schedules = schedule_builder.build_schedules()
print(f"Loaded {len(dynamic_schedules)} schedules from database")

# Build sensors from database
sensor_builder = DynamicSensorBuilder(db_manager)
dynamic_sensors = sensor_builder.build_sensors()
print(f"Loaded {len(dynamic_sensors)} sensors from database")

# Build resources from database
dynamic_resources = {}
for resource_data in resources_data:
    resource_name = resource_data.get('name') if isinstance(resource_data, dict) else resource_data.name
    dynamic_resources[resource_name] = DynamicResourceFactory.create_resource(resource_data)
print(f"Loaded {len(dynamic_resources)} resources from database")

# Add default resources
dynamic_resources['io_manager'] = FilesystemIOManager(base_dir=storage_base_dir)
dynamic_resources['db_manager'] = db_manager

# ============================================================================
# REGISTER WITH DAGSTER
# ============================================================================
defs = Definitions(
    assets=dynamic_assets,
    schedules=dynamic_schedules,
    sensors=dynamic_sensors,
    resources=dynamic_resources,
)

