"""
Asset Builder for creating dynamic Dagster assets from database
"""
from dagster import asset, AssetExecutionContext
from typing import Any, TYPE_CHECKING
from ..factories import DynamicAssetFactory
from ..task_client import TaskClient

if TYPE_CHECKING:
    # Type hint only - actual DatabaseManager comes from task generator server
    from typing import Protocol
    class DatabaseManager(Protocol):
        def get_assets(self): ...
else:
    DatabaseManager = None


class DynamicAssetBuilder:
    def __init__(self, db_manager, task_client: TaskClient):
        self.db_manager = db_manager
        self.task_client = task_client
        self.assets_data = db_manager.get_assets()
        self.asset_map = {}
    
    def build_assets(self):
        """Build all assets from database"""
        assets = []

        for asset_data in self.assets_data:
            # Handle both dict and object access
            if isinstance(asset_data, dict):
                name = asset_data.get('name')
                group_name = asset_data.get('group_name')
                description = asset_data.get('description') or ''
                dependencies = asset_data.get('dependencies') or []
                partition_type = asset_data.get('partition_type')
                partition_config = asset_data.get('partition_config') or {}
                celery_task_name = asset_data.get('celery_task_name')
                task_args = asset_data.get('task_args') or []
                task_kwargs = asset_data.get('task_kwargs') or {}
                config = asset_data.get('config') or {}
            else:
                # Fallback for ORM objects
                name = asset_data.name
                group_name = asset_data.group_name
                description = asset_data.description or ''
                dependencies = asset_data.dependencies if asset_data.dependencies else []
                partition_type = asset_data.partition_type
                partition_config = asset_data.partition_config if asset_data.partition_config else {}
                celery_task_name = asset_data.celery_task_name
                task_args = asset_data.task_args if asset_data.task_args else []
                task_kwargs = asset_data.task_kwargs if asset_data.task_kwargs else {}
                config = asset_data.config if asset_data.config else {}

            partition_def = None
            if partition_type:
                partition_def = DynamicAssetFactory.create_partition_def(partition_type, partition_config)

            asset_func = DynamicAssetFactory.create_asset_function(asset_data, self.task_client)

            if dependencies:
                original_func = asset_func

                def make_wrapper(func, deps):
                    # Build parameter string for explicit dependency parameters
                    dep_params = ', '.join([f"{dep}: Any" for dep in deps])
                    dep_dict = ', '.join([f"'{dep}': {dep}" for dep in deps])

                    # Create function code with explicit named parameters
                    # This is CRITICAL for Dagster to recognize dependencies
                    func_code = f"""
def wrapper(context: AssetExecutionContext{', ' + dep_params if dep_params else ''}):
    return func(context, **{{{dep_dict}}})
"""

                    # Execute to create function with proper signature
                    local_vars = {'func': func, 'AssetExecutionContext': AssetExecutionContext, 'Any': Any}
                    exec(func_code, local_vars)
                    wrapper = local_vars['wrapper']
                    wrapper.__name__ = func.__name__

                    return wrapper

                asset_func = make_wrapper(original_func, dependencies)

            decorator_kwargs = {
                'name': name,
                'description': description,
            }
            if group_name:
                decorator_kwargs['group_name'] = group_name
            if partition_def:
                decorator_kwargs['partitions_def'] = partition_def

            decorated_asset = asset(**decorator_kwargs)(asset_func)
            assets.append(decorated_asset)
            self.asset_map[name] = decorated_asset

        return assets

