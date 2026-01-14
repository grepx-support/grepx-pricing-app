"""
Asset Factory for creating dynamic Dagster assets
"""
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    StaticPartitionsDefinition,
)
from typing import Dict
from datetime import datetime
from ..task_client import TaskClient


class DynamicAssetFactory:
    @staticmethod
    def create_partition_def(partition_type: str, partition_config: Dict):
        """Create partition definition from config"""
        if partition_type == 'daily':
            start_date = partition_config.get('start_date', '2024-01-01')
            return DailyPartitionsDefinition(start_date=start_date)
        elif partition_type == 'static':
            partitions = partition_config.get('partitions', [])
            return StaticPartitionsDefinition(partitions)
        return None

    @staticmethod
    def create_asset_function(asset_config, task_client: TaskClient):
        """Create asset function from database config that executes Celery tasks"""
        # Handle both dict and object access
        if isinstance(asset_config, dict):
            name = asset_config.get('name')
            description = asset_config.get('description') or ''
            dependencies = asset_config.get('dependencies') or []
            celery_task_name = asset_config.get('celery_task_name')
            task_args = asset_config.get('task_args') or []
            task_kwargs = asset_config.get('task_kwargs') or {}
            config = asset_config.get('config') or {}
        else:
            # Fallback for ORM objects
            name = asset_config.name
            description = asset_config.description or ''
            dependencies = asset_config.dependencies if asset_config.dependencies else []
            celery_task_name = asset_config.celery_task_name
            task_args = asset_config.task_args if asset_config.task_args else []
            task_kwargs = asset_config.task_kwargs if asset_config.task_kwargs else {}
            config = asset_config.config if asset_config.config else {}

        def asset_function(context: AssetExecutionContext, **deps):
            try:
                # Combine task_kwargs, config, and dependency values
                combined_kwargs = {**task_kwargs, **config}

                # Add partition key if available (check if run is partitioned)
                if context.has_partition_key:
                    combined_kwargs['partition_key'] = context.partition_key

                # Add dependency values using asset names from database
                for dep_name, dep_value in deps.items():
                    combined_kwargs[dep_name] = dep_value

                context.log.info(f"Submitting Celery task: {celery_task_name}")
                context.log.info(f"Task args: {task_args}")
                context.log.info(f"Task kwargs: {task_kwargs}")
                context.log.info(f"Combined kwargs: {combined_kwargs}")
                context.log.info(f"Task kwargs keys: {list(combined_kwargs.keys())}")
                context.log.info(f"Dependencies received: {list(deps.keys())}")

                # Submit and execute Celery task
                result = task_client.execute(celery_task_name, *task_args, **combined_kwargs)

                context.log.info(f"Task completed successfully")

                metadata = {
                    'celery_task': celery_task_name,
                    'execution_time': datetime.now().isoformat(),
                    'has_dependencies': len(deps) > 0
                }
                context.add_output_metadata(metadata)

                return result

            except Exception as e:
                context.log.error(f"Celery task failed: {str(e)}")
                raise

        asset_function.__name__ = name
        return asset_function

