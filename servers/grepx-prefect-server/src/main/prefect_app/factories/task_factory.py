"""
Dynamic Task Factory for Prefect
Creates Prefect task functions that execute Celery tasks
"""
from typing import Dict, Any, List, Optional, Callable
from prefect import task
import logging

logger = logging.getLogger(__name__)


class DynamicTaskFactory:
    """
    Factory for creating Prefect task functions dynamically from database configuration
    """

    def __init__(self, task_client):
        """
        Initialize factory with Celery task client

        Args:
            task_client: TaskClient instance for executing Celery tasks
        """
        self.task_client = task_client

    def create_task_function(
        self,
        task_name: str,
        celery_task_name: str,
        task_args: Optional[List[Any]] = None,
        task_kwargs: Optional[Dict[str, Any]] = None,
        retry_config: Optional[Dict[str, Any]] = None,
        timeout_seconds: int = 300,
        tags: Optional[List[str]] = None
    ) -> Callable:
        """
        Create a Prefect task function that executes a Celery task

        Args:
            task_name: Name of the Prefect task
            celery_task_name: Name of the Celery task to execute
            task_args: Positional arguments for the Celery task
            task_kwargs: Keyword arguments for the Celery task
            retry_config: Retry configuration (max_retries, retry_delay_seconds)
            timeout_seconds: Timeout in seconds
            tags: Task tags

        Returns:
            Prefect task function
        """
        task_args = task_args or []
        task_kwargs = task_kwargs or {}
        retry_config = retry_config or {}
        tags = tags or []

        # Extract retry parameters
        max_retries = retry_config.get('max_retries', 0)
        retry_delay_seconds = retry_config.get('retry_delay_seconds', 0)

        # Create the task function
        @task(
            name=task_name,
            retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
            timeout_seconds=timeout_seconds,
            tags=tags
        )
        def prefect_task_function():
            """
            Prefect task that executes a Celery task
            """
            logger.info(f"Executing Prefect task: {task_name}")
            logger.info(f"Submitting Celery task: {celery_task_name}")
            logger.info(f"Args: {task_args}, Kwargs: {task_kwargs}")

            try:
                # Submit task to Celery and wait for result
                result = self.task_client.execute(
                    celery_task_name,
                    *task_args,
                    **task_kwargs
                )
                logger.info(f"Task {task_name} completed successfully")
                return result
            except Exception as e:
                logger.error(f"Task {task_name} failed: {str(e)}")
                raise

        return prefect_task_function
