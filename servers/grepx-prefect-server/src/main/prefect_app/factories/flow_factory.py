"""
Dynamic Flow Factory for Prefect
Creates Prefect flow functions from database configuration
"""
from typing import Dict, Any, List, Optional, Callable
from prefect import flow
import logging

logger = logging.getLogger(__name__)

# Module-level variable - MUST be set before worker tries to import
prefect_flow_function = None


class DynamicFlowFactory:
    """
    Factory for creating Prefect flow functions dynamically from database configuration
    """

    def __init__(self, task_factory):
        """
        Initialize factory with task factory

        Args:
            task_factory: DynamicTaskFactory instance for creating tasks
        """
        self.task_factory = task_factory

    def create_flow_function(
        self,
        flow_name: str,
        flow_tasks: List[Dict[str, Any]],
        description: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> Callable:
        """
        Create a Prefect flow function that orchestrates multiple tasks

        Args:
            flow_name: Name of the Prefect flow
            flow_tasks: List of task configuration dictionaries
            description: Flow description
            tags: Flow tags

        Returns:
            Prefect flow function
        """
        global prefect_flow_function  # Access module-level variable
        
        tags = tags or []

        # Create task functions for all tasks in the flow
        task_functions = {}
        task_dependencies = {}

        for task_config in flow_tasks:
            task_name = task_config['name']
            task_func = self.task_factory.create_task_function(
                task_name=task_name,
                celery_task_name=task_config['celery_task_name'],
                task_args=task_config.get('task_args', []),
                task_kwargs=task_config.get('task_kwargs', {}),
                retry_config=task_config.get('retry_config', {}),
                timeout_seconds=task_config.get('timeout_seconds', 300),
                tags=task_config.get('tags', [])
            )
            task_functions[task_name] = task_func
            task_dependencies[task_name] = task_config.get('depends_on', [])

        # Create the flow function
        # Note: tags are not supported in @flow decorator, they should be set on deployments
        @flow(
            name=flow_name,
            description=description
        )
        def flow_func(environment: str = "production", debug: bool = False, **kwargs):
            """
            Prefect flow that orchestrates multiple tasks with dependencies
            
            Args:
                environment: Environment to run in (e.g., 'production', 'staging')
                debug: Enable debug mode
                **kwargs: Additional parameters passed to the flow
            """
            logger.info(f"Starting flow: {flow_name}")
            logger.info(f"Environment: {environment}, Debug: {debug}")

            # Track task results for dependency management
            task_results = {}

            # Execute tasks in order, respecting dependencies
            executed = set()
            remaining = set(task_functions.keys())

            while remaining:
                # Find tasks whose dependencies have been satisfied
                ready = []
                for task_name in remaining:
                    deps = task_dependencies.get(task_name, [])
                    if all(dep in executed for dep in deps):
                        ready.append(task_name)

                if not ready:
                    # Check for circular dependencies
                    raise RuntimeError(
                        f"Circular dependency detected or missing tasks in flow {flow_name}. "
                        f"Remaining tasks: {remaining}, Executed: {executed}"
                    )

                # Execute ready tasks
                for task_name in ready:
                    logger.info(f"Executing task: {task_name}")
                    try:
                        result = task_functions[task_name]()
                        task_results[task_name] = result
                        executed.add(task_name)
                        remaining.remove(task_name)
                        logger.info(f"Task {task_name} completed")
                    except Exception as e:
                        logger.error(f"Task {task_name} failed: {str(e)}")
                        raise

            logger.info(f"Flow {flow_name} completed successfully")
            return task_results

        # Set the module-level variable
        prefect_flow_function = flow_func
        
        return flow_func