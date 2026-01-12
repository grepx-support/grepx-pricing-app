"""
Prefect Flow Generator - Creates Prefect flows, tasks, deployments, and work pools from configuration
"""
from typing import Dict, List, Any, Optional
from datetime import datetime


class PrefectFlowGenerator:
    """
    Generates Prefect flows, tasks, deployments, and work pools in the database
    """

    def __init__(self, db_manager):
        """
        Initialize Prefect flow generator with database manager

        Args:
            db_manager: DatabaseManager instance for database operations
        """
        self.db_manager = db_manager

    def create_flow(
        self,
        name: str,
        description: Optional[str] = None,
        flow_type: Optional[str] = None,
        tags: Optional[List[str]] = None,
        is_active: bool = True
    ) -> bool:
        """
        Create a Prefect flow in the database

        Args:
            name: Unique name for the flow
            description: Optional description
            flow_type: Type of flow (e.g., 'data_pipeline', 'batch_job')
            tags: List of tags for categorization
            is_active: Whether the flow is active

        Returns:
            True if created successfully, False otherwise
        """
        from grepx_models import PrefectFlow

        with self.db_manager.get_session() as session:
            try:
                # Check if flow already exists
                existing = session.query(PrefectFlow).filter_by(name=name).first()
                if existing:
                    print(f"Flow '{name}' already exists, skipping creation")
                    return False

                flow = PrefectFlow(
                    name=name,
                    description=description,
                    flow_type=flow_type,
                    tags=tags or [],
                    is_active=is_active,
                    created_at=datetime.now()
                )
                session.add(flow)
                session.commit()
                print(f"Created Prefect flow: {name}")
                return True
            except Exception as e:
                session.rollback()
                print(f"Error creating flow '{name}': {e}")
                return False

    def create_task(
        self,
        name: str,
        flow_name: str,
        celery_task_name: str,
        description: Optional[str] = None,
        task_args: Optional[List[Any]] = None,
        task_kwargs: Optional[Dict[str, Any]] = None,
        depends_on: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        retry_config: Optional[Dict[str, Any]] = None,
        timeout_seconds: int = 300,
        is_active: bool = True
    ) -> bool:
        """
        Create a Prefect task in the database

        Args:
            name: Unique name for the task
            flow_name: Name of the flow this task belongs to
            celery_task_name: Name of the Celery task to execute
            description: Optional description
            task_args: Positional arguments for the Celery task
            task_kwargs: Keyword arguments for the Celery task
            depends_on: List of task names this task depends on
            tags: List of tags for categorization
            retry_config: Retry configuration (max_retries, retry_delay_seconds, etc.)
            timeout_seconds: Task timeout in seconds
            is_active: Whether the task is active

        Returns:
            True if created successfully, False otherwise
        """
        from grepx_models import PrefectTask, PrefectFlow

        with self.db_manager.get_session() as session:
            try:
                # Get flow_id
                flow = session.query(PrefectFlow).filter_by(name=flow_name).first()
                if not flow:
                    print(f"Flow '{flow_name}' not found, cannot create task '{name}'")
                    return False

                # Check if task already exists
                existing = session.query(PrefectTask).filter_by(name=name).first()
                if existing:
                    print(f"Task '{name}' already exists, skipping creation")
                    return False

                task = PrefectTask(
                    name=name,
                    flow_id=flow.id,
                    celery_task_name=celery_task_name,
                    description=description,
                    task_args=task_args or [],
                    task_kwargs=task_kwargs or {},
                    depends_on=depends_on or [],
                    tags=tags or [],
                    retry_config=retry_config or {},
                    timeout_seconds=timeout_seconds,
                    is_active=is_active,
                    created_at=datetime.now()
                )
                session.add(task)
                session.commit()
                print(f"Created Prefect task: {name} for flow: {flow_name}")
                return True
            except Exception as e:
                session.rollback()
                print(f"Error creating task '{name}': {e}")
                return False

    def create_deployment(
        self,
        name: str,
        flow_name: str,
        work_pool_name: str,
        description: Optional[str] = None,
        work_queue_name: str = "default",
        schedule_type: Optional[str] = None,
        schedule_config: Optional[Dict[str, Any]] = None,
        parameters: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        is_active: bool = True
    ) -> bool:
        """
        Create a Prefect deployment in the database

        Args:
            name: Unique name for the deployment
            flow_name: Name of the flow to deploy
            work_pool_name: Name of the work pool to use
            description: Optional description
            work_queue_name: Queue within the work pool
            schedule_type: Type of schedule ('cron', 'interval', 'rrule', or None)
            schedule_config: Schedule configuration (cron_string, interval_seconds, etc.)
            parameters: Default parameters for the flow
            tags: List of tags for categorization
            is_active: Whether the deployment is active

        Returns:
            True if created successfully, False otherwise
        """
        from grepx_models import PrefectDeployment, PrefectFlow

        with self.db_manager.get_session() as session:
            try:
                # Get flow_id
                flow = session.query(PrefectFlow).filter_by(name=flow_name).first()
                if not flow:
                    print(f"Flow '{flow_name}' not found, cannot create deployment '{name}'")
                    return False

                # Check if deployment already exists
                existing = session.query(PrefectDeployment).filter_by(name=name).first()
                if existing:
                    print(f"Deployment '{name}' already exists, skipping creation")
                    return False

                deployment = PrefectDeployment(
                    name=name,
                    flow_id=flow.id,
                    work_pool_name=work_pool_name,
                    description=description,
                    work_queue_name=work_queue_name,
                    schedule_type=schedule_type,
                    schedule_config=schedule_config or {},
                    parameters=parameters or {},
                    tags=tags or [],
                    is_active=is_active,
                    created_at=datetime.now()
                )
                session.add(deployment)
                session.commit()
                print(f"Created Prefect deployment: {name} for flow: {flow_name}")
                return True
            except Exception as e:
                session.rollback()
                print(f"Error creating deployment '{name}': {e}")
                return False

    def create_work_pool(
        self,
        name: str,
        description: Optional[str] = None,
        pool_type: str = "process",
        config: Optional[Dict[str, Any]] = None,
        concurrency_limit: int = 10,
        is_active: bool = True
    ) -> bool:
        """
        Create a Prefect work pool in the database

        Args:
            name: Unique name for the work pool
            description: Optional description
            pool_type: Type of work pool ('process', 'docker', 'kubernetes', etc.)
            config: Pool-specific configuration
            concurrency_limit: Maximum concurrent runs
            is_active: Whether the work pool is active

        Returns:
            True if created successfully, False otherwise
        """
        from grepx_models import PrefectWorkPool

        with self.db_manager.get_session() as session:
            try:
                # Check if work pool already exists
                existing = session.query(PrefectWorkPool).filter_by(name=name).first()
                if existing:
                    print(f"Work pool '{name}' already exists, skipping creation")
                    return False

                work_pool = PrefectWorkPool(
                    name=name,
                    description=description,
                    pool_type=pool_type,
                    config=config or {},
                    concurrency_limit=concurrency_limit,
                    is_active=is_active,
                    created_at=datetime.now()
                )
                session.add(work_pool)
                session.commit()
                print(f"Created Prefect work pool: {name}")
                return True
            except Exception as e:
                session.rollback()
                print(f"Error creating work pool '{name}': {e}")
                return False
