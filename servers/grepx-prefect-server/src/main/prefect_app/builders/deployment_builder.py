"""
Deployment Builder for Prefect
Creates and manages Prefect deployments programmatically
"""
from typing import Dict, Any, List, Optional, Callable
from prefect.deployments import Deployment
from prefect.client.schemas.schedules import CronSchedule, IntervalSchedule, RRuleSchedule
from datetime import timedelta
import logging
import asyncio

logger = logging.getLogger(__name__)


class DeploymentBuilder:
    """
    Builds and manages Prefect deployments
    """

    def __init__(self):
        """Initialize deployment builder"""
        pass

    def _create_schedule(
        self,
        schedule_type: Optional[str],
        schedule_config: Dict[str, Any]
    ):
        """
        Create a Prefect schedule from configuration

        Args:
            schedule_type: Type of schedule ('cron', 'interval', 'rrule', or None)
            schedule_config: Schedule configuration

        Returns:
            Prefect schedule object or None
        """
        if not schedule_type:
            return None

        try:
            if schedule_type == 'cron':
                return CronSchedule(
                    cron=schedule_config.get('cron_string', '0 0 * * *'),
                    timezone=schedule_config.get('timezone', 'UTC')
                )
            elif schedule_type == 'interval':
                interval_seconds = schedule_config.get('interval_seconds', 3600)
                anchor_date = schedule_config.get('anchor_date')
                timezone = schedule_config.get('timezone', 'UTC')

                return IntervalSchedule(
                    interval=timedelta(seconds=interval_seconds),
                    anchor_date=anchor_date,
                    timezone=timezone
                )
            elif schedule_type == 'rrule':
                return RRuleSchedule(
                    rrule=schedule_config.get('rrule'),
                    timezone=schedule_config.get('timezone', 'UTC')
                )
            else:
                logger.warning(f"Unknown schedule type: {schedule_type}")
                return None
        except Exception as e:
            logger.error(f"Error creating schedule: {str(e)}")
            return None

    async def create_deployment_async(
        self,
        deployment_name: str,
        flow_function: Callable,
        work_pool_name: str,
        work_queue_name: str = "default",
        schedule_type: Optional[str] = None,
        schedule_config: Optional[Dict[str, Any]] = None,
        parameters: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        description: Optional[str] = None
    ) -> Optional[str]:
        """
        Create a deployment asynchronously
        """
        schedule_config = schedule_config or {}
        parameters = parameters or {}
        tags = tags or []

        try:
            # Create schedule if specified
            schedule = self._create_schedule(schedule_type, schedule_config)

            # Build the deployment with entrypoint to dynamic_flow.py
            deployment = await Deployment.build_from_flow(
                flow=flow_function,
                name=deployment_name,
                work_pool_name=work_pool_name,
                work_queue_name=work_queue_name,
                schedule=schedule,
                parameters=parameters,
                tags=tags,
                description=description,
                entrypoint="prefect_app/flows/dynamic_flow.py:prefect_flow_function"  # ADD THIS LINE
            )

            # Apply the deployment (this registers it with Prefect)
            deployment_id = await deployment.apply()
            logger.info(f"Created deployment: {deployment_name} (ID: {deployment_id})")
            return str(deployment_id)

        except Exception as e:
            logger.error(f"Error creating deployment '{deployment_name}': {str(e)}")
            return None


    def create_deployment(
        self,
        deployment_name: str,
        flow_function: Callable,
        work_pool_name: str,
        work_queue_name: str = "default",
        schedule_type: Optional[str] = None,
        schedule_config: Optional[Dict[str, Any]] = None,
        parameters: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        description: Optional[str] = None
    ) -> Optional[str]:
        """
        Create a deployment (synchronous wrapper)

        Args:
            deployment_name: Name of the deployment
            flow_function: The flow function to deploy
            work_pool_name: Name of the work pool to use
            work_queue_name: Queue within the work pool
            schedule_type: Type of schedule
            schedule_config: Schedule configuration
            parameters: Default parameters
            tags: Deployment tags
            description: Deployment description

        Returns:
            Deployment ID if created, None otherwise
        """
        return asyncio.run(
            self.create_deployment_async(
                deployment_name=deployment_name,
                flow_function=flow_function,
                work_pool_name=work_pool_name,
                work_queue_name=work_queue_name,
                schedule_type=schedule_type,
                schedule_config=schedule_config,
                parameters=parameters,
                tags=tags,
                description=description
            )
        )
