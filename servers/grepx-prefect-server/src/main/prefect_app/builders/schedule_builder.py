"""
Prefect Schedule Builder
Mirrors the Dagster schedule builder pattern for creating Prefect-based schedules
"""
from typing import Dict, Any, Callable, Optional
from prefect import flow
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import IntervalSchedule, CronSchedule
from datetime import timedelta
import pendulum


class PrefectScheduleBuilder:
    """
    Builder class for creating Prefect schedules that mirror Dagster schedule patterns
    """
    
    def __init__(self):
        self.schedules = {}
    
    def create_schedule(
        self,
        name: str,
        flow_fn: Callable,
        cron_schedule: Optional[str] = None,
        interval_hours: Optional[int] = None,
        interval_minutes: Optional[int] = None,
        timezone: str = "UTC",
        parameters: Dict[str, Any] = None,
        work_pool_name: str = "default-agent-pool"
    ):
        """
        Create a scheduled deployment for a flow, similar to Dagster schedules
        """
        if parameters is None:
            parameters = {}
        
        # Determine the schedule type
        if cron_schedule:
            schedule = CronSchedule(cron=cron_schedule, timezone=timezone)
        elif interval_hours or interval_minutes:
            interval_seconds = (interval_hours or 0) * 3600 + (interval_minutes or 0) * 60
            schedule = IntervalSchedule(interval=timedelta(seconds=interval_seconds))
        else:
            raise ValueError("Either cron_schedule or interval (hours/minutes) must be provided")
        
        # Create deployment with schedule
        deployment = Deployment.build_from_flow(
            flow=flow_fn,
            name=name,
            schedule=schedule,
            parameters=parameters,
            work_pool_name=work_pool_name,
            tags=[f"schedule:{name}", "prefect-asset"]
        )
        
        # Apply the deployment (this registers it with the Prefect server)
        deployment_id = deployment.apply()
        
        # Register the schedule
        self.schedules[name] = {
            "deployment_id": deployment_id,
            "flow": flow_fn,
            "schedule": schedule,
            "parameters": parameters,
            "work_pool_name": work_pool_name,
            "timezone": timezone
        }
        
        return deployment
    
    def create_daily_schedule(
        self,
        name: str,
        flow_fn: Callable,
        hour: int = 0,
        minute: int = 0,
        timezone: str = "UTC",
        parameters: Dict[str, Any] = None,
        work_pool_name: str = "default-agent-pool"
    ):
        """
        Create a daily schedule (similar to Dagster's daily schedules)
        """
        cron_expr = f"{minute} {hour} * * *"
        return self.create_schedule(
            name=name,
            flow_fn=flow_fn,
            cron_schedule=cron_expr,
            timezone=timezone,
            parameters=parameters,
            work_pool_name=work_pool_name
        )
    
    def create_weekly_schedule(
        self,
        name: str,
        flow_fn: Callable,
        day_of_week: int = 1,  # Monday = 1
        hour: int = 0,
        minute: int = 0,
        timezone: str = "UTC",
        parameters: Dict[str, Any] = None,
        work_pool_name: str = "default-agent-pool"
    ):
        """
        Create a weekly schedule (runs every week on specified day)
        """
        cron_expr = f"{minute} {hour} * * {day_of_week}"
        return self.create_schedule(
            name=name,
            flow_fn=flow_fn,
            cron_schedule=cron_expr,
            timezone=timezone,
            parameters=parameters,
            work_pool_name=work_pool_name
        )
    
    def create_interval_schedule(
        self,
        name: str,
        flow_fn: Callable,
        hours: int = 1,
        minutes: int = 0,
        timezone: str = "UTC",
        parameters: Dict[str, Any] = None,
        work_pool_name: str = "default-agent-pool"
    ):
        """
        Create an interval-based schedule
        """
        return self.create_schedule(
            name=name,
            flow_fn=flow_fn,
            interval_hours=hours,
            interval_minutes=minutes,
            timezone=timezone,
            parameters=parameters,
            work_pool_name=work_pool_name
        )
    
    def get_schedule(self, name: str):
        """Get a registered schedule by name"""
        return self.schedules.get(name)
    
    def get_all_schedules(self):
        """Get all registered schedules"""
        return self.schedules


# Global instance for convenience
schedule_builder = PrefectScheduleBuilder()


def build_schedule_from_config(schedule_config: Dict[str, Any], flow_registry: Dict[str, flow]):
    """
    Build a schedule from configuration, similar to how Dagster builds schedules
    """
    name = schedule_config["name"]
    target_flow_name = schedule_config["target_flow"]
    cron_schedule = schedule_config.get("cron_schedule")
    timezone = schedule_config.get("timezone", "UTC")
    parameters = schedule_config.get("parameters", {})
    work_pool_name = schedule_config.get("work_pool_name", "default-agent-pool")
    
    # Get the target flow from registry
    if target_flow_name not in flow_registry:
        raise ValueError(f"Flow '{target_flow_name}' not found in registry")
    
    target_flow = flow_registry[target_flow_name]
    
    # Create the schedule
    if cron_schedule:
        return schedule_builder.create_schedule(
            name=name,
            flow_fn=target_flow,
            cron_schedule=cron_schedule,
            timezone=timezone,
            parameters=parameters,
            work_pool_name=work_pool_name
        )
    else:
        # Default to daily at midnight
        return schedule_builder.create_daily_schedule(
            name=name,
            flow_fn=target_flow,
            parameters=parameters,
            work_pool_name=work_pool_name
        )


if __name__ == "__main__":
    from prefect import flow
    
    @flow
    def sample_flow(message: str = "Hello"):
        print(f"Running scheduled flow: {message}")
        return {"status": "completed", "message": message}
    
    # Create a schedule for the sample flow
    schedule = schedule_builder.create_daily_schedule(
        name="daily_sample_flow",
        flow_fn=sample_flow,
        hour=9,
        minute=30,
        parameters={"message": "Daily scheduled run"},
        work_pool_name="default-agent-pool"
    )
    
    print(f"Created schedule: {schedule.name}")
    print(f"Deployment ID: {schedule.id}")