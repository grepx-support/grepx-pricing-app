"""
Schedule Builder for creating dynamic Dagster schedules from database
"""
from dagster import ScheduleDefinition, AssetSelection
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Protocol
    class DatabaseManager(Protocol):
        def get_schedules(self): ...
else:
    DatabaseManager = None


class DynamicScheduleBuilder:
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    def build_schedules(self):
        """Build all schedules from database"""
        schedules_data = self.db_manager.get_schedules()
        schedules = []
        
        for schedule_data in schedules_data:
            # Handle both dict and object access
            if isinstance(schedule_data, dict):
                name = schedule_data.get('name')
                cron_schedule = schedule_data.get('cron_schedule')
                target_assets = schedule_data.get('target_assets') or []
            else:
                name = schedule_data.name
                cron_schedule = schedule_data.cron_schedule
                target_assets = schedule_data.target_assets if schedule_data.target_assets else []
            
            if target_assets:
                target = AssetSelection.assets(*target_assets)
            else:
                target = AssetSelection.all()
            
            schedule = ScheduleDefinition(
                name=name,
                target=target,
                cron_schedule=cron_schedule,
            )
            schedules.append(schedule)
        
        return schedules

