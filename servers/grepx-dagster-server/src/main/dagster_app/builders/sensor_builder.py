"""
Sensor Builder for creating dynamic Dagster sensors from database
"""
from dagster import sensor, SensorEvaluationContext, RunRequest, AssetSelection
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Protocol
    class DatabaseManager(Protocol):
        def get_sensors(self): ...
else:
    DatabaseManager = None


class DynamicSensorBuilder:
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    def build_sensors(self):
        """Build all sensors from database"""
        sensors_data = self.db_manager.get_sensors()
        sensors_list = []
        
        for sensor_data in sensors_data:
            # Handle both dict and object access
            if isinstance(sensor_data, dict):
                name = sensor_data.get('name')
                sensor_type = sensor_data.get('sensor_type')
                target_assets = sensor_data.get('target_assets') or []
                config = sensor_data.get('config') or {}
                min_interval = sensor_data.get('minimum_interval_seconds') or 30
            else:
                name = sensor_data.name
                sensor_type = sensor_data.sensor_type
                target_assets = sensor_data.target_assets if sensor_data.target_assets else []
                config = sensor_data.config if sensor_data.config else {}
                min_interval = sensor_data.minimum_interval_seconds or 30
            
            if target_assets:
                target = AssetSelection.assets(*target_assets)
            else:
                target = AssetSelection.all()
            
            if sensor_type == 'file_watcher':
                watch_dir = config.get('watch_directory', './incoming')
                
                def make_sensor_fn(watch_directory):
                    def sensor_fn(context: SensorEvaluationContext):
                        from pathlib import Path
                        watch_path = Path(watch_directory)
                        if not watch_path.exists():
                            return
                        
                        files = list(watch_path.glob('*.*'))
                        for file in files:
                            yield RunRequest(
                                run_key=f"{file.name}_{file.stat().st_mtime}",
                                tags={"file": file.name}
                            )
                    return sensor_fn
                
                sensor_fn = make_sensor_fn(watch_dir)
                sensor_fn.__name__ = name
                
                sensor_obj = sensor(
                    name=name,
                    target=target,
                    minimum_interval_seconds=min_interval,
                )(sensor_fn)
                
                sensors_list.append(sensor_obj)
            
            elif sensor_type == 'interval':
                def make_interval_sensor(interval_config):
                    def sensor_fn(context: SensorEvaluationContext):
                        yield RunRequest(
                            run_key=f"interval_{datetime.now().timestamp()}",
                            tags={"trigger": "interval"}
                        )
                    return sensor_fn
                
                sensor_fn = make_interval_sensor(config)
                sensor_fn.__name__ = name
                
                sensor_obj = sensor(
                    name=name,
                    target=target,
                    minimum_interval_seconds=min_interval,
                )(sensor_fn)
                
                sensors_list.append(sensor_obj)
        
        return sensors_list

