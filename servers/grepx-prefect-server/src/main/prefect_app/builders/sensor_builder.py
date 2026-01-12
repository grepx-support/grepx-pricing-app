"""
Prefect Sensor Builder
Mirrors the Dagster sensor builder pattern for creating Prefect-based event-driven flows
"""
from typing import Dict, Any, Callable, Optional
from prefect import flow, task
from prefect.events import Event, emit_event
from prefect.deployments import Deployment
import asyncio
import time
from datetime import datetime, timedelta


class PrefectSensorBuilder:
    """
    Builder class for creating Prefect event-driven flows that mirror Dagster sensor patterns
    """
    
    def __init__(self):
        self.sensors = {}
    
    def create_sensor(
        self,
        name: str,
        check_function: Callable,
        target_flow: Callable,
        minimum_interval_seconds: int = 60,
        parameters: Dict[str, Any] = None,
        work_pool_name: str = "default-agent-pool"
    ):
        """
        Create a sensor-like flow that monitors for conditions and triggers other flows
        """
        if parameters is None:
            parameters = {}
        
        @task(name=f"{name}_check_task")
        def check_condition():
            """Check the condition that determines if the target flow should run"""
            result = check_function()
            return result
        
        @flow(name=name)
        def sensor_flow():
            """Main sensor flow that checks conditions and triggers target flows"""
            condition_met = check_condition()
            
            if condition_met:
                print(f"Condition met for sensor {name}, triggering target flow...")
                
                # Trigger the target flow
                target_result = target_flow(**parameters)
                
                # Emit an event indicating the sensor triggered
                event = emit_event(
                    event=f"sensor.{name}.triggered",
                    resource={"prefect.resource.id": f"sensor.{name}"},
                    payload={
                        "condition_met": True,
                        "triggered_flow": target_flow.__name__,
                        "timestamp": datetime.now().isoformat(),
                        "result": str(target_result)
                    }
                )
                
                return {
                    "status": "triggered",
                    "target_flow": target_flow.__name__,
                    "result": target_result,
                    "event_id": str(event.id) if event else None
                }
            else:
                print(f"Condition not met for sensor {name}")
                return {"status": "no_action", "condition_met": False}
        
        # Register the sensor
        self.sensors[name] = {
            "flow": sensor_flow,
            "check_function": check_function,
            "target_flow": target_flow,
            "minimum_interval": minimum_interval_seconds,
            "parameters": parameters,
            "work_pool_name": work_pool_name
        }
        
        return sensor_flow
    
    def create_file_sensor(
        self,
        name: str,
        directory_path: str,
        file_pattern: str,
        target_flow: Callable,
        minimum_interval_seconds: int = 60,
        parameters: Dict[str, Any] = None,
        work_pool_name: str = "default-agent-pool"
    ):
        """
        Create a file-based sensor that watches for new files
        """
        import os
        import glob
        
        def check_file_condition():
            """Check if new files matching the pattern exist in the directory"""
            pattern = os.path.join(directory_path, file_pattern)
            files = glob.glob(pattern)
            # You could add logic to track which files have already been processed
            return len(files) > 0
        
        return self.create_sensor(
            name=name,
            check_function=check_file_condition,
            target_flow=target_flow,
            minimum_interval_seconds=minimum_interval_seconds,
            parameters=parameters,
            work_pool_name=work_pool_name
        )
    
    def create_external_api_sensor(
        self,
        name: str,
        api_endpoint: str,
        check_condition_fn: Callable,
        target_flow: Callable,
        minimum_interval_seconds: int = 60,
        parameters: Dict[str, Any] = None,
        work_pool_name: str = "default-agent-pool"
    ):
        """
        Create a sensor that polls an external API for conditions
        """
        import requests
        
        def check_api_condition():
            """Check the external API for conditions"""
            try:
                response = requests.get(api_endpoint)
                if response.status_code == 200:
                    data = response.json()
                    return check_condition_fn(data)
                return False
            except Exception as e:
                print(f"Error checking API for sensor {name}: {e}")
                return False
        
        return self.create_sensor(
            name=name,
            check_function=check_api_condition,
            target_flow=target_flow,
            minimum_interval_seconds=minimum_interval_seconds,
            parameters=parameters,
            work_pool_name=work_pool_name
        )
    
    def create_interval_sensor(
        self,
        name: str,
        check_function: Callable,
        target_flow: Callable,
        interval_seconds: int = 60,
        parameters: Dict[str, Any] = None,
        work_pool_name: str = "default-agent-pool"
    ):
        """
        Create an interval-based sensor that runs continuously
        """
        return self.create_sensor(
            name=name,
            check_function=check_function,
            target_flow=target_flow,
            minimum_interval_seconds=interval_seconds,
            parameters=parameters,
            work_pool_name=work_pool_name
        )
    
    def get_sensor(self, name: str):
        """Get a registered sensor by name"""
        return self.sensors.get(name)
    
    def get_all_sensors(self):
        """Get all registered sensors"""
        return self.sensors
    
    def run_sensor_once(self, name: str):
        """Run a specific sensor once"""
        sensor_info = self.sensors.get(name)
        if sensor_info:
            flow = sensor_info["flow"]
            return flow()
        else:
            raise ValueError(f"Sensor '{name}' not found")


# Global instance for convenience
sensor_builder = PrefectSensorBuilder()


def build_sensor_from_config(sensor_config: Dict[str, Any], flow_registry: Dict[str, flow]):
    """
    Build a sensor from configuration, similar to how Dagster builds sensors
    """
    name = sensor_config["name"]
    sensor_type = sensor_config["sensor_type"]
    target_flow_name = sensor_config["target_flow"]
    minimum_interval_seconds = sensor_config.get("minimum_interval_seconds", 60)
    parameters = sensor_config.get("parameters", {})
    work_pool_name = sensor_config.get("work_pool_name", "default-agent-pool")
    
    # Get the target flow from registry
    if target_flow_name not in flow_registry:
        raise ValueError(f"Flow '{target_flow_name}' not found in registry")
    
    target_flow = flow_registry[target_flow_name]
    
    if sensor_type == "file_watcher":
        directory = sensor_config["config"]["watch_directory"]
        pattern = sensor_config.get("config", {}).get("file_pattern", "*.*")
        
        return sensor_builder.create_file_sensor(
            name=name,
            directory_path=directory,
            file_pattern=pattern,
            target_flow=target_flow,
            minimum_interval_seconds=minimum_interval_seconds,
            parameters=parameters,
            work_pool_name=work_pool_name
        )
    elif sensor_type == "interval":
        # Define a simple check function for interval-based sensors
        def always_true():
            return True  # Or implement your specific condition
        
        return sensor_builder.create_interval_sensor(
            name=name,
            check_function=always_true,
            target_flow=target_flow,
            interval_seconds=minimum_interval_seconds,
            parameters=parameters,
            work_pool_name=work_pool_name
        )
    else:
        # Default to a generic sensor with a simple check function
        def default_check():
            # Implement your default condition checking logic here
            return True
        
        return sensor_builder.create_sensor(
            name=name,
            check_function=default_check,
            target_flow=target_flow,
            minimum_interval_seconds=minimum_interval_seconds,
            parameters=parameters,
            work_pool_name=work_pool_name
        )


if __name__ == "__main__":
    from prefect import flow
    
    @flow
    def sample_target_flow(trigger_source: str = "unknown"):
        print(f"Target flow triggered by: {trigger_source}")
        return {"status": "executed", "trigger": trigger_source}
    
    def sample_condition_check():
        """Sample condition check that randomly returns True/False"""
        import random
        return random.choice([True, False])
    
    # Create a sensor
    sensor_flow = sensor_builder.create_sensor(
        name="sample_sensor",
        check_function=sample_condition_check,
        target_flow=sample_target_flow,
        minimum_interval_seconds=30,
        parameters={"trigger_source": "sample_sensor"}
    )
    
    print(f"Created sensor: {sensor_flow.name}")
    
    # Optionally run the sensor once
    # result = sensor_builder.run_sensor_once("sample_sensor")
    # print("Sensor result:", result)