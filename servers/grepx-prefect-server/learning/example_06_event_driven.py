"""
Prefect Learning Example 06: Event-Driven Flows
===============================================
Example demonstrating event-driven execution in Prefect flows
"""
from prefect import flow, task
from prefect.events import Event, RelatedResource, emit_event
from typing import Dict, Any, List
import asyncio
import time
from datetime import datetime


@task(name="monitor_data_source")
def monitor_data_source(source_name: str) -> Dict[str, Any]:
    """Monitor a data source for changes"""
    print(f"Monitoring data source: {source_name}")
    
    # Simulate checking for new data
    import random
    has_new_data = random.choice([True, False])
    
    status = {
        "source": source_name,
        "checked_at": datetime.now().isoformat(),
        "has_new_data": has_new_data,
        "data_count": random.randint(0, 50) if has_new_data else 0,
        "status": "new_data_available" if has_new_data else "no_new_data"
    }
    
    print(f"Monitoring result: {status}")
    return status


@task(name="trigger_on_new_data")
def trigger_on_new_data(monitor_result: Dict[str, Any]) -> bool:
    """Trigger downstream processing if new data is available"""
    print(f"Checking if we should trigger processing: {monitor_result}")
    
    should_trigger = monitor_result.get("has_new_data", False)
    
    if should_trigger:
        print(f"Triggering processing for {monitor_result['data_count']} new records")
        
        # Emit an event indicating new data is available
        event = Event(
            event="data.available",
            resource={"prefect.resource.id": f"datasource.{monitor_result['source']}"},
            payload={
                "record_count": monitor_result["data_count"],
                "timestamp": monitor_result["checked_at"]
            }
        )
        # In a real implementation, we would emit this event
        print(f"Emitted event: {event.event}")
    
    return should_trigger


@task(name="process_triggered_data")
def process_triggered_data(source_name: str, record_count: int) -> Dict[str, Any]:
    """Process data when triggered"""
    print(f"Processing {record_count} records from {source_name}")
    
    # Simulate processing
    start_time = time.time()
    time.sleep(0.1)  # Simulate processing time
    duration = round(time.time() - start_time, 2)
    
    result = {
        "source": source_name,
        "records_processed": record_count,
        "processing_duration": duration,
        "status": "completed",
        "processed_at": datetime.now().isoformat()
    }
    
    print(f"Processing result: {result}")
    return result


@task(name="send_notification")
def send_notification(process_result: Dict[str, Any]) -> str:
    """Send notification about processing result"""
    print(f"Sending notification for: {process_result}")
    
    notification = {
        "type": "processing_completed",
        "source": process_result["source"],
        "records": process_result["records_processed"],
        "status": process_result["status"],
        "timestamp": process_result["processed_at"]
    }
    
    print(f"Notification sent: {notification}")
    return f"Notification for {process_result['source']} processing"


@flow(name="Event-Based Data Processing Flow")
def event_based_processing_flow(source_name: str = "default_source") -> Dict[str, Any]:
    """
    A flow that demonstrates:
    - Event-driven processing
    - Conditional execution based on events
    - Monitoring and triggering
    """
    print(f"Starting event-based processing for source: {source_name}")
    
    # Monitor the data source
    monitor_result = monitor_data_source(source_name)
    
    # Check if we should trigger processing
    should_process = trigger_on_new_data(monitor_result)
    
    if should_process:
        # Process the new data
        process_result = process_triggered_data(
            source_name, 
            monitor_result["data_count"]
        )
        
        # Send notification about the processing
        notification = send_notification(process_result)
        
        result = {
            "monitor_result": monitor_result,
            "was_processed": True,
            "process_result": process_result,
            "notification_sent": notification
        }
    else:
        result = {
            "monitor_result": monitor_result,
            "was_processed": False,
            "message": "No new data to process"
        }
    
    print(f"Event-based flow result: {result}")
    return result


# Continuous monitoring flow
@flow(name="Continuous Monitoring Flow")
async def continuous_monitoring_flow(
    sources: List[str], 
    check_interval: int = 10,
    duration_minutes: int = 1
) -> List[Dict[str, Any]]:
    """
    A flow that demonstrates:
    - Continuous monitoring
    - Periodic checks
    - Asynchronous event handling
    """
    print(f"Starting continuous monitoring for sources: {sources}")
    print(f"Check interval: {check_interval}s, Duration: {duration_minutes}min")
    
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)
    results = []
    
    iteration = 0
    while time.time() < end_time:
        iteration += 1
        print(f"\n--- Iteration {iteration} ---")
        
        for source in sources:
            # Monitor each source
            monitor_result = monitor_data_source(source)
            
            # Check if we should trigger processing
            should_process = trigger_on_new_data(monitor_result)
            
            if should_process:
                # Process the new data
                process_result = process_triggered_data(
                    source,
                    monitor_result["data_count"]
                )
                
                # Send notification
                notification = send_notification(process_result)
                
                result = {
                    "iteration": iteration,
                    "source": source,
                    "monitor_result": monitor_result,
                    "was_processed": True,
                    "process_result": process_result,
                    "notification": notification
                }
            else:
                result = {
                    "iteration": iteration,
                    "source": source,
                    "monitor_result": monitor_result,
                    "was_processed": False,
                    "message": "No new data to process"
                }
            
            results.append(result)
        
        # Wait for the next check
        if time.time() < end_time:
            print(f"Waiting {check_interval} seconds for next check...")
            await asyncio.sleep(check_interval)
    
    print(f"Continuous monitoring completed with {len(results)} checks")
    return results


# Event listener concept (simulated)
@task(name="simulate_external_event")
def simulate_external_event(event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Simulate receiving an external event"""
    print(f"Received external event: {event_type} with payload: {payload}")
    
    event_response = {
        "received_event": event_type,
        "payload": payload,
        "processed_at": datetime.now().isoformat(),
        "status": "acknowledged"
    }
    
    print(f"Event processed: {event_response}")
    return event_response


@flow(name="External Event Handler Flow")
def external_event_handler_flow() -> Dict[str, Any]:
    """
    A flow that demonstrates:
    - External event handling
    - Event-driven responses
    - Asynchronous event processing
    """
    print("Starting external event handler flow")
    
    # Simulate receiving different types of events
    events = [
        {
            "type": "data.source.updated",
            "payload": {"source_id": "api_endpoint_1", "records": 42, "timestamp": "2024-01-01T10:00:00Z"}
        },
        {
            "type": "system.alert.high_cpu",
            "payload": {"cpu_percent": 85.5, "server": "worker-01", "timestamp": "2024-01-01T10:05:00Z"}
        },
        {
            "type": "data.quality.issue",
            "payload": {"source": "database_export", "issue_type": "null_values", "count": 15, "timestamp": "2024-01-01T10:10:00Z"}
        }
    ]
    
    results = []
    for event in events:
        response = simulate_external_event(event["type"], event["payload"])
        results.append(response)
    
    final_result = {
        "events_handled": len(events),
        "responses": results,
        "handled_at": datetime.now().isoformat()
    }
    
    print(f"Event handler flow completed: {final_result}")
    return final_result


if __name__ == "__main__":
    # Run the event-based processing flow
    print("=== Running Event-Based Processing Flow ===")
    result1 = event_based_processing_flow("api_endpoint_1")
    print(f"Result 1: {result1}")
    
    print("\n=== Running External Event Handler Flow ===")
    result2 = external_event_handler_flow()
    print(f"Result 2: {result2}")
    
    # For async flow, we need to run it in an async context
    print("\n=== Running Continuous Monitoring Flow (short duration) ===")
    import asyncio
    
    async def run_async_flow():
        result3 = await continuous_monitoring_flow(
            sources=["api_source_1", "db_export_2"],
            check_interval=2,  # Short interval for demo
            duration_minutes=0.1  # Very short duration for demo
        )
        print(f"Async flow result count: {len(result3)}")
        return result3
    
    # Run the async flow
    result3 = asyncio.run(run_async_flow())
    print(f"Result 3 sample: {result3[:2] if result3 else 'No results'}")