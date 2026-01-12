"""
Prefect Learning Example 05: Schedules
======================================
Example demonstrating schedule management in Prefect flows
"""
from prefect import flow, task
from prefect.schedules import IntervalSchedule, CronSchedule
from prefect.deployments import Deployment
from datetime import datetime, timedelta
from typing import List, Dict, Any
import asyncio


@task(name="log_current_time")
def log_current_time() -> str:
    """Log the current time"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Execution at: {current_time}")
    return current_time


@task(name="fetch_data_batch")
def fetch_data_batch(batch_id: int) -> Dict[str, Any]:
    """Simulate fetching a batch of data"""
    print(f"Fetching data batch #{batch_id}")
    
    # Simulate data fetching
    data = {
        "batch_id": batch_id,
        "timestamp": datetime.now().isoformat(),
        "records_count": 100,
        "source": "external_api",
        "status": "completed"
    }
    
    print(f"Fetched batch data: {data}")
    return data


@task(name="process_data_batch")
def process_data_batch(batch_data: Dict[str, Any]) -> Dict[str, Any]:
    """Process the fetched data batch"""
    print(f"Processing batch: {batch_data['batch_id']}")
    
    # Simulate processing
    processed_data = {
        **batch_data,
        "processed_at": datetime.now().isoformat(),
        "processing_status": "success",
        "processed_records": batch_data["records_count"],
        "quality_score": 95.5
    }
    
    print(f"Processed batch: {processed_data}")
    return processed_data


@task(name="store_processed_data")
def store_processed_data(processed_data: Dict[str, Any]) -> str:
    """Store the processed data"""
    print(f"Storing processed data for batch: {processed_data['batch_id']}")
    
    # Simulate storing data
    storage_location = f"data_store/batch_{processed_data['batch_id']}.json"
    print(f"Data stored at: {storage_location}")
    
    return storage_location


@flow(name="Scheduled Data Processing Flow")
def scheduled_data_processing_flow(batch_id: int = None) -> Dict[str, Any]:
    """
    A flow that demonstrates:
    - Scheduled execution
    - Batch processing
    - Consistent workflow pattern
    """
    if batch_id is None:
        batch_id = int(datetime.now().timestamp())
    
    print(f"Starting scheduled data processing for batch: {batch_id}")
    
    # Log current time
    time_log = log_current_time()
    
    # Fetch data batch
    batch_data = fetch_data_batch(batch_id)
    
    # Process the batch
    processed_data = process_data_batch(batch_data)
    
    # Store the processed data
    storage_location = store_processed_data(processed_data)
    
    result = {
        "batch_id": batch_id,
        "executed_at": time_log,
        "storage_location": storage_location,
        "status": "completed"
    }
    
    print(f"Flow completed for batch {batch_id}: {result}")
    return result


# Advanced scheduling example
@flow(name="Advanced Scheduling Flow")
def advanced_scheduling_flow(job_type: str = "etl") -> Dict[str, Any]:
    """
    A flow that demonstrates:
    - Different scheduling strategies
    - Job type handling
    - Time-based logic
    """
    print(f"Running {job_type.upper()} job")
    
    current_time = log_current_time()
    
    # Different processing based on job type
    if job_type == "etl":
        print("Running ETL process...")
        result = {
            "job_type": "etl",
            "processed_tables": ["users", "orders", "products"],
            "records_processed": 15000,
            "duration_seconds": 120
        }
    elif job_type == "reporting":
        print("Running reporting process...")
        result = {
            "job_type": "reporting", 
            "reports_generated": ["daily_sales", "inventory_status", "customer_summary"],
            "generated_at": current_time,
            "recipients": ["managers", "analysts"]
        }
    elif job_type == "cleanup":
        print("Running cleanup process...")
        result = {
            "job_type": "cleanup",
            "cleaned_directories": ["temp", "cache", "logs"],
            "files_removed": 250,
            "disk_space_freed_mb": 150
        }
    else:
        print(f"Running generic job: {job_type}")
        result = {
            "job_type": job_type,
            "status": "completed",
            "timestamp": current_time
        }
    
    print(f"Advanced scheduling flow result: {result}")
    return result


# Create deployments with different schedules
def create_scheduled_deployments():
    """
    Create deployments with different scheduling patterns
    """
    print("Creating scheduled deployments...")
    
    # 1. Interval-based deployment (every 5 minutes)
    interval_deployment = Deployment.build_from_flow(
        flow=scheduled_data_processing_flow,
        name="interval-data-processing",
        schedule=IntervalSchedule(interval=timedelta(minutes=5)),
        work_pool_name="default-agent-pool",
        parameters={"batch_id": None},  # Will be auto-generated
        description="Processes data every 5 minutes"
    )
    
    print("Interval deployment created")
    
    # 2. Cron-based deployment (every hour on weekdays)
    hourly_deployment = Deployment.build_from_flow(
        flow=advanced_scheduling_flow,
        name="hourly-reporting-job",
        schedule=CronSchedule(cron="0 * * * 1-5", timezone="UTC"),  # Every hour Mon-Fri
        work_pool_name="default-agent-pool",
        parameters={"job_type": "reporting"},
        description="Runs reporting job every hour on weekdays"
    )
    
    print("Hourly deployment created")
    
    # 3. Daily cleanup job (at 2 AM UTC)
    cleanup_deployment = Deployment.build_from_flow(
        flow=advanced_scheduling_flow,
        name="daily-cleanup-job",
        schedule=CronSchedule(cron="0 2 * * *", timezone="UTC"),  # Daily at 2 AM
        work_pool_name="default-agent-pool",
        parameters={"job_type": "cleanup"},
        description="Performs daily cleanup operations"
    )
    
    print("Daily cleanup deployment created")
    
    # 4. Weekly summary job (Sunday at midnight)
    weekly_deployment = Deployment.build_from_flow(
        flow=advanced_scheduling_flow,
        name="weekly-summary-job",
        schedule=CronSchedule(cron="0 0 * * 0", timezone="UTC"),  # Weekly on Sunday
        work_pool_name="default-agent-pool",
        parameters={"job_type": "etl"},
        description="Generates weekly summary reports"
    )
    
    print("Weekly summary deployment created")
    
    deployments = [
        interval_deployment,
        hourly_deployment,
        cleanup_deployment,
        weekly_deployment
    ]
    
    # Apply deployments (in a real scenario, this would register them)
    for deployment in deployments:
        print(f"Deployment prepared: {deployment.name}")
    
    return deployments


@flow(name="Schedule Management Flow")
def schedule_management_flow(create_deployments: bool = True) -> List[Dict[str, Any]]:
    """
    A flow that demonstrates:
    - Schedule creation and management
    - Different scheduling patterns
    - Deployment configuration
    """
    print("Starting schedule management flow...")
    
    results = []
    
    if create_deployments:
        print("Creating scheduled deployments...")
        deployments = create_scheduled_deployments()
        
        # Return simplified deployment info
        deployment_info = []
        for i, deployment in enumerate(deployments):
            info = {
                "index": i,
                "name": deployment.name,
                "type": type(deployment.schedule).__name__,
                "applied": True  # Simulating that it would be applied
            }
            deployment_info.append(info)
        
        results.append({
            "action": "deployments_created",
            "count": len(deployments),
            "details": deployment_info
        })
    
    # Run a single instance of the scheduled flow
    single_run_result = scheduled_data_processing_flow(batch_id=999)
    results.append({
        "action": "single_run_executed",
        "result": single_run_result
    })
    
    print(f"Schedule management flow completed with {len(results)} actions")
    return results


if __name__ == "__main__":
    # Run the schedule management flow
    print("=== Running Schedule Management Flow ===")
    result1 = schedule_management_flow(create_deployments=True)
    print(f"Result 1: {result1}")
    
    # Run the advanced scheduling flow directly
    print("\n=== Running Advanced Scheduling Flow ===")
    result2 = advanced_scheduling_flow("etl")
    print(f"Result 2: {result2}")