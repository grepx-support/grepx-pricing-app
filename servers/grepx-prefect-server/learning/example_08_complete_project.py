"""
Prefect Learning Example 08: Complete Project
=============================================
Complete end-to-end example combining all Prefect concepts
"""
from prefect import flow, task
from prefect.schedules import IntervalSchedule, CronSchedule
from prefect.deployments import Deployment
from prefect.blocks.system import JSON
from datetime import datetime, timedelta
from typing import Dict, Any, List
import asyncio
import random


@task(name="extract_data")
def extract_data(source_type: str, date_partition: str = None) -> Dict[str, Any]:
    """Extract data from various sources"""
    print(f"Extracting data from {source_type} for partition {date_partition or 'latest'}")
    
    # Simulate extraction
    record_count = random.randint(100, 1000)
    extraction_time = round(random.uniform(1.0, 5.0), 2)
    
    extracted_data = {
        "source": source_type,
        "partition": date_partition,
        "records_extracted": record_count,
        "extraction_time_seconds": extraction_time,
        "status": "completed",
        "extracted_at": datetime.now().isoformat()
    }
    
    print(f"Extraction completed: {extracted_data}")
    return extracted_data


@task(name="transform_data")
def transform_data(extracted_data: Dict[str, Any], transformations: List[str] = None) -> Dict[str, Any]:
    """Transform extracted data"""
    if transformations is None:
        transformations = ["clean", "normalize", "enrich"]
    
    print(f"Transforming data with operations: {transformations}")
    
    # Simulate transformation
    transformed_count = extracted_data["records_extracted"]
    transform_time = round(random.uniform(2.0, 8.0), 2)
    
    transformed_data = {
        **extracted_data,
        "records_transformed": transformed_count,
        "transformations_applied": transformations,
        "transformation_time_seconds": transform_time,
        "status": "completed",
        "transformed_at": datetime.now().isoformat()
    }
    
    print(f"Transformation completed: {transformed_data}")
    return transformed_data


@task(name="load_data")
def load_data(transformed_data: Dict[str, Any], destination: str) -> Dict[str, Any]:
    """Load transformed data to destination"""
    print(f"Loading data to {destination}")
    
    # Simulate loading
    loaded_count = transformed_data["records_transformed"]
    load_time = round(random.uniform(1.0, 4.0), 2)
    
    loaded_data = {
        **transformed_data,
        "destination": destination,
        "records_loaded": loaded_count,
        "load_time_seconds": load_time,
        "status": "completed",
        "loaded_at": datetime.now().isoformat()
    }
    
    print(f"Loading completed: {loaded_data}")
    return loaded_data


@task(name="validate_data_quality")
def validate_data_quality(loaded_data: Dict[str, Any]) -> Dict[str, Any]:
    """Validate data quality after loading"""
    print(f"Validating data quality for {loaded_data['destination']}")
    
    # Simulate validation
    quality_score = round(random.uniform(85.0, 99.5), 2)
    validation_passed = quality_score >= 90.0
    
    validation_result = {
        **loaded_data,
        "quality_score": quality_score,
        "validation_passed": validation_passed,
        "issues_found": [] if validation_passed else ["low_quality_data"],
        "validated_at": datetime.now().isoformat()
    }
    
    print(f"Validation completed: {validation_result}")
    return validation_result


@task(name="send_success_notification")
def send_success_notification(validation_result: Dict[str, Any]) -> str:
    """Send notification about successful ETL"""
    if validation_result["validation_passed"]:
        message = f"ETL completed successfully! Loaded {validation_result['records_loaded']} records to {validation_result['destination']}"
    else:
        message = f"ETL completed with warnings. Quality score: {validation_result['quality_score']}"
    
    print(f"Notification: {message}")
    return message


@task(name="send_failure_notification")
def send_failure_notification(error: str, source: str) -> str:
    """Send notification about ETL failure"""
    message = f"ETL failed for {source}. Error: {error}"
    print(f"Failure notification: {message}")
    return message


@flow(name="ETL Flow")
def etl_flow(
    source_type: str = "api",
    destination: str = "warehouse",
    date_partition: str = None,
    transformations: List[str] = None
) -> Dict[str, Any]:
    """
    Complete ETL flow demonstrating:
    - Extract, Transform, Load pattern
    - Data validation
    - Error handling
    - Success notifications
    """
    try:
        print(f"Starting ETL flow from {source_type} to {destination}")
        
        # Extract
        extracted = extract_data(source_type, date_partition)
        
        # Transform
        transformed = transform_data(extracted, transformations)
        
        # Load
        loaded = load_data(transformed, destination)
        
        # Validate
        validation_result = validate_data_quality(loaded)
        
        # Send success notification
        notification = send_success_notification(validation_result)
        
        result = {
            "status": "success",
            "etl_result": validation_result,
            "notification_sent": notification
        }
        
    except Exception as e:
        # Send failure notification
        notification = send_failure_notification(str(e), source_type)
        result = {
            "status": "failed",
            "error": str(e),
            "notification_sent": notification
        }
    
    print(f"ETL flow completed: {result}")
    return result


@flow(name="Multi-Source ETL Orchestrator")
def multi_source_etl_orchestrator(
    sources: List[Dict[str, Any]],
    destination: str = "central_warehouse"
) -> Dict[str, Any]:
    """
    Orchestrates ETL from multiple sources demonstrating:
    - Parallel execution
    - Error handling across multiple flows
    - Aggregated results
    """
    print(f"Starting multi-source ETL orchestrator for {len(sources)} sources")
    
    results = []
    for source_config in sources:
        result = etl_flow(
            source_type=source_config["type"],
            destination=destination,
            date_partition=source_config.get("partition"),
            transformations=source_config.get("transformations", ["clean", "normalize"])
        )
        results.append({
            "source_config": source_config,
            "result": result
        })
    
    # Count successes and failures
    successful = sum(1 for r in results if r["result"]["status"] == "success")
    failed = len(results) - successful
    
    summary = {
        "total_sources": len(sources),
        "successful_etls": successful,
        "failed_etls": failed,
        "success_rate": round(successful / len(sources) * 100, 2) if sources else 0,
        "completed_at": datetime.now().isoformat(),
        "individual_results": results
    }
    
    print(f"Multi-source ETL completed: {summary}")
    return summary


# Deployment configuration
def configure_deployments():
    """
    Configure deployments for different ETL scenarios
    """
    print("Configuring ETL deployments...")
    
    # Daily ETL deployment
    daily_etl = Deployment.build_from_flow(
        flow=etl_flow,
        name="daily-etl-pipeline",
        schedule=CronSchedule(cron="0 2 * * *", timezone="UTC"),  # Daily at 2 AM
        work_pool_name="default-agent-pool",
        parameters={
            "source_type": "production_api",
            "destination": "data_warehouse",
            "transformations": ["clean", "normalize", "enrich"]
        },
        description="Daily ETL pipeline for production data"
    )
    
    print("Daily ETL deployment configured")
    
    # Hourly monitoring deployment
    monitoring_etl = Deployment.build_from_flow(
        flow=etl_flow,
        name="hourly-monitoring-etl",
        schedule=IntervalSchedule(interval=timedelta(hours=1)),
        work_pool_name="default-agent-pool",
        parameters={
            "source_type": "monitoring_logs",
            "destination": "analytics_db",
            "transformations": ["parse", "aggregate"]
        },
        description="Hourly monitoring and aggregation ETL"
    )
    
    print("Monitoring ETL deployment configured")
    
    # Real-time processing deployment
    real_time_etl = Deployment.build_from_flow(
        flow=etl_flow,
        name="real-time-processing",
        schedule=IntervalSchedule(interval=timedelta(minutes=5)),
        work_pool_name="default-agent-pool",
        parameters={
            "source_type": "streaming_data",
            "destination": "realtime_db",
            "transformations": ["validate", "enrich"]
        },
        description="Real-time data processing ETL"
    )
    
    print("Real-time ETL deployment configured")
    
    deployments = [daily_etl, monitoring_etl, real_time_etl]
    
    for deployment in deployments:
        print(f"Deployment ready: {deployment.name}")
    
    return deployments


@flow(name="Complete ETL Project Flow")
def complete_etl_project_flow(setup_deployments: bool = True) -> Dict[str, Any]:
    """
    Complete project flow demonstrating:
    - All Prefect concepts combined
    - End-to-end ETL process
    - Deployment configuration
    - Multi-source orchestration
    """
    print("Starting complete ETL project flow")
    
    results = {}
    
    # Configure deployments if requested
    if setup_deployments:
        print("\n--- Configuring Deployments ---")
        deployments = configure_deployments()
        results["deployments_configured"] = len(deployments)
        results["deployment_details"] = [d.name for d in deployments]
    
    # Run a single ETL flow
    print("\n--- Running Single ETL Flow ---")
    single_result = etl_flow(
        source_type="test_api",
        destination="test_warehouse",
        date_partition="2024-01-01",
        transformations=["clean", "normalize", "validate"]
    )
    results["single_etl_result"] = single_result
    
    # Run multi-source ETL
    print("\n--- Running Multi-Source ETL ---")
    sources_config = [
        {"type": "users_api", "partition": "2024-01-01", "transformations": ["clean", "enrich"]},
        {"type": "orders_api", "partition": "2024-01-01", "transformations": ["aggregate", "summarize"]},
        {"type": "products_api", "partition": "2024-01-01", "transformations": ["normalize", "validate"]}
    ]
    
    multi_result = multi_source_etl_orchestrator(sources_config, "central_analytics")
    results["multi_etl_result"] = multi_result
    
    # Final summary
    final_summary = {
        "project_stage": "complete",
        "deployments_configured": results.get("deployments_configured", 0),
        "single_etl_status": results["single_etl_result"]["status"],
        "multi_etl_summary": {
            "total_sources": results["multi_etl_result"]["total_sources"],
            "success_rate": results["multi_etl_result"]["success_rate"]
        },
        "completed_at": datetime.now().isoformat(),
        "overall_status": "success"
    }
    
    print(f"\nComplete ETL project completed: {final_summary}")
    return final_summary


if __name__ == "__main__":
    # Run the complete ETL project flow
    print("=== Starting Complete ETL Project ===")
    result = complete_etl_project_flow(setup_deployments=True)
    print(f"\nFinal Result: {result}")
    
    print("\n=== Demonstrating Individual Components ===")
    
    # Run a single ETL
    single_result = etl_flow(
        source_type="sales_api",
        destination="sales_warehouse",
        date_partition="2024-01-15",
        transformations=["clean", "aggregate", "enrich"]
    )
    print(f"Single ETL result: {single_result}")
    
    # Run multi-source ETL
    sources = [
        {"type": "web_events", "partition": "2024-01-15"},
        {"type": "mobile_events", "partition": "2024-01-15"}
    ]
    multi_result = multi_source_etl_orchestrator(sources, "event_analytics")
    print(f"Multi-source ETL result: {multi_result}")