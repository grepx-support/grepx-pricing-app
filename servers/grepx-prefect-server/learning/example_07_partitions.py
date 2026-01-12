"""
Prefect Learning Example 07: Partitions
=======================================
Example demonstrating partition-based processing in Prefect flows
"""
from prefect import flow, task
from typing import List, Dict, Any
import asyncio
from datetime import datetime, timedelta


@task(name="create_partition_keys")
def create_partition_keys(
    partition_type: str = "date", 
    start_date: str = "2024-01-01", 
    end_date: str = "2024-01-05"
) -> List[str]:
    """Create partition keys based on type and date range"""
    print(f"Creating partition keys for type: {partition_type}")
    
    if partition_type == "date":
        from datetime import datetime, timedelta
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        partitions = []
        current = start
        while current <= end:
            partitions.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
    elif partition_type == "range":
        # Create numeric partitions
        partitions = [str(i) for i in range(int(start_date), int(end_date) + 1)]
    elif partition_type == "static":
        # Use predefined list
        partitions = start_date.split(",")  # Expect comma-separated values
    else:
        partitions = [start_date]  # Default single partition
    
    print(f"Created {len(partitions)} partitions: {partitions}")
    return partitions


@task(name="process_partition")
def process_partition(
    partition_key: str, 
    partition_type: str = "date",
    workload_size: int = 10
) -> Dict[str, Any]:
    """Process a single partition"""
    print(f"Processing partition: {partition_key} (type: {partition_type})")
    
    # Simulate different processing based on partition type
    if partition_type == "date":
        import random
        processed_records = random.randint(workload_size, workload_size * 2)
        processing_time = round(random.uniform(0.5, 2.0), 2)
        
        result = {
            "partition_key": partition_key,
            "partition_type": partition_type,
            "processed_records": processed_records,
            "processing_time_seconds": processing_time,
            "status": "completed",
            "completed_at": datetime.now().isoformat()
        }
    elif partition_type == "range":
        # Numeric processing
        partition_num = int(partition_key)
        processed_records = partition_num * workload_size
        processing_time = round(partition_num * 0.1, 2)
        
        result = {
            "partition_key": partition_key,
            "partition_type": partition_type,
            "processed_records": processed_records,
            "processing_time_seconds": processing_time,
            "status": "completed",
            "completed_at": datetime.now().isoformat()
        }
    else:
        # Default processing
        result = {
            "partition_key": partition_key,
            "partition_type": partition_type,
            "processed_records": workload_size,
            "processing_time_seconds": 0.5,
            "status": "completed",
            "completed_at": datetime.now().isoformat()
        }
    
    print(f"Partition {partition_key} processed: {result}")
    return result


@task(name="aggregate_partition_results")
def aggregate_partition_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate results from all partitions"""
    print(f"Aggregating results from {len(results)} partitions")
    
    if not results:
        return {
            "total_partitions": 0,
            "total_records_processed": 0,
            "total_processing_time": 0,
            "average_processing_time": 0,
            "status_summary": {}
        }
    
    total_records = sum(r.get("processed_records", 0) for r in results)
    total_time = sum(r.get("processing_time_seconds", 0) for r in results)
    avg_time = total_time / len(results) if results else 0
    
    # Count statuses
    status_counts = {}
    for result in results:
        status = result.get("status", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
    
    aggregated = {
        "total_partitions": len(results),
        "total_records_processed": total_records,
        "total_processing_time": round(total_time, 2),
        "average_processing_time": round(avg_time, 2),
        "status_summary": status_counts,
        "aggregated_at": datetime.now().isoformat()
    }
    
    print(f"Aggregated results: {aggregated}")
    return aggregated


@flow(name="Partition Processing Flow")
def partition_processing_flow(
    partition_type: str = "date",
    start_date: str = "2024-01-01",
    end_date: str = "2024-01-03",
    workload_size: int = 10
) -> Dict[str, Any]:
    """
    A flow that demonstrates:
    - Creating partition keys
    - Processing each partition independently
    - Aggregating partition results
    """
    print(f"Starting partition processing flow")
    print(f"Type: {partition_type}, Range: {start_date} to {end_date}")
    
    # Create partition keys
    partition_keys = create_partition_keys(partition_type, start_date, end_date)
    
    # Process each partition
    partition_results = []
    for partition_key in partition_keys:
        result = process_partition(partition_key, partition_type, workload_size)
        partition_results.append(result)
    
    # Aggregate results
    aggregated_results = aggregate_partition_results(partition_results)
    
    final_result = {
        "partition_type": partition_type,
        "partition_range": {"start": start_date, "end": end_date},
        "individual_results": partition_results,
        "aggregated_results": aggregated_results
    }
    
    print(f"Partition flow completed: {final_result}")
    return final_result


# Parallel partition processing example
@flow(name="Parallel Partition Processing Flow")
async def parallel_partition_processing_flow(
    partition_type: str = "date",
    start_date: str = "2024-01-01",
    end_date: str = "2024-01-03",
    workload_size: int = 10
) -> Dict[str, Any]:
    """
    A flow that demonstrates:
    - Parallel processing of partitions
    - Concurrent execution of partition tasks
    - Performance benefits of parallelism
    """
    print(f"Starting parallel partition processing flow")
    print(f"Type: {partition_type}, Range: {start_date} to {end_date}")
    
    # Create partition keys
    partition_keys = create_partition_keys(partition_type, start_date, end_date)
    
    # Process partitions in parallel using asyncio
    tasks = []
    for partition_key in partition_keys:
        # Create an async task for each partition
        task = process_partition(partition_key, partition_type, workload_size)
        tasks.append(task)
    
    # Execute all tasks concurrently
    partition_results = []
    for task in tasks:
        result = task  # In Prefect, tasks return their result directly
        partition_results.append(result)
    
    # Aggregate results
    aggregated_results = aggregate_partition_results(partition_results)
    
    final_result = {
        "execution_mode": "parallel",
        "partition_type": partition_type,
        "partition_range": {"start": start_date, "end": end_date},
        "individual_results": partition_results,
        "aggregated_results": aggregated_results
    }
    
    print(f"Parallel partition flow completed: {final_result}")
    return final_result


# Different partition strategies
@flow(name="Multiple Partition Strategies Flow")
def multiple_partition_strategies_flow() -> Dict[str, Any]:
    """
    A flow that demonstrates:
    - Different partitioning strategies
    - Date-based partitions
    - Range-based partitions
    - Static partitions
    """
    print("Starting multiple partition strategies flow")
    
    results = {}
    
    # Date-based partitions
    print("\n--- Date-based partitions ---")
    date_results = partition_processing_flow(
        partition_type="date",
        start_date="2024-01-01",
        end_date="2024-01-02",
        workload_size=5
    )
    results["date_partitions"] = date_results
    
    # Range-based partitions
    print("\n--- Range-based partitions ---")
    range_results = partition_processing_flow(
        partition_type="range",
        start_date="1",
        end_date="3",
        workload_size=10
    )
    results["range_partitions"] = range_results
    
    # Static partitions
    print("\n--- Static partitions ---")
    static_results = partition_processing_flow(
        partition_type="static",
        start_date="region_us,region_eu,region_asia",
        end_date="placeholder",  # Not used for static partitions
        workload_size=15
    )
    results["static_partitions"] = static_results
    
    summary = {
        "date_partition_total_records": results["date_partitions"]["aggregated_results"]["total_records_processed"],
        "range_partition_total_records": results["range_partitions"]["aggregated_results"]["total_records_processed"],
        "static_partition_total_records": results["static_partitions"]["aggregated_results"]["total_records_processed"],
        "completed_at": datetime.now().isoformat()
    }
    
    print(f"Multiple strategies flow completed: {summary}")
    return {
        "strategy_results": results,
        "summary": summary
    }


if __name__ == "__main__":
    # Run the partition processing flow
    print("=== Running Partition Processing Flow ===")
    result1 = partition_processing_flow(
        partition_type="date",
        start_date="2024-01-01",
        end_date="2024-01-02",
        workload_size=8
    )
    print(f"Result 1: {result1}")
    
    print("\n=== Running Multiple Partition Strategies Flow ===")
    result2 = multiple_partition_strategies_flow()
    print(f"Result 2: Total strategies: {len(result2['strategy_results'])}")
    
    # Run the parallel version
    print("\n=== Running Parallel Partition Processing Flow ===")
    import asyncio
    
    async def run_parallel():
        result3 = await parallel_partition_processing_flow(
            partition_type="date",
            start_date="2024-01-01",
            end_date="2024-01-02",
            workload_size=6
        )
        return result3
    
    result3 = asyncio.run(run_parallel())
    print(f"Result 3: {result3}")