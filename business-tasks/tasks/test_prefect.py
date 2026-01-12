"""
Test tasks for Prefect flow demonstration
Simple tasks that demonstrate Prefect workflow orchestration with Celery
"""
import logging
from datetime import datetime
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


def prefect_task_start(flow_name: str = "test_flow") -> Dict[str, Any]:
    """
    Starting task for Prefect flow - initializes the workflow

    Args:
        flow_name: Name of the flow being executed

    Returns:
        Dictionary with initialization data
    """
    result = {
        "task": "prefect_task_start",
        "flow_name": flow_name,
        "start_time": datetime.utcnow().isoformat(),
        "message": f"Starting Prefect flow: {flow_name}",
        "status": "success",
        "data": {
            "initialized": True,
            "workflow_id": f"workflow_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        }
    }

    logger.info(f"[Prefect] {result['message']}")
    return result


def prefect_task_process_data(input_data: List[int] = None) -> Dict[str, Any]:
    """
    Middle task - processes data passed from previous task

    Args:
        input_data: List of numbers to process

    Returns:
        Dictionary with processed data
    """
    if input_data is None:
        input_data = [1, 2, 3, 4, 5]

    total = sum(input_data)
    average = total / len(input_data) if input_data else 0

    result = {
        "task": "prefect_task_process_data",
        "input_data": input_data,
        "processed_time": datetime.utcnow().isoformat(),
        "message": "Data processed successfully",
        "status": "success",
        "data": {
            "sum": total,
            "average": average,
            "count": len(input_data),
            "max": max(input_data) if input_data else 0,
            "min": min(input_data) if input_data else 0
        }
    }

    logger.info(f"[Prefect] Processed {len(input_data)} items: sum={total}, avg={average}")
    return result


def prefect_task_validate(threshold: int = 10) -> Dict[str, Any]:
    """
    Validation task - runs in parallel with process_data

    Args:
        threshold: Validation threshold

    Returns:
        Dictionary with validation results
    """
    result = {
        "task": "prefect_task_validate",
        "validation_time": datetime.utcnow().isoformat(),
        "message": "Validation completed",
        "status": "success",
        "data": {
            "threshold": threshold,
            "validation_passed": True,
            "checks_performed": ["type_check", "range_check", "null_check"],
            "checks_passed": 3,
            "checks_failed": 0
        }
    }

    logger.info(f"[Prefect] Validation passed with threshold={threshold}")
    return result


def prefect_task_finalize(summary_data: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Final task - aggregates results and finalizes the workflow
    Depends on both process_data and validate tasks

    Args:
        summary_data: Summary data from previous tasks

    Returns:
        Dictionary with final results
    """
    if summary_data is None:
        summary_data = {}

    result = {
        "task": "prefect_task_finalize",
        "finalized_time": datetime.utcnow().isoformat(),
        "message": "Workflow completed successfully",
        "status": "success",
        "data": {
            "workflow_status": "completed",
            "tasks_executed": 4,
            "total_duration_estimate": "< 1 minute",
            "summary": summary_data,
            "final_result": "All tasks completed successfully"
        }
    }

    logger.info(f"[Prefect] Workflow finalized: {result['data']['final_result']}")
    return result
