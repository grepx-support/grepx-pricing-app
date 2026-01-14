"""
Additional Prefect tasks that need to be registered with Celery
"""
from celery import current_app
import logging
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)


def register_tasks():
    """Register all Prefect-related tasks with the current Celery app"""
    app = current_app._get_current_object()
    
    @app.task(name='prefect.start')
    def prefect_start(flow_name: str = "test_flow") -> Dict[str, Any]:
        """
        Main entry point task for Prefect flows
        
        Args:
            flow_name: Name of the flow being executed
            
        Returns:
            Dictionary with initialization data
        """
        result = {
            "task": "prefect.start",
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
    
    @app.task(name='prefect.process')
    def prefect_process(input_data=None) -> Dict[str, Any]:
        """Process data task"""
        if input_data is None:
            input_data = [1, 2, 3, 4, 5]
        
        total = sum(input_data)
        
        result = {
            "task": "prefect.process",
            "input_data": input_data,
            "processed_time": datetime.utcnow().isoformat(),
            "message": "Data processed successfully",
            "status": "success",
            "data": {
                "sum": total,
                "count": len(input_data)
            }
        }
        
        logger.info(f"[Prefect] Processed {len(input_data)} items: sum={total}")
        return result
    
    @app.task(name='prefect.finalize')
    def prefect_finalize(summary_data=None) -> Dict[str, Any]:
        """Finalize workflow task"""
        result = {
            "task": "prefect.finalize",
            "finalized_time": datetime.utcnow().isoformat(),
            "message": "Workflow completed successfully",
            "status": "success",
            "data": {
                "workflow_status": "completed",
                "final_result": "All tasks completed successfully"
            }
        }
        
        logger.info(f"[Prefect] Workflow finalized")
        return result
    
    # Return the task names that were registered
    return ['prefect.start', 'prefect.process', 'prefect.finalize']