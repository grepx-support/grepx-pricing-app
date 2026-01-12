"""
Entry point for Prefect workers to load dynamically configured flows
This file is imported by workers and initializes the flow at module load time
"""
import logging
import sys
from pathlib import Path

# Add parent directory to path to import prefect_app modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from prefect_app.config_loader import ConfigLoader
from prefect_app.database_manager import DatabaseManager
from prefect_app.task_client import TaskClient
from prefect_app.factories.task_factory import DynamicTaskFactory
from prefect_app.factories.flow_factory import DynamicFlowFactory

logger = logging.getLogger(__name__)

# Initialize at module import time (when worker loads this)
try:
    config_loader = ConfigLoader()
    config = config_loader.load_config()

    db_url = config['database']['db_url']
    broker_url = config['celery']['broker_url']

    # Create necessary instances
    db_manager = DatabaseManager(db_url=db_url)
    task_client = TaskClient(broker_url=broker_url)
    task_factory = DynamicTaskFactory(task_client=task_client)
    flow_factory = DynamicFlowFactory(task_factory=task_factory)

    # Load the first flow from database (or specific flow by name/id)
    flows = db_manager.get_flows()
    
    if flows:
        flow_config = flows[0]  # TODO: Make this configurable to load specific flow
        flow_id = flow_config['id']
        flow_name = flow_config['name']
        
        # Get tasks for this flow
        tasks = db_manager.get_tasks(flow_id=flow_id)
        
        if tasks:
            # Create the flow - this MUST happen at module import time
            prefect_flow_function = flow_factory.create_flow_function(
                flow_name=flow_name,
                flow_tasks=tasks,
                description=flow_config.get('description'),
                tags=flow_config.get('tags', [])
            )
            logger.info(f"✓ Initialized flow '{flow_name}' with {len(tasks)} tasks")
        else:
            logger.error(f"✗ No tasks found for flow '{flow_name}'")
            raise RuntimeError(f"No tasks found for flow '{flow_name}'")
    else:
        logger.error("✗ No flows found in database")
        raise RuntimeError("No flows found in database")
        
except Exception as e:
    logger.error(f"✗ Failed to initialize flow: {e}")
    raise