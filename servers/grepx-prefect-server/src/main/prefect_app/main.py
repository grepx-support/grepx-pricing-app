"""
Main entry point for Grepx Prefect Server
Loads flows, tasks, deployments, and work pools from database and registers them with Prefect
"""
import logging
import sys
from pathlib import Path
from typing import Dict, List, Any

from .config_loader import ConfigLoader
from .database_manager import DatabaseManager
from .task_client import TaskClient
from .factories import DynamicFlowFactory, DynamicTaskFactory
from .builders import DeploymentBuilder, WorkPoolManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PrefectServerOrchestrator:
    """
    Main orchestrator for Prefect server
    Loads configuration from database and creates flows, deployments, and work pools
    """

    def __init__(self, config_loader: ConfigLoader):
        """
        Initialize orchestrator

        Args:
            config_loader: ConfigLoader instance
        """
        self.config_loader = config_loader
        self.config = config_loader.load_config()

        # Initialize components
        db_url = self.config['database']['db_url']
        broker_url = self.config['celery']['broker_url']

        self.db_manager = DatabaseManager(db_url=db_url)
        self.task_client = TaskClient(broker_url=broker_url)
        self.task_factory = DynamicTaskFactory(task_client=self.task_client)
        self.flow_factory = DynamicFlowFactory(task_factory=self.task_factory)
        self.deployment_builder = DeploymentBuilder()
        self.work_pool_manager = WorkPoolManager()

        # Storage for created objects
        self.flows = {}
        self.deployments = {}

    def setup_work_pools(self):
        """Create work pools from database configuration"""
        logger.info("Setting up work pools...")

        work_pools = self.db_manager.get_work_pools()
        logger.info(f"Found {len(work_pools)} work pools in database")

        for pool_config in work_pools:
            try:
                pool_id = self.work_pool_manager.create_work_pool(
                    name=pool_config['name'],
                    pool_type=pool_config.get('pool_type', 'process'),
                    description=pool_config.get('description'),
                    concurrency_limit=pool_config.get('concurrency_limit', 10),
                    config=pool_config.get('config', {})
                )
                if pool_id:
                    logger.info(f"✓ Work pool '{pool_config['name']}' ready")
            except Exception as e:
                logger.error(f"✗ Failed to create work pool '{pool_config['name']}': {str(e)}")

    def create_flows(self):
        """Create Prefect flows from database configuration"""
        logger.info("Creating flows...")

        flows = self.db_manager.get_flows()
        logger.info(f"Found {len(flows)} flows in database")

        for flow_config in flows:
            try:
                flow_id = flow_config['id']
                flow_name = flow_config['name']

                # Get tasks for this flow
                tasks = self.db_manager.get_tasks(flow_id=flow_id)
                logger.info(f"Flow '{flow_name}' has {len(tasks)} tasks")

                if not tasks:
                    logger.warning(f"Flow '{flow_name}' has no tasks, skipping")
                    continue

                # Create the flow function
                flow_function = self.flow_factory.create_flow_function(
                    flow_name=flow_name,
                    flow_tasks=tasks,
                    description=flow_config.get('description'),
                    tags=flow_config.get('tags', [])
                )

                self.flows[flow_name] = {
                    'function': flow_function,
                    'config': flow_config
                }
                logger.info(f"✓ Created flow: {flow_name}")

            except Exception as e:
                logger.error(f"✗ Failed to create flow '{flow_config.get('name')}': {str(e)}")

    def create_deployments(self):
        """Create Prefect deployments from database configuration"""
        logger.info("Creating deployments...")

        deployments = self.db_manager.get_deployments()
        logger.info(f"Found {len(deployments)} deployments in database")

        for deployment_config in deployments:
            try:
                deployment_name = deployment_config['name']
                flow_id = deployment_config['flow_id']

                # Get the flow for this deployment
                flow_info = self.db_manager.get_flow_by_id(flow_id)
                if not flow_info:
                    logger.error(f"Flow ID {flow_id} not found for deployment '{deployment_name}'")
                    continue

                flow_name = flow_info['name']
                if flow_name not in self.flows:
                    logger.error(f"Flow '{flow_name}' not created, cannot create deployment '{deployment_name}'")
                    continue

                flow_function = self.flows[flow_name]['function']

                # Create the deployment
                deployment_id = self.deployment_builder.create_deployment(
                    deployment_name=deployment_name,
                    flow_function=flow_function,
                    work_pool_name=deployment_config['work_pool_name'],
                    work_queue_name=deployment_config.get('work_queue_name', 'default'),
                    schedule_type=deployment_config.get('schedule_type'),
                    schedule_config=deployment_config.get('schedule_config', {}),
                    parameters=deployment_config.get('parameters', {}),
                    tags=deployment_config.get('tags', []),
                    description=deployment_config.get('description')
                )

                if deployment_id:
                    self.deployments[deployment_name] = {
                        'id': deployment_id,
                        'flow_name': flow_name,
                        'config': deployment_config
                    }
                    logger.info(f"✓ Created deployment: {deployment_name}")

            except Exception as e:
                logger.error(f"✗ Failed to create deployment '{deployment_config.get('name')}': {str(e)}")

    def run(self):
        """Run the orchestrator - setup work pools, create flows, and deployments"""
        try:
            logger.info("="*80)
            logger.info("Grepx Prefect Server Orchestrator Starting")
            logger.info("="*80)

            # Step 1: Setup work pools
            self.setup_work_pools()

            # Step 2: Create flows
            self.create_flows()

            # Step 3: Create deployments
            self.create_deployments()

            logger.info("="*80)
            logger.info("Orchestrator Setup Complete")
            logger.info(f"Created {len(self.flows)} flows")
            logger.info(f"Created {len(self.deployments)} deployments")
            logger.info("="*80)

            # Print summary
            if self.flows:
                logger.info("\nRegistered Flows:")
                for flow_name in self.flows:
                    logger.info(f"  - {flow_name}")

            if self.deployments:
                logger.info("\nRegistered Deployments:")
                for deployment_name, info in self.deployments.items():
                    logger.info(f"  - {deployment_name} (flow: {info['flow_name']})")

            logger.info("\n✓ Prefect server is ready!")
            logger.info("To start a worker, run: prefect worker start --pool <pool-name>")

        except Exception as e:
            logger.error(f"Error running orchestrator: {str(e)}")
            raise


def main():
    """Main entry point"""
    try:
        config_loader = ConfigLoader()
        orchestrator = PrefectServerOrchestrator(config_loader)
        orchestrator.run()
    except Exception as e:
        logger.error(f"Failed to start Prefect server: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
