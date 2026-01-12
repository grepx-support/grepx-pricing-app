"""
Work Pool Manager for Prefect
Manages Prefect work pools programmatically
"""
from typing import Dict, Any, List, Optional
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate, WorkPoolUpdate
import logging
import asyncio

logger = logging.getLogger(__name__)


class WorkPoolManager:
    """
    Manages Prefect work pools
    """

    def __init__(self):
        """Initialize work pool manager"""
        pass

    async def create_work_pool_async(
        self,
        name: str,
        pool_type: str = "process",
        description: Optional[str] = None,
        concurrency_limit: int = 10,
        config: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Create a work pool asynchronously

        Args:
            name: Name of the work pool
            pool_type: Type of work pool ('process', 'docker', etc.)
            description: Optional description
            concurrency_limit: Maximum concurrent runs
            config: Pool-specific configuration

        Returns:
            Work pool ID if created, None otherwise
        """
        config = config or {}

        try:
            async with get_client() as client:
                # Check if work pool already exists
                try:
                    existing_pool = await client.read_work_pool(name)
                    logger.info(f"Work pool '{name}' already exists")
                    return str(existing_pool.id)
                except Exception:
                    # Pool doesn't exist, create it
                    pass

                # Create the work pool
                work_pool = WorkPoolCreate(
                    name=name,
                    type=pool_type,
                    description=description,
                    base_job_template=config.get('base_job_template', {}),
                    is_paused=False,
                    concurrency_limit=concurrency_limit
                )

                created_pool = await client.create_work_pool(work_pool)
                logger.info(f"Created work pool: {name} (ID: {created_pool.id})")
                return str(created_pool.id)

        except Exception as e:
            logger.error(f"Error creating work pool '{name}': {str(e)}")
            return None

    def create_work_pool(
        self,
        name: str,
        pool_type: str = "process",
        description: Optional[str] = None,
        concurrency_limit: int = 10,
        config: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """
        Create a work pool (synchronous wrapper)

        Args:
            name: Name of the work pool
            pool_type: Type of work pool
            description: Optional description
            concurrency_limit: Maximum concurrent runs
            config: Pool-specific configuration

        Returns:
            Work pool ID if created, None otherwise
        """
        return asyncio.run(
            self.create_work_pool_async(
                name=name,
                pool_type=pool_type,
                description=description,
                concurrency_limit=concurrency_limit,
                config=config
            )
        )

    async def list_work_pools_async(self) -> List[Dict[str, Any]]:
        """List all work pools asynchronously"""
        try:
            async with get_client() as client:
                pools = await client.read_work_pools()
                return [
                    {
                        'id': str(pool.id),
                        'name': pool.name,
                        'type': pool.type,
                        'is_paused': pool.is_paused,
                        'concurrency_limit': pool.concurrency_limit
                    }
                    for pool in pools
                ]
        except Exception as e:
            logger.error(f"Error listing work pools: {str(e)}")
            return []

    def list_work_pools(self) -> List[Dict[str, Any]]:
        """List all work pools (synchronous wrapper)"""
        return asyncio.run(self.list_work_pools_async())
