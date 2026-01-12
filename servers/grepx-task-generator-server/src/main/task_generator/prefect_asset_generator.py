"""
Prefect Flow/Deployment Generator - Creates and registers Prefect flows in the database
"""

from typing import Dict, List, Optional, Any
from datetime import datetime

from sqlalchemy.exc import IntegrityError


class PrefectFlowGenerator:
    """
    Generates and registers Prefect flows/deployments in the database
    """

    def __init__(self, db_manager):
        """
        Initialize Prefect flow generator

        Args:
            db_manager: DatabaseManager instance
        """
        self.db_manager = db_manager

    def create_flow(
        self,
        name: str,
        entrypoint: str,
        description: str = "",
        group_name: Optional[str] = None,
        artifact_type: Optional[str] = None,
        dependencies: List[str] = None,
        config: Dict[str, Any] = None,
        parameters: Dict[str, Any] = None,
        work_pool_name: Optional[str] = None,
        deployment_name: Optional[str] = None,
        partition_type: Optional[str] = None,
        partition_config: Dict[str, Any] = None,
        is_active: bool = True,
    ) -> bool:
        """
        Create a Prefect flow/deployment record in the database

        Args:
            name: Logical name (e.g. "generic-asset-etl-futures")
            entrypoint: Python entrypoint "module.submodule:function"
                        e.g. "prefect_app.flows.etl_price_flow:generic_asset_etl"
            description: Human description
            group_name: Group name ("stocks", "futures", "indices")
            artifact_type: Domain type ("prices", "indicators", "generic-etl")
            dependencies: Other logical artifact names this depends on
            config: Static configuration (e.g. asset.yaml subset)
            parameters: Default Prefect flow parameters
            work_pool_name: Prefect work pool name ("price-pool")
            deployment_name: Prefect deployment name ("generic-asset-etl")
            partition_type: e.g. "time", "symbol", or None
            partition_config: Partition configuration
            is_active: Whether this flow definition is active

        Returns:
            True if created, False if exists or failed
        """
        from grepx_models import PrefectArtifact

        with self.db_manager.get_session() as session:
            try:
                # Check if record already exists
                existing = session.query(PrefectArtifact).filter_by(name=name).first()
                if existing:
                    print(f"Prefect flow '{name}' already exists, skipping...")
                    return False

                # (Optional) you could validate entrypoint here by trying importlib

                artifact = PrefectArtifact(
                    name=name,
                    description=description,
                    group_name=group_name,
                    artifact_type=artifact_type,
                    dependencies=dependencies or [],
                    config=config or {},
                    entrypoint=entrypoint,
                    deployment_name=deployment_name,
                    work_pool_name=work_pool_name,
                    parameters=parameters or {},
                    partition_type=partition_type,
                    partition_config=partition_config or {},
                    is_active=is_active,
                    created_at=datetime.now(),
                )

                session.add(artifact)
                session.commit()
                print(f"Created Prefect flow: {name} -> {entrypoint}")
                return True

            except IntegrityError:
                session.rollback()
                print(f"Failed to create Prefect flow '{name}' (integrity error)")
                return False
            except Exception as e:
                session.rollback()
                print(f"Failed to create Prefect flow '{name}': {e}")
                return False

    def create_flows_batch(self, flows: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Create multiple flows in batch

        Args:
            flows: List of flow configuration dicts (same keys as create_flow)

        Returns:
            Dict with success and failure counts
        """
        results = {"success": 0, "failed": 0, "skipped": 0}

        for flow_cfg in flows:
            result = self.create_flow(**flow_cfg)
            if result:
                results["success"] += 1
            else:
                # We already print inside create_flow; treat "exists" also as skipped
                # Simplify: count everything that returned False as failed or skipped
                # If you want exact "skipped" vs "failed", you can change return type
                results["failed"] += 1

        return results

    def update_flow(self, name: str, **kwargs) -> bool:
        """
        Update an existing Prefect flow/deployment record

        Args:
            name: Flow name
            **kwargs: Fields to update

        Returns:
            True if updated successfully
        """
        from grepx_models import PrefectArtifact

        with self.db_manager.get_session() as session:
            try:
                artifact = session.query(PrefectArtifact).filter_by(name=name).first()
                if not artifact:
                    print(f"Prefect flow '{name}' not found")
                    return False

                for key, value in kwargs.items():
                    if hasattr(artifact, key):
                        setattr(artifact, key, value)

                session.commit()
                print(f"Updated Prefect flow: {name}")
                return True
            except Exception as e:
                session.rollback()
                print(f"Failed to update Prefect flow '{name}': {e}")
                return False

    def deactivate_flow(self, name: str) -> bool:
        """Deactivate a Prefect flow definition"""
        return self.update_flow(name, is_active=False)

    def activate_flow(self, name: str) -> bool:
        """Activate a Prefect flow definition"""
        return self.update_flow(name, is_active=True)
