"""
Dagster Asset Generator - Creates and registers Dagster assets in the database
"""
from typing import Dict, List, Optional, Any
from datetime import datetime
from sqlalchemy.exc import IntegrityError


class DagsterAssetGenerator:
    """
    Generates and registers Dagster assets in the database
    """
    
    def __init__(self, db_manager):
        """
        Initialize Dagster asset generator
        
        Args:
            db_manager: DatabaseManager instance
        """
        self.db_manager = db_manager
    
    def create_asset(
        self,
        name: str,
        celery_task_name: str,
        description: str = "",
        group_name: Optional[str] = None,
        asset_type: Optional[str] = None,
        dependencies: List[str] = None,
        config: Dict[str, Any] = None,
        task_args: List[Any] = None,
        task_kwargs: Dict[str, Any] = None,
        partition_type: Optional[str] = None,
        partition_config: Dict[str, Any] = None,
        is_active: bool = True
    ) -> bool:
        """
        Create a Dagster asset in the database
        
        Args:
            name: Asset name
            celery_task_name: Name of the Celery task to execute
            description: Asset description
            group_name: Asset group name
            asset_type: Type of asset (source, transformation, analytics, etc.)
            dependencies: List of asset dependencies
            config: Asset configuration dictionary
            task_args: Arguments to pass to Celery task
            task_kwargs: Keyword arguments to pass to Celery task
            partition_type: Partition type ('daily', 'static', or None)
            partition_config: Partition configuration
            is_active: Whether asset is active
        
        Returns:
            True if created successfully, False otherwise
        """
        from .models import Asset
        
        with self.db_manager.get_session() as session:
            try:
                # Check if asset already exists
                existing = session.query(Asset).filter_by(name=name).first()
                if existing:
                    print(f"Asset '{name}' already exists, skipping...")
                    return False
                
                # Verify Celery task exists
                from .models import CeleryTask
                task = session.query(CeleryTask).filter_by(name=celery_task_name).first()
                if not task:
                    print(f"Warning: Celery task '{celery_task_name}' not found. Asset will be created but may fail at runtime.")
                
                asset = Asset(
                    name=name,
                    description=description,
                    group_name=group_name,
                    asset_type=asset_type,
                    dependencies=dependencies or [],
                    config=config or {},
                    celery_task_name=celery_task_name,
                    task_args=task_args or [],
                    task_kwargs=task_kwargs or {},
                    partition_type=partition_type,
                    partition_config=partition_config or {},
                    is_active=is_active,
                    created_at=datetime.now()
                )
                session.add(asset)
                session.commit()
                print(f"Created Dagster asset: {name} -> {celery_task_name}")
                return True
            except IntegrityError:
                session.rollback()
                print(f"Failed to create asset '{name}' (integrity error)")
                return False
            except Exception as e:
                session.rollback()
                print(f"Failed to create asset '{name}': {e}")
                return False
    
    def create_assets_batch(self, assets: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Create multiple assets in batch
        
        Args:
            assets: List of asset configuration dictionaries
        
        Returns:
            Dict with success and failure counts
        """
        results = {'success': 0, 'failed': 0, 'skipped': 0}
        
        for asset_config in assets:
            result = self.create_asset(**asset_config)
            if result:
                results['success'] += 1
            elif 'already exists' in str(result):
                results['skipped'] += 1
            else:
                results['failed'] += 1
        
        return results
    
    def update_asset(self, name: str, **kwargs) -> bool:
        """
        Update an existing asset
        
        Args:
            name: Asset name
            **kwargs: Fields to update
        
        Returns:
            True if updated successfully
        """
        from .models import Asset
        
        with self.db_manager.get_session() as session:
            try:
                asset = session.query(Asset).filter_by(name=name).first()
                if not asset:
                    print(f"Asset '{name}' not found")
                    return False
                
                for key, value in kwargs.items():
                    if hasattr(asset, key):
                        setattr(asset, key, value)
                
                session.commit()
                print(f"Updated asset: {name}")
                return True
            except Exception as e:
                session.rollback()
                print(f"Failed to update asset '{name}': {e}")
                return False
    
    def deactivate_asset(self, name: str) -> bool:
        """Deactivate an asset"""
        return self.update_asset(name, is_active=False)
    
    def activate_asset(self, name: str) -> bool:
        """Activate an asset"""
        return self.update_asset(name, is_active=True)

