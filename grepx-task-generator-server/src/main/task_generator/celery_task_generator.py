"""
Celery Task Generator - Creates and registers Celery tasks in the database
"""
from typing import Dict, List, Optional, Any
from datetime import datetime
from sqlalchemy.exc import IntegrityError


class CeleryTaskGenerator:
    """
    Generates and registers Celery tasks in the database
    """
    
    def __init__(self, db_manager):
        """
        Initialize Celery task generator
        
        Args:
            db_manager: DatabaseManager instance
        """
        self.db_manager = db_manager
    
    def create_task(
        self,
        name: str,
        module_path: str,
        function_name: str,
        description: str = "",
        tags: List[str] = None,
        options: Dict = None,
        retry_policy: Dict = None,
        timeout: int = 300,
        is_active: bool = True
    ) -> bool:
        """
        Create a Celery task in the database
        
        Args:
            name: Full task name (e.g., 'tasks.fetch_stock_prices')
            module_path: Python module path (e.g., 'tasks.stock_tasks')
            function_name: Function name in the module
            description: Task description
            tags: List of tags
            options: Celery task options
            retry_policy: Retry policy configuration
            timeout: Task timeout in seconds
            is_active: Whether task is active
        
        Returns:
            True if created successfully, False otherwise
        """
        from .models import CeleryTask
        
        with self.db_manager.get_session() as session:
            try:
                # Check if task already exists
                existing = session.query(CeleryTask).filter_by(name=name).first()
                if existing:
                    print(f"Task '{name}' already exists, skipping...")
                    return False
                
                task = CeleryTask(
                    name=name,
                    module_path=module_path,
                    function_name=function_name,
                    description=description,
                    tags=tags or [],
                    options=options or {},
                    retry_policy=retry_policy or {},
                    timeout=timeout,
                    is_active=is_active,
                    created_at=datetime.now()
                )
                session.add(task)
                session.commit()
                print(f"Created Celery task: {name}")
                return True
            except IntegrityError:
                session.rollback()
                print(f"Failed to create task '{name}' (integrity error)")
                return False
            except Exception as e:
                session.rollback()
                print(f"Failed to create task '{name}': {e}")
                return False
    
    def create_tasks_batch(self, tasks: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Create multiple tasks in batch
        
        Args:
            tasks: List of task configuration dictionaries
        
        Returns:
            Dict with success and failure counts
        """
        results = {'success': 0, 'failed': 0, 'skipped': 0}
        
        for task_config in tasks:
            result = self.create_task(**task_config)
            if result:
                results['success'] += 1
            elif 'already exists' in str(result):
                results['skipped'] += 1
            else:
                results['failed'] += 1
        
        return results
    
    def update_task(self, name: str, **kwargs) -> bool:
        """
        Update an existing task
        
        Args:
            name: Task name
            **kwargs: Fields to update
        
        Returns:
            True if updated successfully
        """
        from .models import CeleryTask
        
        with self.db_manager.get_session() as session:
            try:
                task = session.query(CeleryTask).filter_by(name=name).first()
                if not task:
                    print(f"Task '{name}' not found")
                    return False
                
                for key, value in kwargs.items():
                    if hasattr(task, key):
                        setattr(task, key, value)
                
                session.commit()
                print(f"Updated task: {name}")
                return True
            except Exception as e:
                session.rollback()
                print(f"Failed to update task '{name}': {e}")
                return False
    
    def deactivate_task(self, name: str) -> bool:
        """Deactivate a task"""
        return self.update_task(name, is_active=False)
    
    def activate_task(self, name: str) -> bool:
        """Activate a task"""
        return self.update_task(name, is_active=True)

