"""Load tasks from database using SQLAlchemy."""
import logging
from typing import List
from dataclasses import dataclass

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from grepx_models import CeleryTask

logger = logging.getLogger(__name__)


@dataclass
class TaskInfo:
    """Task information from database."""
    name: str
    module_path: str
    function_name: str
    options: dict
    
    def load_function(self):
        """Load the task function."""
        import importlib
        import importlib.util
        from pathlib import Path
        
        # If module path contains 'business-tasks', load it directly by file path
        if 'business-tasks' in self.module_path:
            # Get the project root (task_loader.py -> celery_app -> main -> src -> grepx-celery-server -> servers -> root)
            project_root = Path(__file__).parent.parent.parent.parent.parent.parent
            
            # Convert module path to file path
            # business-tasks.providers.storage -> business-tasks/providers/storage.py
            parts = self.module_path.split('.')
            file_path = project_root / '/'.join(parts[:-1]) / f"{parts[-1]}.py"
            
            if not file_path.exists():
                # Try without the last part (might be a package)
                file_path = project_root / '/'.join(parts) / '__init__.py'
            
            # Load module from file path
            spec = importlib.util.spec_from_file_location(self.module_path, file_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return getattr(module, self.function_name)
        else:
            # Standard import
            print("===========startdatd import fails===================={}".format(self.module_path))
            module = importlib.import_module(self.module_path)
            return getattr(module, self.function_name)


def load_tasks_from_db(db_uri: str, table: str = "celery_tasks") -> List[TaskInfo]:
    """Load tasks from database using SQLAlchemy.
    
    Args:
        db_uri: Database URI (e.g., "sqlite:///path/to/db.db")
        table: Table name (not used with SQLAlchemy ORM, kept for compatibility)
    
    Returns:
        List of TaskInfo objects
    """
    try:
        # Create engine and session
        engine = create_engine(db_uri, echo=False)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Query active tasks using SQLAlchemy
        stmt = select(CeleryTask).where(CeleryTask.is_active == True)
        results = session.scalars(stmt).all()
        
        logger.info(f"Found {len(results)} active tasks")
        
        # Convert to TaskInfo objects
        tasks = []
        for task in results:
            # Merge retry_policy into options
            options = dict(task.options or {})
            if task.retry_policy:
                options.update(task.retry_policy)
            
            task_info = TaskInfo(
                name=task.name,
                module_path=task.module_path,
                function_name=task.function_name,
                options=options
            )
            tasks.append(task_info)
        
        logger.info(f"Loaded {len(tasks)} tasks")
        session.close()
        return tasks
        
    except Exception as e:
        logger.error(f"Database error: {e}")
        return []
