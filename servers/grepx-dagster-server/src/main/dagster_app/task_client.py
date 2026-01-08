"""
TaskClient for interacting with Celery tasks
"""
from typing import List, Optional
from celery import Celery
from celery.result import AsyncResult


class TaskClient:
    """Client to interact with Celery tasks"""
    
    def __init__(self, broker_url: str = "redis://localhost:6379/0"):
        self.broker_url = broker_url
        self.app = None
        self._initialize_celery()
    
    def _initialize_celery(self):
        """Initialize Celery app"""
        try:
            self.app = Celery('dagster_tasks', broker=self.broker_url)
            self.app.conf.update(
                result_backend=self.broker_url,
                task_serializer='json',
                accept_content=['json'],
                result_serializer='json',
                timezone='UTC',
                enable_utc=True,
            )
        except ImportError:
            print("Warning: Celery not installed. Install with: pip install celery redis")
            self.app = None
    
    def list_tasks(self) -> List[str]:
        """List all registered tasks"""
        if not self.app:
            return []
        return [name for name in self.app.tasks.keys() 
                if not name.startswith('celery.')]
    
    def submit(self, task_name: str, *args, **kwargs) -> AsyncResult:
        """Submit a task and return AsyncResult"""
        if not self.app:
            raise RuntimeError("Celery not initialized. Install with: pip install celery redis")

        # Use send_task which works even if task isn't registered locally
        # The actual task registration happens on the Celery worker
        result = self.app.send_task(task_name, args=args, kwargs=kwargs)
        return result
    
    def get_result(self, task_id: str, timeout: int = 30):
        """Get task result by ID"""
        if not self.app:
            raise RuntimeError("Celery not initialized")
        
        result = AsyncResult(task_id, app=self.app)
        return result.get(timeout=timeout)
    
    def execute(self, task_name: str, *args, **kwargs):
        """Submit task and wait for result"""
        result = self.submit(task_name, *args, **kwargs)
        return self.get_result(result.id)

