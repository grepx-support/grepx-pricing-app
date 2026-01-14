"""Celery application."""
import logging
import sys
from pathlib import Path

from celery import Celery
from omegaconf import DictConfig

logger = logging.getLogger(__name__)


def create_app(cfg: DictConfig) -> Celery:
    """Create Celery app."""
    logger.info(f"Creating: {cfg.app.name}")

    # Add project root to Python path
    # This allows importing business-tasks as business.tasks (dots replace hyphens)
    project_root = Path(__file__).parent.parent.parent.parent.parent.parent
    business_tasks_dir = project_root / "business-tasks"
    
    for path in [str(business_tasks_dir), str(project_root)]:
        if path not in sys.path:
            sys.path.insert(0, path)
            logger.info(f"Added to Python path: {path}")

    celery_app = Celery(cfg.app.name)
    celery_app.conf.update(
        broker_url=cfg.celery.broker_url,
        result_backend=cfg.celery.result_backend,
        task_serializer=cfg.celery.task_serializer,
        result_serializer=cfg.celery.result_serializer,
        accept_content=list(cfg.celery.accept_content),
        timezone=cfg.celery.timezone,
        enable_utc=cfg.celery.enable_utc,
        worker_prefetch_multiplier=cfg.worker.prefetch_multiplier,
        worker_max_tasks_per_child=cfg.worker.max_tasks_per_child,
        task_track_started=cfg.task.track_started,
        task_time_limit=cfg.task.time_limit,
        task_soft_time_limit=cfg.task.soft_time_limit,
        result_expires=cfg.task.result_expires,
    )
    logger.info("✓ Configured")

    # Load tasks from database
    from .task_loader import load_tasks_from_db
    from . import prefect_tasks
    
    logger.info("Loading tasks from database...")
    tasks = load_tasks_from_db(
        db_uri=cfg.tasks.database.uri,
        table=cfg.tasks.database.get("table", "celery_tasks")
    )
    
    # Initialize and register Prefect tasks
    logger.info("Registering Prefect tasks...")
    prefect_tasks.register_tasks()
    
    # Register tasks
    logger.info("Registering tasks...")
    for task_info in tasks:
        try:
            func = task_info.load_function()
            celery_app.task(name=task_info.name, **task_info.options)(func)
            logger.info(f"  ✓ {task_info.name}")
        except Exception as e:
            logger.error(f"  ✗ {task_info.name}: {e}")
    
    logger.info(f"✓ Registered {len(tasks)} tasks")
    return celery_app
