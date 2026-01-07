"""Celery package."""
from .main import app

# Export the celery app instance
celery = app

__all__ = ["app", "celery"]

