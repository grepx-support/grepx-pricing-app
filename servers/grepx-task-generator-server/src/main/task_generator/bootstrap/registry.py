# bootstrap/registry.py
import logging
import grepx_models  # noqa: F401 (forces model import)
from grepx_models.base import Base

logger = logging.getLogger("task-generator")


def build_model_registry():
    registry = {}
    for mapper in Base.registry.mappers:
        model = mapper.class_
        table = model.__tablename__
        registry[table] = model
    
    logger.debug(f"Built model registry with {len(registry)} models: {list(registry.keys())}")
    return registry


def validate_registry(registry):
    for table, model in registry.items():
        if model.__tablename__ != table:
            raise RuntimeError(
                f"Registry mismatch: {model.__name__} -> {table}"
            )
    logger.debug("Registry validation passed")


MODEL_REGISTRY = build_model_registry()
