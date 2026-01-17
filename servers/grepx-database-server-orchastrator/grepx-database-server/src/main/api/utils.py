"""API route utilities"""

import time
import logging
from functools import wraps
from typing import Callable

logger = logging.getLogger(__name__)


def log_request(operation: str):
    """Decorator to log request timing"""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start = time.time()
            try:
                result = await func(*args, **kwargs)
                elapsed = time.time() - start
                logger.info(f"{operation} completed in {elapsed:.3f}s")
                return result
            except Exception as e:
                elapsed = time.time() - start
                logger.error(f"{operation} failed after {elapsed:.3f}s: {e}", exc_info=True)
                raise
        return wrapper
    return decorator
