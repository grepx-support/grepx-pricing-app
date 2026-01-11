"""Simple test task for verification."""
import logging
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any
import asyncio
import json

# Add grepx-database-server paths
db_server_path = Path(__file__).parent.parent.parent / 'servers' / 'grepx-database-server-orchastrator' / 'grepx-database-server' / 'src' / 'main'
orm_path = Path(__file__).parent.parent.parent / 'servers' / 'grepx-database-server-orchastrator' / 'libs' / 'grepx-orm' / 'src'

if str(db_server_path) not in sys.path:
    sys.path.insert(0, str(db_server_path))
if str(orm_path) not in sys.path:
    sys.path.insert(0, str(orm_path))

logger = logging.getLogger(__name__)


def hello_world(name: str = "World", message: str = None, store_output: bool = False) -> Dict[str, Any]:
    """
    Simple test task that returns a greeting message.

    Args:
        name: Name to greet (default: "World")
        message: Optional custom message
        store_output: Whether to store output in database

    Returns:
        Dictionary with greeting and metadata
    """
    logger.info(f"Running hello_world task for: {name}")

    greeting = message if message else f"Hello, {name}!"

    result = {
        "greeting": greeting,
        "name": name,
        "timestamp": datetime.utcnow().isoformat(),
        "status": "success"
    }

    logger.info(f"Task completed: {result}")

    return result


def calculate_sum(numbers: list, store_output: bool = False, task_id: str = None, run_id: str = None) -> Dict[str, Any]:
    logger.info(f"Calculating sum of {len(numbers)} numbers")

    if not numbers:
        raise ValueError("Numbers list cannot be empty")

    total = sum(numbers)

    result = {
        "numbers": numbers,
        "sum": total,
        "count": len(numbers),
        "average": total / len(numbers),
        "timestamp": datetime.utcnow().isoformat(),
        "status": "success"
    }

    logger.info(f"Calculation completed: sum={total}, avg={result['average']}")
    return result
