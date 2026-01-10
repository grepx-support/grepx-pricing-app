"""Simple test task for verification."""
import logging
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)


def hello_world(name: str = "World", message: str = None) -> Dict[str, Any]:
    """
    Simple test task that returns a greeting message.
    
    Args:
        name: Name to greet (default: "World")
        message: Optional custom message
        
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


def calculate_sum(numbers: list) -> Dict[str, Any]:
    """
    Simple calculation task that sums a list of numbers.
    
    Args:
        numbers: List of numbers to sum
        
    Returns:
        Dictionary with sum result and metadata
    """
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

