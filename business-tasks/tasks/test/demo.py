"""Test and demo tasks for verification."""
import logging
import asyncio
import json
import aiohttp
from datetime import datetime
from typing import Dict, Any
from .. import config

logger = logging.getLogger(__name__)


async def _store_result_to_mongodb(result: Dict[str, Any], collection_name: str = "task_results"):
    """Store result to MongoDB using database API"""
    api_endpoint = f"{config.API_URL}/write"

    payload = {
        "storage_name": config.STORAGE_NAME,
        "model_class_name": collection_name,
        "data": result
    }

    logger.debug(f"Sending write request to {api_endpoint}")
    logger.debug(f"Payload: {json.dumps(payload, indent=2, default=str)}")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(api_endpoint, json=payload) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"API request failed with status {response.status}: {error_text}")

                response_data = await response.json()
                logger.debug(f"API Response: {response_data}")

                if not response_data.get("success"):
                    raise Exception("Write operation failed")

                inserted_id = response_data.get("id")
                return str(inserted_id) if inserted_id else "unknown"

    except aiohttp.ClientError as e:
        logger.error(f"HTTP request failed: {str(e)}")
        raise Exception(f"Failed to connect to database API at {config.API_URL}: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to store result: {str(e)}")
        raise


def hello_world(name: str = "World", message: str = None, store_output: bool = False) -> Dict[str, Any]:
    """Simple test task that returns a greeting message"""
    greeting = message if message else f"Hello, {name}!"
    result = {
        "greeting": greeting,
        "name": name,
        "timestamp": datetime.utcnow().isoformat(),
        "status": "success"
    }

    if store_output:
        logger.debug("store_output=True, storing result to MongoDB...")
        try:
            inserted_id = asyncio.run(_store_result_to_mongodb(result))
            logger.debug(f"Result stored with ID: {inserted_id}")
            result["stored"] = True
        except Exception as e:
            logger.error(f"Storage failed: {str(e)}")
            result["storage_error"] = str(e)

    return result


def calculate_sum(numbers: list, store_output: bool = False, task_id: str = None, run_id: str = None) -> Dict[str, Any]:
    """Calculate sum and statistics for a list of numbers"""
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

    return result
