"""Simple test task for verification."""
import logging
import os
from datetime import datetime
from typing import Dict, Any
import asyncio
import json
import aiohttp


async def _store_result_to_mongodb(result: Dict[str, Any], collection_name: str = "task_results"):
    """
    Store result to MongoDB using grepx-database-server API.

    Args:
        result: Result dictionary to store
        collection_name: MongoDB collection name (default: task_results)

    Returns:
        str: The inserted document ID
    """
    # Get API URL from environment variable with fallback to localhost:8000
    api_base_url = os.getenv("GREPX_DATABASE_API_URL", "http://localhost:8000")
    api_endpoint = f"{api_base_url}/write"

    # Storage name from grepx-master.db (storage_master table)
    storage_name = "stock_analysis_mongodb"

    # Prepare request payload according to WriteRequest schema
    payload = {
        "storage_name": storage_name,
        "model_class_name": collection_name,  # Collection name for MongoDB
        "data": result
    }

    print(f"[DEBUG] Sending write request to {api_endpoint}")
    print(f"[DEBUG] Payload: {json.dumps(payload, indent=2, default=str)}")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(api_endpoint, json=payload) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"API request failed with status {response.status}: {error_text}")

                response_data = await response.json()
                print(f"[DEBUG] API Response: {response_data}")

                if not response_data.get("success"):
                    raise Exception("Write operation failed")

                # Return the inserted ID
                inserted_id = response_data.get("id")
                return str(inserted_id) if inserted_id else "unknown"

    except aiohttp.ClientError as e:
        print(f"[ERROR] HTTP request failed: {str(e)}")
        raise Exception(f"Failed to connect to database API at {api_base_url}: {str(e)}")
    except Exception as e:
        print(f"[ERROR] Failed to store result: {str(e)}")
        raise


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

    greeting = message if message else f"Hello, {name}!"
    result = {
        "greeting": greeting,
        "name": name,
        "timestamp": datetime.utcnow().isoformat(),
        "status": "success"
    }

    # Store to MongoDB if requested
    if store_output:
        print(f"[DEBUG] store_output=True, storing result to MongoDB...")
        try:
            inserted_id = asyncio.run(_store_result_to_mongodb(result))
            print(f"[DEBUG] Result stored with ID: {inserted_id}")

            # Celery-safe: do NOT return ObjectId or DB identifiers
            result["stored"] = True

        except Exception as e:
            print(f"[ERROR] Storage failed: {str(e)}")
            result["storage_error"] = str(e)

    return result


def calculate_sum(numbers: list, store_output: bool = False, task_id: str = None, run_id: str = None) -> Dict[str, Any]:

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
