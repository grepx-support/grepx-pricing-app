"""Simple test task for verification."""
import logging
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any
import asyncio
import json
from unittest import result


async def _store_result_to_mongodb(result: Dict[str, Any], collection_name: str = "task_results"):
    """
    Store result to MongoDB using grepx-database-server library.

    Args:
        result: Result dictionary to store
        collection_name: MongoDB collection name (default: task_results)
    """
    # Lazy imports - only load when actually storing to avoid dependency issues at module load time
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    print(f"[DEBUG] PROJECT_ROOT: {PROJECT_ROOT}")
    os.environ['GREPX_MASTER_DB_URL'] = f"sqlite:///{PROJECT_ROOT}/data/grepx-master.db"

    sys.path.insert(0, str(PROJECT_ROOT / "servers" / "grepx-database-server-orchastrator" / "grepx-database-server" / "src" / "main"))
    sys.path.insert(0, str(PROJECT_ROOT / "servers" / "grepx-database-server-orchastrator" / "libs" / "grepx-orm" / "src"))
    print(f"[DEBUG] sys.path updated for grepx-database-server")
    from database_server import DatabaseServer

    print(f"[DEBUG] Initializing DatabaseServer...")
    server = DatabaseServer()
    await server.initialize()
    print(f"[DEBUG] DatabaseServer initialized successfully")

    try:
        # Get MongoDB backend from write service
        storage_name = "stock_analysis_mongodb"

        backend = server.write_service.get_backend(storage_name)
        if not backend:
            raise Exception(f"Storage '{storage_name}' not found in write service")

        # Insert directly using backend.database
        print(f"[DEBUG] Inserting into collection: {collection_name}")
        collection = backend.database[collection_name]
        print(f"[DEBUG] Collection obtained: {collection}")
        document = dict(result)  
        print(f"[DEBUG] Document to insert: {document}")
        insert_result = await collection.insert_one(document)

        print(f"[DEBUG] Data inserted successfully! ID: {insert_result.inserted_id}")
        return str(insert_result.inserted_id)

    except Exception as e:
        print(f"[ERROR] Failed to store result: {str(e)}")
        raise
    finally:
        print(f"[DEBUG] Shutting down DatabaseServer...")
        await server.shutdown()
        print(f"[DEBUG] DatabaseServer shutdown complete")


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
