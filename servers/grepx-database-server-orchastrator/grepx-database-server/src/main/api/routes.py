from fastapi import APIRouter, HTTPException
from typing import Optional
from api.schemas import (
    QueryRequest, WriteRequest, UpsertRequest, BulkUpsertRequest,
    QueryResponse, WriteResponse, UpsertResponse, BulkUpsertResponse,
    CountResponse, HealthResponse
)

router = APIRouter()
server = None

def set_server(srv):
    global server
    server = srv

@router.get("/health", response_model=HealthResponse)
async def health_check():
    return {"status": "healthy"}

@router.post("/query", response_model=QueryResponse)
async def query_data(request: QueryRequest):
    if not server:
        raise HTTPException(status_code=503, detail="Server not initialized")

    try:
        backend = server.read_service.get_backend(request.storage_name)
        if not backend:
            raise ValueError(f"No backend found for storage: {request.storage_name}")

        if hasattr(backend, 'backend_name') and backend.backend_name == 'mongodb':
            from bson import ObjectId

            collection = backend.database[request.model_class_name]
            mongo_query = request.filters or {}

            cursor = collection.find(mongo_query)

            if request.limit:
                cursor = cursor.limit(request.limit)
            if request.offset:
                cursor = cursor.skip(request.offset)

            results = []
            async for doc in cursor:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
                results.append(doc)

            return {"data": results, "count": len(results)}
        else:
            results = await server.select_data(
                request.storage_name,
                request.model_class_name,
                request.filters,
                request.limit,
                request.offset
            )
            return {"data": results, "count": len(results)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/query/one", response_model=QueryResponse)
async def query_one(request: QueryRequest):
    if not server:
        raise HTTPException(status_code=503, detail="Server not initialized")

    try:
        backend = server.read_service.get_backend(request.storage_name)
        if not backend:
            raise ValueError(f"No backend found for storage: {request.storage_name}")

        if hasattr(backend, 'backend_name') and backend.backend_name == 'mongodb':
            collection = backend.database[request.model_class_name]
            mongo_query = request.filters or {}

            result = await collection.find_one(mongo_query)

            if result and '_id' in result:
                result['_id'] = str(result['_id'])

            return {"data": result}
        else:
            result = await server.select_one(
                request.storage_name,
                request.model_class_name,
                request.filters or {}
            )
            return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/write", response_model=WriteResponse)
async def write_data(request: WriteRequest):
    if not server:
        raise HTTPException(status_code=503, detail="Server not initialized")

    try:
        # Get the backend for this storage
        backend = server.write_service.get_backend(request.storage_name)
        if not backend:
            raise ValueError(f"No backend found for storage: {request.storage_name}")

        # For MongoDB backend, support raw dict writes directly to collection
        if hasattr(backend, 'backend_name') and backend.backend_name == 'mongodb':
            # Direct MongoDB collection write for raw dict data
            collection = backend.database[request.model_class_name]
            result = await collection.insert_one(request.data)
            return {"success": True, "id": str(result.inserted_id)}
        else:
            # For other backends, expect Model-based writes
            # Note: request.data should be a Model instance for non-MongoDB backends
            result = await server.write_data(
                request.storage_name,
                request.data
            )
            return {"success": True, "id": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/upsert", response_model=UpsertResponse)
async def upsert_data(request: UpsertRequest):
    if not server:
        raise HTTPException(status_code=503, detail="Server not initialized")

    try:
        backend = server.write_service.get_backend(request.storage_name)
        if not backend:
            raise ValueError(f"No backend found for storage: {request.storage_name}")

        # For MongoDB backend, support raw dict upserts directly to collection
        if hasattr(backend, 'backend_name') and backend.backend_name == 'mongodb':
            collection = backend.database[request.model_class_name]

            # Build filter from filter_fields
            mongo_filter = request.filter_fields or {}

            # Perform upsert with $set operator
            result = await collection.update_one(
                mongo_filter,
                {'$set': request.data},
                upsert=True
            )
            return {"success": True}
        else:
            # For other backends, use ORM upsert
            success = await server.upsert_data(
                request.storage_name,
                request.data,
                request.filter_fields
            )
            return {"success": success}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/bulk-upsert", response_model=BulkUpsertResponse)
async def bulk_upsert_data(request: BulkUpsertRequest):
    if not server:
        raise HTTPException(status_code=503, detail="Server not initialized")
    
    try:
        count = await server.bulk_upsert_data(
            request.storage_name,
            request.model_class_name,
            request.records,
            request.key_fields,
            request.batch_size
        )
        return {"success": True, "count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/count", response_model=CountResponse)
async def count_data(request: QueryRequest):
    if not server:
        raise HTTPException(status_code=503, detail="Server not initialized")

    try:
        backend = server.read_service.get_backend(request.storage_name)
        if not backend:
            raise ValueError(f"No backend found for storage: {request.storage_name}")

        if hasattr(backend, 'backend_name') and backend.backend_name == 'mongodb':
            collection = backend.database[request.model_class_name]
            mongo_query = request.filters or {}

            count = await collection.count_documents(mongo_query)

            return {"count": count}
        else:
            count = await server.count_data(
                request.storage_name,
                request.model_class_name,
                request.filters
            )
            return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
