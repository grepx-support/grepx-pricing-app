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
        count = await server.count_data(
            request.storage_name,
            request.model_class_name,
            request.filters
        )
        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
