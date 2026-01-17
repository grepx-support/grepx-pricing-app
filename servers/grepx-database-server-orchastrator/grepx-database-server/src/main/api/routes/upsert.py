"""Upsert operations"""

from fastapi import APIRouter, HTTPException
from api.schemas import UpsertRequest, UpsertResponse, BulkUpsertRequest, BulkUpsertResponse
from api.utils import log_request

router = APIRouter()
server = None


@router.post("/upsert", response_model=UpsertResponse)
@log_request("Upsert")
async def upsert_data(req: UpsertRequest):
    if not server:
        raise HTTPException(status_code=503, detail="Server not initialized")
    
    try:
        backend = server.storage.get(req.storage_name)
        if not backend:
            raise ValueError(f"Storage not found: {req.storage_name}")
        
        # MongoDB direct upsert
        if hasattr(backend, 'backend_name') and backend.backend_name == 'mongodb':
            collection = backend.database[req.model_class_name]
            await collection.update_one(
                req.filter_fields or {},
                {'$set': req.data},
                upsert=True
            )
            return UpsertResponse(success=True)
        
        # Other databases via server
        success = await server.upsert_data(
            req.storage_name,
            req.data,
            req.filter_fields
        )
        return UpsertResponse(success=success)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/bulk-upsert", response_model=BulkUpsertResponse)
@log_request("Bulk upsert")
async def bulk_upsert_data(req: BulkUpsertRequest):
    if not server:
        raise HTTPException(status_code=503, detail="Server not initialized")
    
    try:
        count = await server.bulk_upsert_data(
            req.storage_name,
            req.model_class_name,
            req.records,
            req.key_fields,
            req.batch_size
        )
        return BulkUpsertResponse(success=True, count=count)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
