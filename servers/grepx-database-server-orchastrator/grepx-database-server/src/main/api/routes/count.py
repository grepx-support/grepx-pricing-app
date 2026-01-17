"""Count operations"""

from fastapi import APIRouter, HTTPException
from api.schemas import QueryRequest, CountResponse
from api.utils import log_request

router = APIRouter()
server = None


@router.post("/count", response_model=CountResponse)
@log_request("Count")
async def count_data(req: QueryRequest):
    if not server:
        raise HTTPException(status_code=503, detail="Server not initialized")
    
    try:
        backend = server.storage.get(req.storage_name)
        if not backend:
            raise ValueError(f"Storage not found: {req.storage_name}")
        
        # MongoDB direct count
        if hasattr(backend, 'backend_name') and backend.backend_name == 'mongodb':
            collection = backend.database[req.model_class_name]
            count = await collection.count_documents(req.filters or {})
            return CountResponse(count=count)
        
        # Other databases via server
        count = await server.count_data(
            req.storage_name,
            req.model_class_name,
            req.filters
        )
        return CountResponse(count=count)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
