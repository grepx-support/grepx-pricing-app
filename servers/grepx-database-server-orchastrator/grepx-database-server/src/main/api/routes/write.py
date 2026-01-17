"""Write operations"""

from fastapi import APIRouter, HTTPException
from api.schemas import WriteRequest, WriteResponse
from api.utils import log_request

router = APIRouter()
server = None


@router.post("/write", response_model=WriteResponse)
@log_request("Write")
async def write_data(req: WriteRequest):
    if not server:
        raise HTTPException(status_code=503, detail="Server not initialized")
    
    try:
        backend = server.storage.get(req.storage_name)
        if not backend:
            raise ValueError(f"Storage not found: {req.storage_name}")
        
        # MongoDB direct insert
        if hasattr(backend, 'backend_name') and backend.backend_name == 'mongodb':
            collection = backend.database[req.model_class_name]
            result = await collection.insert_one(req.data)
            return WriteResponse(success=True, id=str(result.inserted_id))
        
        # Other databases via server
        result = await server.write_data(req.storage_name, req.data)
        return WriteResponse(success=True, id=result)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
