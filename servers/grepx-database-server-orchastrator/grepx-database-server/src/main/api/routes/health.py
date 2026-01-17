from fastapi import APIRouter, Request
import logging
from api.schemas import HealthResponse

logger = logging.getLogger(__name__)
router = APIRouter()
server = None

@router.get("/health", response_model=HealthResponse)
async def health_check(request: Request):
    logger.info(f"Health check from {request.client.host}")
    return {"status": "healthy"}
