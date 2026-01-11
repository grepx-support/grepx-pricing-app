from pydantic import BaseModel
from typing import List, Dict, Any, Optional


class QueryRequest(BaseModel):
    storage_name: str
    model_class_name: str
    filters: Optional[Dict[str, Any]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None


class WriteRequest(BaseModel):
    storage_name: str
    model_class_name: str
    data: Dict[str, Any]


class UpsertRequest(BaseModel):
    storage_name: str
    model_class_name: str
    data: Dict[str, Any]
    filter_fields: Dict[str, Any]


class BulkUpsertRequest(BaseModel):
    storage_name: str
    model_class_name: str
    records: List[Dict[str, Any]]
    key_fields: List[str]
    batch_size: int = 100


class QueryResponse(BaseModel):
    data: Any
    count: Optional[int] = None


class WriteResponse(BaseModel):
    success: bool
    id: Optional[Any] = None


class UpsertResponse(BaseModel):
    success: bool


class BulkUpsertResponse(BaseModel):
    success: bool
    count: int


class CountResponse(BaseModel):
    count: int


class HealthResponse(BaseModel):
    status: str
