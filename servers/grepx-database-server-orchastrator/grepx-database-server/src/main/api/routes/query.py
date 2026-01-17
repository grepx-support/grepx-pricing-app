"""Query operations"""
import logging

from fastapi import APIRouter, HTTPException
from api.schemas import QueryRequest, QueryResponse
from api.utils import log_request

router = APIRouter()
server = None


@router.post("/query", response_model=QueryResponse)
@log_request("Query")
async def query_data(req: QueryRequest):
    logging.info(
        "Received Query request: storage=%s model=%s filters=%s limit=%s offset=%s",
        req.storage_name,
        req.model_class_name,
        req.filters,
        req.limit,
        req.offset,
    )
    if not server:
        raise HTTPException(status_code=503, detail="Server not initialized")

    try:
        backend = server.storage.get(req.storage_name)
        if not backend:
            raise ValueError(f"Storage not found: {req.storage_name}")

        # MongoDB direct query
        if hasattr(backend, 'backend_name') and backend.backend_name == 'mongodb':
            collection = backend.database[req.model_class_name]
            cursor = collection.find(req.filters or {})

            if req.limit:
                cursor = cursor.limit(req.limit)
            if req.offset:
                cursor = cursor.skip(req.offset)

            results = []
            async for doc in cursor:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
                results.append(doc)
            resp = QueryResponse(data=results, count=len(results))
            logging.info(
                "Query response (mongodb): storage=%s model=%s count=%d",
                req.storage_name,
                req.model_class_name,
                resp.count,
            )
            return resp

        # Other databases via server
        results = await server.select_data(
            req.storage_name,
            req.model_class_name,
            req.filters,
            req.limit,
            req.offset
        )
        resp = QueryResponse(data=results, count=len(results))
        logging.info(
            "Query response (backend): storage=%s model=%s count=%d",
            req.storage_name,
            req.model_class_name,
            resp.count,
        )
        return resp
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/query/one", response_model=QueryResponse)
@log_request("Query one")
async def query_one(req: QueryRequest):
    logging.info(
        "Received Query one request: storage=%s model=%s filters=%s",
        req.storage_name,
        req.model_class_name,
        req.filters,
    )

    if not server:
        raise HTTPException(status_code=503, detail="Server not initialized")

    try:
        backend = server.storage.get(req.storage_name)
        if not backend:
            raise ValueError(f"Storage not found: {req.storage_name}")

        # MongoDB direct query
        if hasattr(backend, 'backend_name') and backend.backend_name == 'mongodb':
            collection = backend.database[req.model_class_name]
            result = await collection.find_one(req.filters or {})

            if result and '_id' in result:
                result['_id'] = str(result['_id'])

            resp = QueryResponse(data=result)
            logging.info(
                "Query one response (mongodb): storage=%s model=%s result_present=%s",
                req.storage_name,
                req.model_class_name,
                result is not None,
            )
            return resp

        # Other databases via server
        result = await server.select_one(
            req.storage_name,
            req.model_class_name,
            req.filters or {},
        )
        resp = QueryResponse(data=result)
        logging.info(
            "Query one response (backend): storage=%s model=%s result_present=%s",
            req.storage_name,
            req.model_class_name,
            result is not None,
        )
        return resp

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
