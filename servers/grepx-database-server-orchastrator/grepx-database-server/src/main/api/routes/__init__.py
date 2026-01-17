from fastapi import APIRouter
from .health import router as health_router
from .query import router as query_router
from .write import router as write_router
from .upsert import router as upsert_router
from .count import router as count_router

router = APIRouter()
server = None

def set_server(srv):
    global server
    server = srv
    
    import api.routes.health as health
    import api.routes.query as query
    import api.routes.write as write
    import api.routes.upsert as upsert
    import api.routes.count as count
    
    health.server = srv
    query.server = srv
    write.server = srv
    upsert.server = srv
    count.server = srv

router.include_router(health_router)
router.include_router(query_router)
router.include_router(write_router)
router.include_router(upsert_router)
router.include_router(count_router)

__all__ = ['router', 'set_server']
