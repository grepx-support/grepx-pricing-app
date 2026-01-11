import setup_paths

from contextlib import asynccontextmanager
from fastapi import FastAPI
from api.routes import router, set_server
from database_server import DatabaseServer

@asynccontextmanager
async def lifespan(app: FastAPI):
    server = DatabaseServer()
    await server.initialize()
    set_server(server)
    app.state.server = server
    
    yield
    
    if hasattr(app.state, 'server'):
        await app.state.server.shutdown()

def create_app() -> FastAPI:
    app = FastAPI(
        title="GrepX Database Server",
        lifespan=lifespan
    )
    
    app.include_router(router)
    
    return app
