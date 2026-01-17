# app.py - FIXED VERSION
import os
import sys

# Add src/ to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))  # /api
main_dir = os.path.dirname(current_dir)  # /main
src_dir = os.path.dirname(main_dir)  # /src

sys.path.insert(0, src_dir)

from contextlib import asynccontextmanager
from fastapi import FastAPI
from database_server import DatabaseServer
from api.routes import router, set_server


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
