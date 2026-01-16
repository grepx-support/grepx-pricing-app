# db/engine.py
import os
import logging
from sqlalchemy import create_engine

logger = logging.getLogger("task-generator")

DATABASE_URL = os.getenv("GREPX_MASTER_DB_URL")
if not DATABASE_URL:
    raise RuntimeError("GREPX_MASTER_DB_URL not set")

logger.debug(f"Connecting to database: {DATABASE_URL.split('@')[-1] if '@' in DATABASE_URL else 'local'}")

engine = create_engine(
    DATABASE_URL,
    future=True,
    pool_pre_ping=True,
)
