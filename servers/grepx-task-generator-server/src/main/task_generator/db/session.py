# db/session.py
import logging
from sqlalchemy.orm import sessionmaker
from .engine import engine

logger = logging.getLogger("task-generator")

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    future=True,
)


def get_session():
    logger.debug("Creating new database session")
    return SessionLocal()
