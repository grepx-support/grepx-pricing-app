"""
Prefect Work Pool model definition
"""
from sqlalchemy import Column, Integer, String, Boolean, Text, TIMESTAMP, JSON
from .base import Base


class PrefectWorkPool(Base):
    __tablename__ = "prefect_work_pools"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(Text)
    pool_type = Column(String, default="process")  # 'process', 'docker', 'kubernetes', etc.
    config = Column(JSON, default={})  # Pool-specific configuration
    concurrency_limit = Column(Integer, default=10)  # Max concurrent runs
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP)
