"""
Prefect Flow model definition
"""
from sqlalchemy import Column, Integer, String, Boolean, Text, TIMESTAMP, JSON
from .base import Base


class PrefectFlow(Base):
    __tablename__ = "prefect_flows"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(Text)
    flow_type = Column(String)  # e.g., 'data_pipeline', 'batch_job', etc.
    tags = Column(JSON, default=[])
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP)
