"""
CeleryTask model definition
"""
from sqlalchemy import Column, Integer, String, Boolean, Text, TIMESTAMP, JSON
from .base import Base


class CeleryTask(Base):
    __tablename__ = "celery_tasks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    module_path = Column(String, nullable=False)
    function_name = Column(String, nullable=False)
    description = Column(Text)
    tags = Column(JSON, default=[])
    options = Column(JSON, default={})
    retry_policy = Column(JSON, default={})
    timeout = Column(Integer, default=300)
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP)

