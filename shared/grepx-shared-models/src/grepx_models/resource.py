"""
Resource model definition
"""
from sqlalchemy import Column, Integer, String, Boolean, TIMESTAMP, JSON
from .base import Base


class Resource(Base):
    __tablename__ = "resources"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    resource_type = Column(String, nullable=False)
    config = Column(JSON, default={})
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP)

