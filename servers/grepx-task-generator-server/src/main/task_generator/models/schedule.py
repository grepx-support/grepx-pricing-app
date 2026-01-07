"""
Schedule model definition
"""
from sqlalchemy import Column, Integer, String, Boolean, TIMESTAMP, JSON
from .base import Base


class Schedule(Base):
    __tablename__ = "schedules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    cron_schedule = Column(String, nullable=False)
    target_assets = Column(JSON, default=[])
    config = Column(JSON, default={})
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP)

