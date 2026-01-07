"""
Sensor model definition
"""
from sqlalchemy import Column, Integer, String, Boolean, TIMESTAMP, JSON
from .base import Base


class Sensor(Base):
    __tablename__ = "sensors"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    sensor_type = Column(String, nullable=False)
    target_assets = Column(JSON, default=[])
    config = Column(JSON, default={})
    minimum_interval_seconds = Column(Integer, default=30)
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP)

