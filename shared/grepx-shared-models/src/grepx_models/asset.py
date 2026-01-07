"""
Asset model definition
"""
from sqlalchemy import Column, Integer, String, Boolean, Text, TIMESTAMP, JSON
from sqlalchemy.orm import relationship
from .base import Base


class Asset(Base):
    __tablename__ = "assets"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(Text)
    group_name = Column(String)
    asset_type = Column(String)
    dependencies = Column(JSON, default=[])
    config = Column(JSON, default={})
    code = Column(Text)  # Kept for backward compatibility
    celery_task_name = Column(String, nullable=False)  # Required for Celery integration
    task_args = Column(JSON, default=[])
    task_kwargs = Column(JSON, default={})
    partition_type = Column(String)
    partition_config = Column(JSON, default={})
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP)
    metadata_records = relationship("AssetMetadata", back_populates="asset")

