"""
AssetMetadata model definition
"""
from sqlalchemy import Column, Integer, String, TIMESTAMP, JSON, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base


class AssetMetadata(Base):
    __tablename__ = "asset_metadata"

    id = Column(Integer, primary_key=True, autoincrement=True)
    asset_name = Column(String, ForeignKey("assets.name"), nullable=False)
    run_id = Column(String)
    task_id = Column(String)  # Celery task ID
    metadata_json = Column("metadata", JSON, default={})  # Column name in DB is "metadata", but attribute is "metadata_json" to avoid conflict
    created_at = Column(TIMESTAMP)
    asset = relationship("Asset", back_populates="metadata_records")

