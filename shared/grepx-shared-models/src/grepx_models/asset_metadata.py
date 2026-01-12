"""
AssetMetadata model definition
"""
from sqlalchemy import Column, Integer, String, TIMESTAMP, JSON, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base


class AssetMetadata(Base):
    __tablename__ = "asset_metadata"

    id = Column(Integer, primary_key=True, autoincrement=True)
    asset_name = Column(String, nullable=False)  # Generic asset name (can be Dagster asset or Prefect artifact)
    asset_type = Column(String, default="dagster")  # Type of asset: "dagster", "prefect", etc.
    run_id = Column(String)
    task_id = Column(String)  # Celery task ID
    metadata_json = Column("metadata", JSON, default={})  # Column name in DB is "metadata", but attribute is "metadata_json" to avoid conflict
    created_at = Column(TIMESTAMP)
    # Note: No direct relationship to avoid configuration conflicts
    # Access related assets via manual queries filtering by asset_name and asset_type

