"""
Prefect flow/asset model definition
"""
from sqlalchemy import Column, Integer, String, Boolean, Text, TIMESTAMP, JSON
from sqlalchemy.orm import relationship
from .base import Base


class PrefectArtifact(Base):
    __tablename__ = "prefect_artifacts"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Logical name of this pipeline piece (flow or task-group)
    name = Column(String, unique=True, nullable=False)

    # Human description
    description = Column(Text)

    # Grouping like "stocks", "futures", "indices"
    group_name = Column(String)

    # Domain type: "prices", "indicators", "generic-etl"
    artifact_type = Column(String)

    # Upstream logical dependencies (other artifact names)
    dependencies = Column(JSON, default=[])

    # Static configuration (YAML config, parameters, etc.)
    config = Column(JSON, default={})

    # Python entrypoint for Prefect (module:function)
    # e.g. "prefect_app.flows.etl_price_flow:generic_asset_etl"
    entrypoint = Column(String, nullable=False)

    # Optional Prefect deployment name / work pool
    deployment_name = Column(String)
    work_pool_name = Column(String)

    # Default parameters for this flow/deployment
    parameters = Column(JSON, default={})

    # Partitioning / scheduling info (cron, intervals)
    partition_type = Column(String)           # e.g. "time", "symbol"
    partition_config = Column(JSON, default={})

    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP)

    metadata_records = relationship("AssetMetadata", back_populates="asset")
