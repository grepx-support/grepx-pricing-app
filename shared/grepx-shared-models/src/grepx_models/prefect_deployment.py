"""
Prefect Deployment model definition
"""
from sqlalchemy import Column, Integer, String, Boolean, Text, TIMESTAMP, JSON, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base


class PrefectDeployment(Base):
    __tablename__ = "prefect_deployments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(Text)
    flow_id = Column(Integer, ForeignKey('prefect_flows.id'), nullable=False)
    work_pool_name = Column(String, nullable=False)  # Which work pool to use
    work_queue_name = Column(String, default="default")  # Queue within the work pool
    schedule_type = Column(String)  # 'cron', 'interval', 'rrule', or null for manual
    schedule_config = Column(JSON, default={})  # cron_string, interval_seconds, rrule, timezone, etc.
    parameters = Column(JSON, default={})  # Default parameters for the flow
    tags = Column(JSON, default=[])
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP)

    # Relationship
    flow = relationship("PrefectFlow", backref="deployments")
