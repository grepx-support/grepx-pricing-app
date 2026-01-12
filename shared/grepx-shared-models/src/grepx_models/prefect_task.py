"""
Prefect Task model definition
"""
from sqlalchemy import Column, Integer, String, Boolean, Text, TIMESTAMP, JSON, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base


class PrefectTask(Base):
    __tablename__ = "prefect_tasks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(Text)
    flow_id = Column(Integer, ForeignKey('prefect_flows.id'), nullable=False)
    celery_task_name = Column(String, nullable=False)  # The Celery task to execute
    task_args = Column(JSON, default=[])
    task_kwargs = Column(JSON, default={})
    depends_on = Column(JSON, default=[])  # List of task names this task depends on
    tags = Column(JSON, default=[])
    retry_config = Column(JSON, default={})  # max_retries, retry_delay_seconds, etc.
    timeout_seconds = Column(Integer, default=300)
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP)

    # Relationship
    flow = relationship("PrefectFlow", backref="tasks")
