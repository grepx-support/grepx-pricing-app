"""Business tasks master table model."""
from sqlalchemy import Column, String, Integer, Boolean, DateTime, JSON, Text
from datetime import datetime
from .base import Base


class BusinessTasksMaster(Base):
    """Master table for all business tasks configuration."""
    
    __tablename__ = 'business_tasks_master'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    task_name = Column(String(255), nullable=False, unique=True, index=True)
    description = Column(Text, nullable=True)
    task_type = Column(String(100), nullable=False, comment='Type of task: data_download, indicator_calculation, etc.')
    input_arguments = Column(JSON, nullable=True, comment='JSON schema of input arguments')
    destination = Column(String(255), nullable=True, comment='Where output is stored (table name, file path, etc.)')
    provider_type = Column(String(50), nullable=True, comment='Provider to use if applicable')
    active_flag = Column(Boolean, default=True, nullable=False)
    retry_attempts = Column(Integer, default=3, nullable=False)
    timeout_seconds = Column(Integer, default=300, nullable=False)
    batch_size = Column(Integer, default=10, nullable=True)
    priority = Column(Integer, default=5, nullable=False, comment='Task priority (1-10, higher = more priority)')
    schedule_expression = Column(String(100), nullable=True, comment='Cron expression or schedule string')
    depends_on = Column(JSON, nullable=True, comment='List of task IDs this task depends on')
    metadata = Column(JSON, nullable=True, comment='Additional task-specific metadata')
    created_date = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    created_by = Column(String(100), nullable=True)
    updated_by = Column(String(100), nullable=True)
    
    def __repr__(self):
        return f"<BusinessTasksMaster(id={self.id}, task_name='{self.task_name}', active={self.active_flag})>"
    
    def to_dict(self):
        """Convert model to dictionary."""
        return {
            'id': self.id,
            'task_name': self.task_name,
            'description': self.description,
            'task_type': self.task_type,
            'input_arguments': self.input_arguments,
            'destination': self.destination,
            'provider_type': self.provider_type,
            'active_flag': self.active_flag,
            'retry_attempts': self.retry_attempts,
            'timeout_seconds': self.timeout_seconds,
            'batch_size': self.batch_size,
            'priority': self.priority,
            'schedule_expression': self.schedule_expression,
            'depends_on': self.depends_on,
            'metadata': self.metadata,
            'created_date': self.created_date.isoformat() if self.created_date else None,
            'updated_date': self.updated_date.isoformat() if self.updated_date else None,
            'created_by': self.created_by,
            'updated_by': self.updated_by
        }

