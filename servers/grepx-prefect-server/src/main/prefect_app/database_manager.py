"""
Lightweight Database Manager for Prefect - Uses Raw SQL Queries
No ORM model dependencies - only reads from database
"""
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from typing import Dict, Any, List, Optional
import json


class DatabaseManager:
    """Lightweight database manager - read-only, uses raw SQL"""

    def __init__(self, db_url: str = "sqlite:///./prefect_config.db"):
        """
        Initialize database manager

        Args:
            db_url: Database URL to connect to
        """
        self.db_url = db_url
        self._engine = create_engine(db_url, echo=False)
        self._Session = sessionmaker(bind=self._engine)

    def get_session(self):
        """Get a new database session"""
        return self._Session()

    def _row_to_dict(self, row) -> Dict[str, Any]:
        """Convert SQLAlchemy row to dictionary"""
        return dict(row._mapping)

    def _parse_json_field(self, value):
        """Parse JSON field from database"""
        if value is None:
            return {}
        if isinstance(value, str):
            try:
                return json.loads(value)
            except:
                return {}
        return value

    def get_flows(self) -> List[Dict[str, Any]]:
        """Get all active Prefect flows - returns list of dicts"""
        with self.get_session() as session:
            result = session.execute(text("""
                SELECT id, name, description, flow_type, tags, is_active, created_at
                FROM prefect_flows
                WHERE is_active = 1
            """))
            flows = []
            for row in result:
                flow_dict = self._row_to_dict(row)
                flow_dict['tags'] = self._parse_json_field(flow_dict.get('tags'))
                flows.append(flow_dict)
            return flows

    def get_tasks(self, flow_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get all active Prefect tasks - returns list of dicts

        Args:
            flow_id: Optional flow ID to filter tasks by flow

        Returns:
            List of task dictionaries
        """
        with self.get_session() as session:
            if flow_id:
                result = session.execute(text("""
                    SELECT id, name, description, flow_id, celery_task_name,
                           task_args, task_kwargs, depends_on, tags, retry_config,
                           timeout_seconds, is_active, created_at
                    FROM prefect_tasks
                    WHERE is_active = 1 AND flow_id = :flow_id
                """), {"flow_id": flow_id})
            else:
                result = session.execute(text("""
                    SELECT id, name, description, flow_id, celery_task_name,
                           task_args, task_kwargs, depends_on, tags, retry_config,
                           timeout_seconds, is_active, created_at
                    FROM prefect_tasks
                    WHERE is_active = 1
                """))
            tasks = []
            for row in result:
                task_dict = self._row_to_dict(row)
                task_dict['task_args'] = self._parse_json_field(task_dict.get('task_args'))
                task_dict['task_kwargs'] = self._parse_json_field(task_dict.get('task_kwargs'))
                task_dict['depends_on'] = self._parse_json_field(task_dict.get('depends_on'))
                task_dict['tags'] = self._parse_json_field(task_dict.get('tags'))
                task_dict['retry_config'] = self._parse_json_field(task_dict.get('retry_config'))
                tasks.append(task_dict)
            return tasks

    def get_deployments(self, flow_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get all active Prefect deployments - returns list of dicts

        Args:
            flow_id: Optional flow ID to filter deployments by flow

        Returns:
            List of deployment dictionaries
        """
        with self.get_session() as session:
            if flow_id:
                result = session.execute(text("""
                    SELECT id, name, description, flow_id, work_pool_name,
                           work_queue_name, schedule_type, schedule_config,
                           parameters, tags, is_active, created_at
                    FROM prefect_deployments
                    WHERE is_active = 1 AND flow_id = :flow_id
                """), {"flow_id": flow_id})
            else:
                result = session.execute(text("""
                    SELECT id, name, description, flow_id, work_pool_name,
                           work_queue_name, schedule_type, schedule_config,
                           parameters, tags, is_active, created_at
                    FROM prefect_deployments
                    WHERE is_active = 1
                """))
            deployments = []
            for row in result:
                deployment_dict = self._row_to_dict(row)
                deployment_dict['schedule_config'] = self._parse_json_field(deployment_dict.get('schedule_config'))
                deployment_dict['parameters'] = self._parse_json_field(deployment_dict.get('parameters'))
                deployment_dict['tags'] = self._parse_json_field(deployment_dict.get('tags'))
                deployments.append(deployment_dict)
            return deployments

    def get_work_pools(self) -> List[Dict[str, Any]]:
        """Get all active Prefect work pools - returns list of dicts"""
        with self.get_session() as session:
            result = session.execute(text("""
                SELECT id, name, description, pool_type, config,
                       concurrency_limit, is_active, created_at
                FROM prefect_work_pools
                WHERE is_active = 1
            """))
            work_pools = []
            for row in result:
                pool_dict = self._row_to_dict(row)
                pool_dict['config'] = self._parse_json_field(pool_dict.get('config'))
                work_pools.append(pool_dict)
            return work_pools

    def get_flow_by_id(self, flow_id: int) -> Optional[Dict[str, Any]]:
        """Get a single flow by ID"""
        with self.get_session() as session:
            result = session.execute(text("""
                SELECT id, name, description, flow_type, tags, is_active, created_at
                FROM prefect_flows
                WHERE id = :flow_id
            """), {"flow_id": flow_id})
            row = result.first()
            if row:
                flow_dict = self._row_to_dict(row)
                flow_dict['tags'] = self._parse_json_field(flow_dict.get('tags'))
                return flow_dict
            return None

    def get_flow_by_name(self, flow_name: str) -> Optional[Dict[str, Any]]:
        """Get a single flow by name"""
        with self.get_session() as session:
            result = session.execute(text("""
                SELECT id, name, description, flow_type, tags, is_active, created_at
                FROM prefect_flows
                WHERE name = :flow_name
            """), {"flow_name": flow_name})
            row = result.first()
            if row:
                flow_dict = self._row_to_dict(row)
                flow_dict['tags'] = self._parse_json_field(flow_dict.get('tags'))
                return flow_dict
            return None
