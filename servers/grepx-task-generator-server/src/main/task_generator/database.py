"""
Database Configuration and Manager
"""
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from dagster import ConfigurableResource
from typing import Dict, Any, List, Optional

# Import models from shared package
from grepx_models import Base, Asset, Resource, Schedule, Sensor, AssetMetadata, CeleryTask


class DatabaseManager(ConfigurableResource):
    """Database Manager using SQLAlchemy ORM"""
    
    db_url: str = "sqlite:///./dagster_config_orm.db"

    def __init__(self, **kwargs):
        # Pass db_url to parent ConfigurableResource initialization
        # If db_url not provided, use default
        if 'db_url' not in kwargs:
            kwargs['db_url'] = "sqlite:///./dagster_config_orm.db"
        # Call parent init first to set db_url properly
        super().__init__(**kwargs)
        # Now initialize engine (db_url is accessible as self.db_url from parent)
        self._ensure_engine()

    def _ensure_engine(self):
        """Ensure engine is initialized"""
        if not hasattr(self, '_engine'):
            # Access db_url from the ConfigurableResource (it's a Pydantic field)
            db_url = self.db_url
            # Use object.__setattr__ to bypass Pydantic's frozen check for internal state
            object.__setattr__(self, '_engine', create_engine(db_url, echo=False))
            object.__setattr__(self, '_Session', sessionmaker(bind=self._engine))

    def setup_for_execution(self, context) -> None:
        """Initialize engine and sessionmaker when resource is set up"""
        self._ensure_engine()

    def initialize_schema(self):
        """Create all tables"""
        self._ensure_engine()
        Base.metadata.create_all(self._engine)

    def get_session(self):
        """Get a new database session"""
        self._ensure_engine()
        return self._Session()

    # -------- CRUD METHODS --------
    def get_assets(self) -> List[Asset]:
        """Get all active assets"""
        with self.get_session() as session:
            stmt = select(Asset).where(Asset.is_active == True)
            return list(session.scalars(stmt).all())

    def get_resources(self) -> List[Resource]:
        """Get all active resources"""
        with self.get_session() as session:
            stmt = select(Resource).where(Resource.is_active == True)
            return list(session.scalars(stmt).all())

    def get_schedules(self) -> List[Schedule]:
        """Get all active schedules"""
        with self.get_session() as session:
            stmt = select(Schedule).where(Schedule.is_active == True)
            return list(session.scalars(stmt).all())

    def get_sensors(self) -> List[Sensor]:
        """Get all active sensors"""
        with self.get_session() as session:
            stmt = select(Sensor).where(Sensor.is_active == True)
            return list(session.scalars(stmt).all())

    def get_celery_tasks(self) -> List[CeleryTask]:
        """Get all active Celery tasks"""
        with self.get_session() as session:
            stmt = select(CeleryTask).where(CeleryTask.is_active == True)
            return list(session.scalars(stmt).all())

    def save_asset_metadata(self, asset_name: str, run_id: str, task_id: Optional[str], metadata: Dict[str, Any]):
        """Save metadata for an asset run"""
        with self.get_session() as session:
            meta = AssetMetadata(
                asset_name=asset_name,
                run_id=run_id,
                task_id=task_id,
                metadata_json=metadata
            )
            session.add(meta)
            session.commit()

    def add_task(self, name: str, module_path: str, function_name: str, 
                 description: str = "", tags: List[str] = None, 
                 options: Dict = None, retry_policy: Dict = None, timeout: int = 300):
        """Add a new Celery task to the database"""
        with self.get_session() as session:
            try:
                task = CeleryTask(
                    name=name,
                    module_path=module_path,
                    function_name=function_name,
                    description=description,
                    tags=tags or [],
                    options=options or {},
                    retry_policy=retry_policy or {},
                    timeout=timeout,
                    is_active=True
                )
                session.add(task)
                session.commit()
                return True
            except IntegrityError:
                session.rollback()
                return False

