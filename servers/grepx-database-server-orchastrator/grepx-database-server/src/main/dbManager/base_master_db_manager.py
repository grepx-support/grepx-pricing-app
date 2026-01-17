import logging
from typing import Dict, List, Any, Optional

import setup_paths

from core.session import Session
from .models import StorageMaster


class BaseMasterDBManager:
    def __init__(self, master_db_config: dict):
        self.config = master_db_config
        self.session: Session = None
        self.db_type = master_db_config.get('type', 'sqlite')
        self.logger = logging.getLogger(self.__class__.__name__)

    async def connect(self):
        connection_string = self.config.get('connection_string')

        if not connection_string:
            raise ValueError("Master DB connection_string not found in config")

        self.session = Session.from_connection_string(connection_string)
        await self.session.__aenter__()

        self.logger.info(f"{self.db_type.upper()} Master DB connected successfully")

    async def get_all_active_storages(self) -> List[Dict[str, Any]]:
        results = await StorageMaster.query().filter(active_flag=True).all()
        return [result.to_dict() for result in results]

    async def get_storage_by_name(self, storage_name: str) -> Optional[Dict[str, Any]]:
        result = await StorageMaster.query().filter(storage_name=storage_name, active_flag=True).first()
        return result.to_dict() if result else None

    async def get_default_storage(self) -> Optional[Dict[str, Any]]:
        result = await StorageMaster.query().filter(is_default=True, active_flag=True).first()
        return result.to_dict() if result else None

    async def get_storages_by_type(self, storage_type: str) -> List[Dict[str, Any]]:
        results = await StorageMaster.query().filter(storage_type=storage_type, active_flag=True).all()
        return [result.to_dict() for result in results]

    async def disconnect(self):
        if self.session:
            await self.session.__aexit__(None, None, None)
            self.logger.info(f"{self.db_type.upper()} Master DB disconnected")
