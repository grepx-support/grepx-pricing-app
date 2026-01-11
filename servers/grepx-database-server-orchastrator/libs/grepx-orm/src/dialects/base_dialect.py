"""Base dialect class"""

from abc import ABC, abstractmethod


class BaseDialect(ABC):
    """Abstract base class for database dialects"""

    @abstractmethod
    def create_table_sql(self, model_class) -> str:
        """Generate CREATE TABLE SQL"""
        pass

    @abstractmethod
    def drop_table_sql(self, model_class) -> str:
        """Generate DROP TABLE SQL"""
        pass
