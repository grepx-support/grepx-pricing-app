"""Base model class with metaclass for field collection"""

from typing import Dict, Any
from .field import Field


class MetaModel(type):
    """Metaclass that collects Field instances from class attributes"""

    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return super().__new__(cls, name, bases, attrs)

        # Collect fields from class attributes
        fields = {}
        for key, value in attrs.items():
            if isinstance(value, Field):
                fields[key] = value

        # Store fields in _fields attribute
        attrs['_fields'] = fields
        return super().__new__(cls, name, bases, attrs)


class Model(metaclass=MetaModel):
    """Base model class for all ORM models"""

    _fields: Dict[str, Field] = {}

    def __init__(self, **kwargs):
        self._data = {}
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __getattribute__(self, name: str) -> Any:
        # First check if it's a private attribute or method
        if name.startswith('_') or name in ('to_dict', 'get_table_name', 'query'):
            return object.__getattribute__(self, name)

        # Check if it's a field value in _data
        try:
            data = object.__getattribute__(self, '_data')
            if name in data:
                return data[name]
        except AttributeError:
            pass

        # Fall back to default attribute lookup (for class attributes like Field definitions)
        return object.__getattribute__(self, name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith('_'):
            super().__setattr__(name, value)
        elif name in self._fields:
            # Use object.__getattribute__ to safely access _data
            try:
                data = object.__getattribute__(self, '_data')
            except AttributeError:
                super().__setattr__('_data', {})
                data = object.__getattribute__(self, '_data')
            data[name] = value
        else:
            raise AttributeError(f"'{self.__class__.__name__}' has no field '{name}'")

    def to_dict(self) -> Dict[str, Any]:
        """Convert model instance to dictionary"""
        try:
            data = object.__getattribute__(self, '_data')
            return data.copy()
        except AttributeError:
            return {}

    @classmethod
    def get_table_name(cls) -> str:
        """Get the table/collection name for this model"""
        return cls.__name__.lower() + 's'

    @classmethod
    def get_primary_keys(cls) -> list:
        """
        Get list of primary key field names for this model.

        Returns:
            List of field names marked as primary_key=True, in order they appear
        """
        pk_fields = []
        for field_name, field in cls._fields.items():
            if hasattr(field, 'primary_key') and field.primary_key:
                pk_fields.append(field_name)
        return pk_fields

    def get_primary_key_values(self) -> Dict[str, Any]:
        """
        Get the primary key field names and their values from this model instance.

        Returns:
            Dictionary of {field_name: value} for all primary key fields
        """
        pk_values = {}
        for field_name, field in self._fields.items():
            if hasattr(field, 'primary_key') and field.primary_key:
                try:
                    value = getattr(self, field_name)
                    pk_values[field_name] = value
                except AttributeError:
                    pass
        return pk_values

    @classmethod
    def query(cls) -> 'Query':
        """Create a query builder for this model"""
        from .query import Query
        return Query(cls)
