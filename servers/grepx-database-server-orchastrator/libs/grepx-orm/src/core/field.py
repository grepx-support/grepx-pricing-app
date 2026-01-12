"""Field types and field definitions for the ORM"""

from enum import Enum
from typing import Any


class FieldType(Enum):
    """Enumeration of supported field types"""
    INTEGER = "integer"
    STRING = "string"
    TEXT = "text"
    BOOLEAN = "boolean"
    FLOAT = "float"
    DATETIME = "datetime"
    JSON = "json"


class Field:
    """Base field class for all field types"""
    
    def __init__(
        self,
        field_type: FieldType,
        primary_key: bool = False,
        nullable: bool = True,
        default: Any = None,
        unique: bool = False,
        index: bool = False
    ):
        self.field_type = field_type
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default
        self.unique = unique
        self.index = index


class IntegerField(Field):
    """Field for integer values"""
    
    def __init__(self, **kwargs):
        super().__init__(FieldType.INTEGER, **kwargs)


class StringField(Field):
    """Field for string values with optional max length"""
    
    def __init__(self, max_length: int = 255, **kwargs):
        super().__init__(FieldType.STRING, **kwargs)
        self.max_length = max_length


class TextField(Field):
    """Field for large text values"""
    
    def __init__(self, **kwargs):
        super().__init__(FieldType.TEXT, **kwargs)


class BooleanField(Field):
    """Field for boolean values"""
    
    def __init__(self, **kwargs):
        super().__init__(FieldType.BOOLEAN, **kwargs)


class FloatField(Field):
    """Field for floating point values"""
    
    def __init__(self, **kwargs):
        super().__init__(FieldType.FLOAT, **kwargs)


class DateTimeField(Field):
    """Field for datetime values"""
    
    def __init__(self, **kwargs):
        super().__init__(FieldType.DATETIME, **kwargs)


class JSONField(Field):
    """Field for JSON data"""
    
    def __init__(self, **kwargs):
        super().__init__(FieldType.JSON, **kwargs)
