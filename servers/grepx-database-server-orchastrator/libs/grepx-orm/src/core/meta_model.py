from core import Field


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
