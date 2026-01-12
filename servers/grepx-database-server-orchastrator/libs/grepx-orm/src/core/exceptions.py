"""Custom exceptions for the ORM library"""


class ORMError(Exception):
    """Base exception for all ORM errors"""
    pass


class ValidationError(ORMError):
    """Raised when field validation fails"""
    pass


class DatabaseError(ORMError):
    """Raised when database operations fail"""
    pass


class ConnectionError(ORMError):
    """Raised when database connection fails"""
    pass


class BackendNotFoundError(ORMError):
    """Raised when a requested backend is not found"""
    pass


class MigrationError(ORMError):
    """Raised when migration operations fail"""
    pass
