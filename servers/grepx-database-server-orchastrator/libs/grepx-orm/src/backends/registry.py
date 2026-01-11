"""Backend registry system for plugin architecture"""
from typing import Dict, Type
from .base import DatabaseBackend


class BackendRegistry:
    """Registry for database backends"""

    _backends: Dict[str, Type[DatabaseBackend]] = {}

    @classmethod
    def register(cls, name: str, backend_class: Type[DatabaseBackend]):
        """Register a backend class with a name"""
        cls._backends[name] = backend_class

    @classmethod
    def get_backend(cls, name: str) -> Type[DatabaseBackend]:
        """Get a backend class by name"""
        if name not in cls._backends:
            raise ValueError(
                f"Backend '{name}' not found. "
                f"Available backends: {list(cls._backends.keys())}"
            )
        return cls._backends[name]

    @classmethod
    def list_backends(cls) -> list:
        """List all registered backend names"""
        return list(cls._backends.keys())


# Create a global registry instance
registry = BackendRegistry()


# Export functions for easy registration
def register_backend(name: str):
    """Decorator to register a backend class"""

    def decorator(backend_class: Type[DatabaseBackend]):
        registry.register(name, backend_class)
        return backend_class

    return decorator


def get_backend_class(name: str) -> Type[DatabaseBackend]:
    """Get a backend class by name"""
    return registry.get_backend(name)


def list_available_backends():
    """List all available backend names"""
    return registry.list_backends()
