"""Dynamic backend discovery system"""

import importlib
import pkgutil
from typing import List

from . import registry
from .base import DatabaseBackend


def discover_backends(package_path: str = 'src.backends') -> List[str]:
    """
    Automatically discover and register backends from the backends package
    """
    try:
        package = importlib.import_module(package_path)
        discovered = []

        for _, name, is_pkg in pkgutil.iter_modules(package.__path__):
            if not is_pkg and name not in ['base', 'discovery', 'registry', '__init__']:
                try:
                    module = importlib.import_module(f'{package_path}.{name}')
                    # Look for classes that inherit from DatabaseBackend
                    for attr_name in dir(module):
                        attr = getattr(module, attr_name)
                        if (isinstance(attr, type) and
                                issubclass(attr, DatabaseBackend) and
                                attr != DatabaseBackend):
                            # Use class name without 'Backend' as registration name
                            backend_name = attr_name.lower().replace('backend', '')
                            registry.register(backend_name, attr)
                            discovered.append(backend_name)

                except ImportError as e:
                    # Silently skip modules that can't be imported
                    # (e.g., missing optional dependencies)
                    pass

        return discovered
    except ImportError:
        return []
