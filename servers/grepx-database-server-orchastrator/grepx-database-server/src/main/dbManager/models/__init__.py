"""
Database models for the database server.

This package contains model definitions that are independent of grepx_models.
While ideally these could be shared from grepx_models, keeping them here
makes the database server self-contained and independent.
"""

from .storage_master import StorageMaster

__all__ = ['StorageMaster']
