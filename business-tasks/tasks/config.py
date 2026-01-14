"""Centralized configuration for all tasks.

Gets storage configuration from grepx-master.db.
"""
import os
import sqlite3
from pathlib import Path

API_URL = os.getenv("GREPX_DATABASE_API_URL", "http://localhost:8000")

def _get_storage_name(storage_type: str, default: str) -> str:
    """Query master DB for storage name by ID"""
    try:
        db_path = Path(__file__).parent.parent.parent / "data" / "grepx-master.db"
        if not db_path.exists():
            return default

        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        # Query by id for predictable matching
        cursor.execute("SELECT storage_name FROM storage_master WHERE id = ?", (storage_type,))
        result = cursor.fetchone()
        conn.close()

        return result[0] if result else default
    except:
        return default

# Load storage names from master DB (IDs from database.yaml)
TICKER_STORAGE = _get_storage_name(2, "tickers")           # id: 2 in database.yaml
FINANCIAL_STORAGE = _get_storage_name(3, "financial_statements")  # id: 3 in database.yaml
METRICS_STORAGE = _get_storage_name(4, "stock_analysis")   # id: 4 in database.yaml

__all__ = ['API_URL', 'TICKER_STORAGE', 'FINANCIAL_STORAGE', 'METRICS_STORAGE']
