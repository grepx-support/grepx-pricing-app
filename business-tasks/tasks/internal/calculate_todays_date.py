"""Calculate and store today's date."""
import logging
from typing import Dict, Any, List
from datetime import datetime

from .base import CalculateTask
from .. import config

logger = logging.getLogger(__name__)


class CalculateTodaysDate(CalculateTask):
    """Calculate and store today's date."""

    def __init__(self):
        super().__init__(
            task_name="calculate_todays_date",
            collection_name="todays_date",
            storage_name=config.METRICS_STORAGE
        )

    def calculate(self, source: str = "system") -> List[Dict[str, Any]]:
        """
        Calculate today's date.

        Args:
            source: Source identifier (default: system)

        Returns:
            List with single date record
        """
        today = datetime.now()

        record = {
            "date": today.strftime("%Y-%m-%d"),
            "source": source
        }

        logger.info(f"Calculated today's date: {record['date']}")
        return [record]

    def _get_filter_fields(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Get filter fields - use date as unique key."""
        return {"date": record["date"]}


# Task instance
_task_instance = CalculateTodaysDate()


def calculate_todays_date(source: str = "system") -> Dict[str, Any]:
    """
    Calculate and store today's date.

    Args:
        source: Source identifier (default: system)

    Returns:
        Result dictionary with status
    """
    return _task_instance.execute(source=source)
