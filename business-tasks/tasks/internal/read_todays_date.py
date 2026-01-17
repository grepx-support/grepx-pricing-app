"""Read today's date from database."""
import logging
from typing import Dict, Any, List
from datetime import datetime

from .base import ReadTask
from .. import config

logger = logging.getLogger(__name__)


class ReadTodaysDate(ReadTask):
    """Read today's date from database."""

    def __init__(self):
        super().__init__(
            task_name="read_todays_date",
            collection_name="todays_date",
            storage_name=config.METRICS_STORAGE
        )

    def read(self, date: str = None) -> List[Dict[str, Any]]:
        """
        Read today's date from database.

        Args:
            date: Specific date to read (default: today)

        Returns:
            List of date records
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        query = {"date": date}
        records = self.query_data(query=query)

        logger.info(f"Read {len(records)} date records for {date}")
        return records


# Task instance
_task_instance = ReadTodaysDate()


def read_todays_date(date: str = None) -> Dict[str, Any]:
    """
    Read today's date from database.

    Args:
        date: Specific date to read (default: today)

    Returns:
        Result dictionary with status and data
    """
    return _task_instance.execute(date=date)
