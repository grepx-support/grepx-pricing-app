"""Base class for fundamental metrics."""
from abc import ABC, abstractmethod
from typing import Dict, Any


class FundamentalMetric(ABC):
    """Abstract base class for fundamental metrics."""

    def __init__(self, name: str):
        """
        Initialize fundamental metric.

        Args:
            name: Metric name
        """
        self.name = name

    @abstractmethod
    def calculate(self, financial_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate metric value.

        Args:
            financial_data: Dict containing financial data with keys:
                - ticker, total_debt, total_equity, current_assets,
                  current_liabilities, net_income, revenue,
                  operating_cash_flow, capital_expenditures, average_equity

        Returns:
            Dict with keys:
                - ticker, date, name, value, metadata
        """
        pass
