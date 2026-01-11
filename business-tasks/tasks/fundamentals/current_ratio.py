"""Current Ratio metric."""
from typing import Dict, Any
from tasks.fundamentals.base import FundamentalMetric


class CurrentRatio(FundamentalMetric):
    """Current Ratio metric."""

    def __init__(self):
        """Initialize Current Ratio metric."""
        super().__init__("Current Ratio")

    def calculate(self, financial_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate Current Ratio.

        Formula: Current Assets / Current Liabilities

        Args:
            financial_data: Dict containing financial data

        Returns:
            Dict with calculated metric
        """
        ticker = financial_data.get('ticker', 'UNKNOWN')
        current_assets = financial_data.get('current_assets', 0)
        current_liabilities = financial_data.get('current_liabilities', 1)  # Avoid division by zero

        # Avoid division by zero
        if current_liabilities == 0:
            current_ratio = None
        else:
            current_ratio = current_assets / current_liabilities

        print(f"\nCurrent Ratio for {ticker}: {current_ratio}")

        return {
            'ticker': ticker,
            'name': self.name,
            'value': round(current_ratio, 2) if current_ratio is not None else None,
            'metadata': {
                'current_assets': current_assets,
                'current_liabilities': current_liabilities
            }
        }


def calculate(financial_data: Dict[str, Any] = None, **kwargs) -> Dict[str, Any]:
    """Calculate Current Ratio metric."""
    # Handle both direct parameter and Dagster dependency injection
    data = financial_data or kwargs.get('financial_statements')

    if data is None:
        raise ValueError("financial_data or financial_statements must be provided")

    metric = CurrentRatio()
    return metric.calculate(data)
