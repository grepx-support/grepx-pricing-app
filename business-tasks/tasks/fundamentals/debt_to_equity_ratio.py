"""Debt-to-Equity Ratio metric."""
from typing import Dict, Any
from tasks.fundamentals.base import FundamentalMetric


class DebtToEquityRatio(FundamentalMetric):
    """Debt-to-Equity Ratio metric."""

    def __init__(self):
        """Initialize Debt-to-Equity Ratio metric."""
        super().__init__("Debt-to-Equity Ratio")

    def calculate(self, financial_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate Debt-to-Equity Ratio.

        Formula: Total Debt / Stockholders Equity

        Args:
            financial_data: Dict containing financial data

        Returns:
            Dict with calculated metric
        """
        ticker = financial_data.get('ticker', 'UNKNOWN')
        total_debt = financial_data.get('total_debt', 0)
        stockholders_equity = financial_data.get('stockholders_equity', 1)  # Avoid division by zero

        # Avoid division by zero
        if stockholders_equity == 0:
            de_ratio = None
        else:
            de_ratio = total_debt / stockholders_equity

        print(f"\nDebt-to-Equity Ratio for {ticker}: {de_ratio}")

        return {
            'ticker': ticker,
            'name': self.name,
            'value': round(de_ratio, 2) if de_ratio is not None else None,
            'metadata': {
                'total_debt': total_debt,
                'stockholders_equity': stockholders_equity
            }
        }


def calculate(financial_data: Dict[str, Any] = None, **kwargs) -> Dict[str, Any]:
    """Calculate Debt-to-Equity Ratio metric."""
    # Handle both direct parameter and Dagster dependency injection
    data = financial_data or kwargs.get('financial_statements')

    if data is None:
        raise ValueError("financial_data or financial_statements must be provided")

    metric = DebtToEquityRatio()
    return metric.calculate(data)
