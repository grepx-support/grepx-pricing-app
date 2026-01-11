"""Net Profit Margin metric."""
from typing import Dict, Any
from tasks.fundamentals.base import FundamentalMetric


class NetProfitMargin(FundamentalMetric):
    """Net Profit Margin metric."""

    def __init__(self):
        """Initialize Net Profit Margin metric."""
        super().__init__("Net Profit Margin")

    def calculate(self, financial_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate Net Profit Margin.

        Formula: (Net Income / Total Revenue) * 100

        Args:
            financial_data: Dict containing financial data

        Returns:
            Dict with calculated metric
        """
        ticker = financial_data.get('ticker', 'UNKNOWN')
        net_income = financial_data.get('net_income', 0)
        total_revenue = financial_data.get('total_revenue', 1)  # Avoid division by zero

        # Avoid division by zero
        if total_revenue == 0:
            npm = None
        else:
            npm = (net_income / total_revenue) * 100

        print(f"\nNet Profit Margin for {ticker}: {npm}%")

        return {
            'ticker': ticker,
            'name': self.name,
            'value': round(npm, 2) if npm is not None else None,
            'metadata': {
                'net_income': net_income,
                'total_revenue': total_revenue
            }
        }


def calculate(financial_data: Dict[str, Any] = None, **kwargs) -> Dict[str, Any]:
    """Calculate Net Profit Margin metric."""
    # Handle both direct parameter and Dagster dependency injection
    data = financial_data or kwargs.get('financial_statements')

    if data is None:
        raise ValueError("financial_data or financial_statements must be provided")

    metric = NetProfitMargin()
    return metric.calculate(data)
