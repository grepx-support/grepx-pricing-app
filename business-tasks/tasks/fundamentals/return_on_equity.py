"""Return on Equity metric."""
from typing import Dict, Any
from tasks.fundamentals.base import FundamentalMetric


class ReturnOnEquity(FundamentalMetric):
    """Return on Equity metric."""

    def __init__(self):
        """Initialize Return on Equity metric."""
        super().__init__("Return on Equity")

    def calculate(self, financial_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate Return on Equity (ROE).

        Formula: (Net Income / Stockholders Equity) * 100

        Args:
            financial_data: Dict containing financial data

        Returns:
            Dict with calculated metric
        """
        ticker = financial_data.get('ticker', 'UNKNOWN')
        net_income = financial_data.get('net_income', 0)
        stockholders_equity = financial_data.get('stockholders_equity', 1)  # Avoid division by zero

        # Avoid division by zero
        if stockholders_equity == 0:
            roe = None
        else:
            roe = (net_income / stockholders_equity) * 100

        print(f"\nReturn on Equity for {ticker}: {roe}%")

        return {
            'ticker': ticker,
            'name': self.name,
            'value': round(roe, 2) if roe is not None else None,
            'metadata': {
                'net_income': net_income,
                'stockholders_equity': stockholders_equity
            }
        }


def calculate(financial_data: Dict[str, Any] = None, **kwargs) -> Dict[str, Any]:
    """Calculate Return on Equity metric."""
    # Handle both direct parameter and Dagster dependency injection
    data = financial_data or kwargs.get('financial_statements')

    if data is None:
        raise ValueError("financial_data or financial_statements must be provided")

    metric = ReturnOnEquity()
    return metric.calculate(data)
