"""Free Cash Flow (FCF) metric."""
from typing import Dict, Any
from tasks.fundamentals.base import FundamentalMetric


class FreeCashFlow(FundamentalMetric):
    """Free Cash Flow metric."""

    def __init__(self):
        """Initialize Free Cash Flow metric."""
        super().__init__("Free Cash Flow")

    def calculate(self, financial_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate Free Cash Flow.

        Formula: Operating Cash Flow + Capital Expenditures
        (Capital Expenditures is negative, so adding it subtracts the amount)

        Args:
            financial_data: Dict containing financial data

        Returns:
            Dict with calculated metric
        """
        ticker = financial_data.get('ticker', 'UNKNOWN')
        operating_cash_flow = financial_data.get('operating_cash_flow', 0)
        capital_expenditures = financial_data.get('capital_expenditures', 0)

        # Capital Expenditures is negative (cash outflow), so we add it
        fcf = operating_cash_flow + capital_expenditures

        print(f"\nFree Cash Flow for {ticker}: {fcf}")

        return {
            'ticker': ticker,
            'name': self.name,
            'value': round(fcf, 2) if fcf is not None else None,
            'metadata': {
                'operating_cash_flow': operating_cash_flow,
                'capital_expenditures': capital_expenditures
            }
        }


def calculate(financial_data: Dict[str, Any] = None, **kwargs) -> Dict[str, Any]:
    """Calculate Free Cash Flow metric.

    Accepts either financial_data directly or financial_statements as kwarg
    (from Dagster asset dependency injection).
    """
    # Handle both direct parameter and Dagster dependency injection
    data = financial_data or kwargs.get('financial_statements')

    if data is None:
        raise ValueError("financial_data or financial_statements must be provided")

    metric = FreeCashFlow()
    return metric.calculate(data)
