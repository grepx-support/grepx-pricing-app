"""Data loader for financial statements."""
from typing import Dict, Any
import sys
import os


def load_financial_data(ticker: str, year: int = 2023, quarter: int = 4) -> Dict[str, Any]:
    """
    Load financial data from Yahoo Finance using providers.

    Fetches balance sheet, income statement, and cash flow data,
    then extracts key financial metrics.

    Args:
        ticker: Stock ticker symbol
        year: Year for financial data (default 2023)
        quarter: Quarter for financial data (default 4)

    Returns:
        Dict with financial data for metric calculation
    """
    # Lazy imports to avoid import errors during task registration
    parent_dir = os.path.join(os.path.dirname(__file__), '../..')
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

    from providers.provider_factory import ProviderFactory

    print(f"\n{'='*60}")
    print(f"LOADING FINANCIAL DATA FOR {ticker}")
    print(f"{'='*60}\n")

    # Get providers
    balance_sheet_provider = ProviderFactory.get_provider("yahoo_for_balance_sheet")
    cash_flow_provider = ProviderFactory.get_provider("yahoo_for_cash_flow")
    income_stmt_provider = balance_sheet_provider  # Same provider handles income statements

    # Download balance sheet
    print("Fetching Balance Sheet...")
    balance_sheet = balance_sheet_provider.download_balance_sheet(ticker)
    print(f"✓ Got balance sheet with shape: {balance_sheet.shape}")
    print(f"  Available rows: {list(balance_sheet.index[:10])}...\n")

    # Download income statement
    print("Fetching Income Statement...")
    income_stmt = balance_sheet_provider.download_income_statement(ticker)
    print(f"✓ Got income statement with shape: {income_stmt.shape}")
    print(f"  Available rows: {list(income_stmt.index[:10])}...\n")

    # Download cash flow
    print("Fetching Cash Flow Statement...")
    cash_flow = cash_flow_provider.download_cash_flow(ticker)
    print(f"✓ Got cash flow with shape: {cash_flow.shape}")
    print(f"  Available rows: {list(cash_flow.index[:10])}...\n")

    # Extract key data points from most recent data
    # Handle empty dataframes
    if balance_sheet.empty or income_stmt.empty or cash_flow.empty:
        raise ValueError(f"No financial data available for {ticker}. Check if ticker symbol is valid and available on Yahoo Finance.")

    # Get the most recent data (first row after transposition)
    most_recent_date = cash_flow.index[0]

    print(f"  Most recent data date: {most_recent_date}\n")

    # Cash Flow data - for FCF calculation
    operating_cash_flow = float(cash_flow.loc[most_recent_date, 'Operating Cash Flow']) if 'Operating Cash Flow' in cash_flow.columns else 0
    capital_expenditures = float(cash_flow.loc[most_recent_date, 'Capital Expenditure']) if 'Capital Expenditure' in cash_flow.columns else 0
    yahoo_fcf = float(cash_flow.loc[most_recent_date, 'Free Cash Flow']) if 'Free Cash Flow' in cash_flow.columns else None

    # Balance Sheet data - for D/E Ratio and Current Ratio
    total_debt = float(balance_sheet.loc[most_recent_date, 'Total Debt']) if 'Total Debt' in balance_sheet.columns else 0
    stockholders_equity = float(balance_sheet.loc[most_recent_date, 'Stockholders Equity']) if 'Stockholders Equity' in balance_sheet.columns else 0
    current_assets = float(balance_sheet.loc[most_recent_date, 'Current Assets']) if 'Current Assets' in balance_sheet.columns else 0
    current_liabilities = float(balance_sheet.loc[most_recent_date, 'Current Liabilities']) if 'Current Liabilities' in balance_sheet.columns else 0

    # Income Statement data - for NPM and ROE
    net_income = float(income_stmt.loc[most_recent_date, 'Net Income']) if 'Net Income' in income_stmt.columns else 0
    total_revenue = float(income_stmt.loc[most_recent_date, 'Total Revenue']) if 'Total Revenue' in income_stmt.columns else 0

    financial_data = {
        'ticker': ticker,
        'year': year,
        'quarter': quarter,
        # Cash Flow metrics
        'operating_cash_flow': operating_cash_flow,
        'capital_expenditures': capital_expenditures,
        'yahoo_free_cash_flow': yahoo_fcf,
        # Balance Sheet metrics
        'total_debt': total_debt,
        'stockholders_equity': stockholders_equity,
        'current_assets': current_assets,
        'current_liabilities': current_liabilities,
        # Income Statement metrics
        'net_income': net_income,
        'total_revenue': total_revenue
    }

    print(f"Extracted Financial Data for {ticker}:")
    print(f"  Cash Flow:")
    print(f"    - Operating Cash Flow: {operating_cash_flow}")
    print(f"    - Capital Expenditures: {capital_expenditures}")
    print(f"  Balance Sheet:")
    print(f"    - Total Debt: {total_debt}")
    print(f"    - Stockholders Equity: {stockholders_equity}")
    print(f"    - Current Assets: {current_assets}")
    print(f"    - Current Liabilities: {current_liabilities}")
    print(f"  Income Statement:")
    print(f"    - Net Income: {net_income}")
    print(f"    - Total Revenue: {total_revenue}\n")

    return financial_data
