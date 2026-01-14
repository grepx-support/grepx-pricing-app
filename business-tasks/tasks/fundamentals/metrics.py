"""Metric calculation functions for fundamental analysis."""
from .utils import extract_params, calculate_metric


def calculate_free_cash_flow(**kwargs):
    """Calculate Free Cash Flow: Operating Cash Flow - Capital Expenditures"""
    year, quarter, extracted_data = extract_params(kwargs)

    def fcf_calc(fields):
        operating_cash_flow = fields.get('operating_cash_flow', 0)
        capital_expenditures = fields.get('capital_expenditures', 0)
        return operating_cash_flow - capital_expenditures

    return calculate_metric(extracted_data, "free_cash_flow", year, quarter, fcf_calc)


def calculate_debt_to_equity(**kwargs):
    """Calculate Debt-to-Equity Ratio: Total Debt / Stockholders Equity"""
    year, quarter, extracted_data = extract_params(kwargs)

    def dte_calc(fields):
        total_debt = fields.get('total_debt', 0)
        stockholders_equity = fields.get('stockholders_equity', 1)
        return total_debt / stockholders_equity if stockholders_equity != 0 else 0

    return calculate_metric(extracted_data, "debt_to_equity", year, quarter, dte_calc)


def calculate_current_ratio(**kwargs):
    """Calculate Current Ratio: Current Assets / Current Liabilities"""
    year, quarter, extracted_data = extract_params(kwargs)

    def cr_calc(fields):
        current_assets = fields.get('current_assets', 0)
        current_liabilities = fields.get('current_liabilities', 1)
        return current_assets / current_liabilities if current_liabilities != 0 else 0

    return calculate_metric(extracted_data, "current_ratio", year, quarter, cr_calc)


def calculate_net_profit_margin(**kwargs):
    """Calculate Net Profit Margin: (Net Income / Total Revenue) * 100"""
    year, quarter, extracted_data = extract_params(kwargs)

    def npm_calc(fields):
        net_income = fields.get('net_income', 0)
        total_revenue = fields.get('total_revenue', 1)
        return (net_income / total_revenue) * 100 if total_revenue != 0 else 0

    return calculate_metric(extracted_data, "net_profit_margin", year, quarter, npm_calc)


def calculate_return_on_equity(**kwargs):
    """Calculate Return on Equity: (Net Income / Stockholders Equity) * 100"""
    year, quarter, extracted_data = extract_params(kwargs)

    def roe_calc(fields):
        net_income = fields.get('net_income', 0)
        stockholders_equity = fields.get('stockholders_equity', 1)
        return (net_income / stockholders_equity) * 100 if stockholders_equity != 0 else 0

    return calculate_metric(extracted_data, "return_on_equity", year, quarter, roe_calc)
