"""Extract specific financial fields from raw financial data."""
import logging
import asyncio
from typing import Dict, Any
from .helpers import fetch_raw_statements, extract_value, get_all_tickers_with_data

logger = logging.getLogger(__name__)


def extract_financial_fields(year: int, quarter: int, **kwargs) -> Dict[str, Any]:
    """Extract needed financial fields from raw statements"""
    tickers = asyncio.run(get_all_tickers_with_data(year, quarter))
    if not tickers:
        raise ValueError("No financial data found to extract")

    extracted_data = {}

    for ticker in tickers:
        try:
            raw_data = asyncio.run(fetch_raw_statements(ticker, year, quarter))
            if not raw_data:
                logger.warning(f"No raw data for {ticker}")
                continue

            balance_sheet = raw_data.get('balance_sheet', [])
            income_stmt = raw_data.get('income_statement', [])
            cash_flow = raw_data.get('cash_flow', [])

            logger.debug(f"BS type: {type(balance_sheet)}, len: {len(balance_sheet) if isinstance(balance_sheet, list) else 'N/A'}")

            extracted = {
                "current_assets": extract_value(balance_sheet, 'Current Assets', 0),
                "current_liabilities": extract_value(balance_sheet, 'Current Liabilities', 0),
                "total_debt": extract_value(balance_sheet, 'Total Debt', 0),
                "stockholders_equity": extract_value(balance_sheet, 'Stockholders Equity', 0),
                "net_income": extract_value(income_stmt, 'Net Income', 0),
                "total_revenue": extract_value(income_stmt, 'Total Revenue', 0),
                "operating_cash_flow": extract_value(cash_flow, 'Operating Cash Flow', 0),
                "capital_expenditures": extract_value(cash_flow, 'Capital Expenditure', 0),
            }

            logger.info(f"Extracted for {ticker}: debt={extracted['total_debt']}, equity={extracted['stockholders_equity']}")

            extracted_data[ticker] = extracted

        except Exception as e:
            logger.error(f"Error extracting for {ticker}: {str(e)}", exc_info=True)

    logger.info(f"Extraction complete: {len(extracted_data)} tickers processed")
    return extracted_data
