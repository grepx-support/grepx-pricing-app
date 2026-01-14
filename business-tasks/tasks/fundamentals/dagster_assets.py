"""
Dagster assets that call Celery tasks for execution.

Dagster is the orchestrator/scheduler.
Celery executes the actual work (download, extract, calculate).

Pipeline:
    financial_data (Dagster calls Celery task)
            ↓ (waits for Celery to finish, data saved to DB)
    extracted_fields (Dagster calls Celery task)
            ↓ (waits for Celery to finish, cached in Dagster)
        ┌───┴───┬───────────┬──────────┬───────────┐
        ↓       ↓           ↓          ↓           ↓
      FCF     D/E        CR         NPM         ROE
      (Dagster calls Celery tasks in parallel)
"""

from dagster import asset, AssetExecutionContext
from typing import Dict, Any
import logging
from celery import Celery

logger = logging.getLogger(__name__)

# Connect to Celery
celery_app = Celery(
    'business_tasks',
    broker='redis://localhost:6379/0',  # Change if your Redis is different
    backend='redis://localhost:6379/0'
)


@asset
def financial_data_asset(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Call Celery task to download financial data.

    Dagster orchestrates, Celery executes.
    """
    context.log.info("Dagster: Calling Celery task - download_financial_data...")

    # Call Celery task and wait for result
    task = celery_app.send_task(
        'business-tasks.fundamentals.download_financial_data',
        args=(2024, 1, ["balance_sheet", "income_statement", "cash_flow"], None)
    )

    result = task.get(timeout=1800)  # Wait 30 minutes max
    context.log.info(f"Celery completed: Downloaded {result.get('tickers_downloaded')} tickers")

    return result


@asset
def extracted_fields(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Call Celery task to extract financial fields.

    Dagster orchestrates, Celery executes and extracts.
    Data cached by Dagster after Celery returns.
    """
    context.log.info("Dagster: Calling Celery task - extract_financial_fields...")

    # Call Celery task and wait for result
    task = celery_app.send_task(
        'business-tasks.fundamentals.extract_financial_fields',
        kwargs={'year': 2024, 'quarter': 1}
    )

    extracted_data = task.get(timeout=600)  # Wait 10 minutes max
    context.log.info(f"Celery completed: Extracted {len(extracted_data)} tickers (now cached in Dagster)")

    return extracted_data


@asset
def free_cash_flow(context: AssetExecutionContext, extracted_fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Call Celery task to calculate Free Cash Flow.

    Receives cached extracted_fields from previous asset.
    Passes it to Celery task.
    """
    context.log.info("Dagster: Calling Celery task - calculate_free_cash_flow...")

    task = celery_app.send_task(
        'business-tasks.fundamentals.calculate_free_cash_flow',
        kwargs={'year': 2024, 'quarter': 1, 'extracted_fields': extracted_fields}
    )

    result = task.get(timeout=600)
    context.log.info(f"Celery completed: Calculated FCF for {result.get('count')} tickers")

    return result


@asset
def debt_to_equity(context: AssetExecutionContext, extracted_fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Call Celery task to calculate Debt-to-Equity Ratio.

    Receives cached extracted_fields from previous asset.
    """
    context.log.info("Dagster: Calling Celery task - calculate_debt_to_equity...")

    task = celery_app.send_task(
        'business-tasks.fundamentals.calculate_debt_to_equity',
        kwargs={'year': 2024, 'quarter': 1, 'extracted_fields': extracted_fields}
    )

    result = task.get(timeout=600)
    context.log.info(f"Celery completed: Calculated D/E for {result.get('count')} tickers")

    return result


@asset
def current_ratio(context: AssetExecutionContext, extracted_fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Call Celery task to calculate Current Ratio.

    Receives cached extracted_fields from previous asset.
    """
    context.log.info("Dagster: Calling Celery task - calculate_current_ratio...")

    task = celery_app.send_task(
        'business-tasks.fundamentals.calculate_current_ratio',
        kwargs={'year': 2024, 'quarter': 1, 'extracted_fields': extracted_fields}
    )

    result = task.get(timeout=600)
    context.log.info(f"Celery completed: Calculated CR for {result.get('count')} tickers")

    return result


@asset
def net_profit_margin(context: AssetExecutionContext, extracted_fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Call Celery task to calculate Net Profit Margin.

    Receives cached extracted_fields from previous asset.
    """
    context.log.info("Dagster: Calling Celery task - calculate_net_profit_margin...")

    task = celery_app.send_task(
        'business-tasks.fundamentals.calculate_net_profit_margin',
        kwargs={'year': 2024, 'quarter': 1, 'extracted_fields': extracted_fields}
    )

    result = task.get(timeout=600)
    context.log.info(f"Celery completed: Calculated NPM for {result.get('count')} tickers")

    return result


@asset
def return_on_equity(context: AssetExecutionContext, extracted_fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Call Celery task to calculate Return on Equity.

    Receives cached extracted_fields from previous asset.
    """
    context.log.info("Dagster: Calling Celery task - calculate_return_on_equity...")

    task = celery_app.send_task(
        'business-tasks.fundamentals.calculate_return_on_equity',
        kwargs={'year': 2024, 'quarter': 1, 'extracted_fields': extracted_fields}
    )

    result = task.get(timeout=600)
    context.log.info(f"Celery completed: Calculated ROE for {result.get('count')} tickers")

    return result
