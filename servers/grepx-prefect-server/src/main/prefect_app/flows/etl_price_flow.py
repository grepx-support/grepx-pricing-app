"""
ETL Price Flow Module
"""
from prefect import flow, task
from typing import Optional, List, Any
import sys
from pathlib import Path

# Add business tasks to path - path relative to project root (where worker runs from)
# When running as deployment, working directory is project root (due to pull step)
business_tasks_path = Path("business-tasks").resolve()
sys.path.insert(0, str(business_tasks_path))

from tasks.stock_tasks import fetch_stock_prices, calculate_volatility, calculate_metrics, analyze_portfolio, daily_analysis, sector_performance


@task(name="fetch_price_data", retries=3, retry_delay_seconds=5)
def fetch_price_data_task(tickers: list = None, days: int = 30):
    """
    Task to fetch price data using the business task
    """
    if tickers is None:
        tickers = ["AAPL", "MSFT", "GOOGL"]
    
    result = fetch_stock_prices(tickers, days)
    print(f"Fetched data: {result}")
    return result


@task(name="clean_price_data")
def clean_price_data_task(raw_data):
    """
    Task to clean and preprocess the raw price data
    """
    # Business logic would be in the original function
    # For now, we'll return the raw data as is
    print(f"Cleaning data: {type(raw_data)}")
    return raw_data


@task(name="validate_price_data")
def validate_price_data_task(cleaned_data) -> bool:
    """
    Task to validate the cleaned price data
    """
    # Validation logic
    is_valid = cleaned_data is not None and len(cleaned_data) > 0
    print(f"Data validation result: {'PASS' if is_valid else 'FAIL'}")
    return is_valid


@flow(name="process_price_data", log_prints=True)
def process_price_data(tickers: Optional[List[Any]] = None, days: int = 30):
    """
    Main ETL flow for processing price data using business tasks
    """
    print(f"Starting ETL process for tickers: {tickers or ['Default tickers']}")
    
    # Fetch raw data using business task
    raw_data = fetch_price_data_task(tickers, days)
    
    # Clean the data
    cleaned_data = clean_price_data_task(raw_data)
    
    # Validate the data
    is_valid = validate_price_data_task(cleaned_data)
    
    if not is_valid:
        print("Warning: Some data validation checks failed")
    
    # Prepare result
    result = {
        "status": "completed",
        "processed_tickers": tickers or ["AAPL", "MSFT", "GOOGL"],
        "validation_passed": is_valid,
        "timestamp": "2026-01-12",
        "record_counts": {"sample": 100}  # Placeholder
    }
    
    print(f"ETL process completed: {result}")
    return result


if __name__ == "__main__":
    # Example run
    result = process_price_data(tickers=["AAPL", "MSFT"], days=7)
    print("Flow result:", result)