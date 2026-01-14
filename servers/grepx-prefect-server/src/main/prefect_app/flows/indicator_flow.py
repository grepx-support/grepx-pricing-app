"""
Indicator Calculation Flow Module
"""
from prefect import flow, task
from typing import Optional, List, Any, Dict
import sys
from pathlib import Path

# Add business tasks to path - path relative to project root (where worker runs from)
# When running as deployment, working directory is project root (due to pull step)
business_tasks_path = Path("business-tasks").resolve()
sys.path.insert(0, str(business_tasks_path))

from tasks.stock_tasks import fetch_stock_prices, calculate_volatility, calculate_metrics, analyze_portfolio, daily_analysis, sector_performance


@task(name="calculate_volatility_task")
def calculate_volatility_task(data):
    """
    Task to calculate volatility using business task
    """
    tickers = ["AAPL", "MSFT", "GOOGL"]  # Default tickers
    result = calculate_volatility(tickers)
    print(f"Calculated volatility: {result}")
    return result


@task(name="calculate_metrics_task")
def calculate_metrics_task(data):
    """
    Task to calculate metrics using business task
    """
    tickers = ["AAPL", "MSFT", "GOOGL"]  # Default tickers
    result = calculate_metrics(tickers)
    print(f"Calculated metrics: {result}")
    return result


@task(name="consolidate_indicators")
def consolidate_indicators_task(volatility_data, metrics_data):
    """
    Task to consolidate all calculated indicators
    """
    consolidated_data = {
        "volatility": volatility_data,
        "metrics": metrics_data
    }
    print(f"Consolidated indicators: {consolidated_data}")
    return consolidated_data


@flow(name="calculate_indicators", log_prints=True)
def calculate_indicators(input_data: Optional[Any] = None, ma_window: int = 20, rsi_period: int = 14, bb_window: int = 20, bb_num_std: int = 2, macd_fast: int = 12, macd_slow: int = 26, macd_signal: int = 9):
    """
    Main flow for calculating technical indicators using business tasks
    """
    print("Starting technical indicator calculations...")
    
    # Calculate different indicators using business tasks
    volatility_data = calculate_volatility_task(input_data)
    metrics_data = calculate_metrics_task(input_data)
    
    # Consolidate all indicators
    consolidated_data = consolidate_indicators_task(volatility_data, metrics_data)
    
    # Prepare result
    result = {
        "status": "completed",
        "processed_tickers": ["AAPL", "MSFT", "GOOGL"],
        "indicators_calculated": ["Volatility", "Metrics"],
        "timestamp": "2026-01-12",
        "data_points_per_ticker": {"sample": 100}  # Placeholder
    }
    
    print(f"Technical indicator calculations completed: {result}")
    return result


if __name__ == "__main__":
    # Example run
    result = calculate_indicators()
    print("Flow result:", result)