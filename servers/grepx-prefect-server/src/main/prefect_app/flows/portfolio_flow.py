"""
Portfolio Analysis Flow Module
"""
from prefect import flow, task
import sys
from pathlib import Path

# Add business tasks to path
business_tasks_path = Path(__file__).parent.parent.parent.parent.parent.parent / "business-tasks"
sys.path.insert(0, str(business_tasks_path))

from tasks.stock_tasks import fetch_stock_prices, calculate_volatility, calculate_metrics, analyze_portfolio, daily_analysis, sector_performance


@task(name="analyze_portfolio_task")
def analyze_portfolio_task():
    """
    Task to analyze portfolio using business task
    """
    tickers = ["AAPL", "MSFT", "GOOGL"]
    result = analyze_portfolio(tickers)
    print(f"Portfolio analysis result: {result}")
    return result


@task(name="calculate_sector_performance")
def calculate_sector_performance_task():
    """
    Task to calculate sector performance using business task
    """
    tickers = ["AAPL", "MSFT", "GOOGL"]
    result = sector_performance(tickers)
    print(f"Sector performance result: {result}")
    return result


@task(name="generate_portfolio_summary")
def generate_portfolio_summary_task(portfolio_data, sector_data):
    """
    Task to generate a comprehensive portfolio summary
    """
    summary = {
        "portfolio_analysis": portfolio_data,
        "sector_performance": sector_data,
        "status": "completed",
        "timestamp": "2026-01-12"
    }
    print(f"Portfolio summary: {summary}")
    return summary


@flow(name="analyze_portfolio", log_prints=True)
def analyze_portfolio_flow(price_data=None, weights: dict = None, risk_free_rate: float = 0.02):
    """
    Main flow for portfolio analysis using business tasks
    """
    print("Starting portfolio analysis...")
    
    # Analyze portfolio using business task
    portfolio_result = analyze_portfolio_task()
    
    # Calculate sector performance
    sector_result = calculate_sector_performance_task()
    
    # Generate summary
    summary = generate_portfolio_summary_task(portfolio_result, sector_result)
    
    # Final result
    result = {
        "status": "completed",
        "analysis_date": "2026-01-12",
        "assets_analyzed": ["AAPL", "MSFT", "GOOGL"],
        "portfolio_weights": weights or {"AAPL": 0.4, "MSFT": 0.4, "GOOGL": 0.2},
        "metrics": summary
    }
    
    print(f"Portfolio analysis completed: {result}")
    return result


if __name__ == "__main__":
    # Example run
    result = analyze_portfolio_flow()
    print("Flow result:", result)