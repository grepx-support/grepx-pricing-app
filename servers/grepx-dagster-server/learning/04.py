# example_yfinance_pipeline_full.py

from dagster import (
    asset,
    Definitions,
    FilesystemIOManager,
    ConfigurableResource,
    AssetExecutionContext,
    define_asset_job,
    ScheduleDefinition,
    AssetSelection,
)
import yfinance as yf
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any
import yaml


# ============================================================================
# CONFIGURATION MANAGEMENT
# ============================================================================

class AppConfig:
    """
    Load configuration from YAML file or use defaults
    """

    def __init__(self, config_path: str = None):
        if config_path and Path(config_path).exists():
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
        else:
            # Default configuration
            self.config = {
                'app': {
                    'name': 'dagster_yfinance',
                    'version': '1.0.0',
                    'environment': 'local'
                },
                'storage': {
                    'base_dir': './storage'
                },
                'stocks': {
                    'tickers': ['AAPL', 'MSFT', 'GOOG', 'AMZN'],
                    'start_date': '2023-01-01',
                    'end_date': '2023-12-31'
                }
            }

    def get(self, key: str, default: Any = None) -> Any:
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
        return value if value is not None else default


config = AppConfig()

storage_dir = Path(config.get('storage.base_dir', './storage'))
storage_dir.mkdir(exist_ok=True)


# ============================================================================
# RESOURCES (Optional - for DB or API connections)
# ============================================================================

class DataWarehouse(ConfigurableResource):
    """Simulated DB connection"""
    connection_string: str = "sqlite:///data.db"

    def execute_query(self, query: str) -> pd.DataFrame:
        print(f"Executing query: {query[:50]}...")
        return pd.DataFrame({'result': ['Mock data']})

    def write_table(self, df: pd.DataFrame, table_name: str):
        print(f"Writing {len(df)} rows to table '{table_name}'")


# ============================================================================
# ASSETS - DATA INGESTION LAYER
# ============================================================================

@asset(
    group_name="data_ingestion",
    description="Download historical stock data from Yahoo Finance"
)
def stock_data(context: AssetExecutionContext) -> pd.DataFrame:
    tickers = config.get("stocks.tickers")
    start_date = config.get("stocks.start_date")
    end_date = config.get("stocks.end_date")

    context.log.info(f"Downloading stock data for {tickers} from {start_date} to {end_date}")

    all_data = {}
    for ticker in tickers:
        df = yf.download(ticker, start=start_date, end=end_date)
        if not df.empty:
            all_data[ticker] = df
            context.log.info(f"Downloaded {len(df)} rows for {ticker}")
        else:
            context.log.warning(f"No data found for {ticker}")

    combined_df = pd.concat(all_data.values(), keys=all_data.keys(), names=["Ticker", "Date"])
    combined_df.reset_index(inplace=True)
    combined_df.to_csv(storage_dir / "raw_stock_data.csv", index=False)

    context.add_output_metadata({
        "num_tickers": len(all_data),
        "rows_total": len(combined_df),
        "storage_file": str(storage_dir / "raw_stock_data.csv")
    })

    return combined_df


# ============================================================================
# ASSETS - TRANSFORMATION LAYER
# ============================================================================

@asset(
    group_name="transformation",
    description="Compute daily returns and rolling volatility"
)
def stock_metrics(context: AssetExecutionContext, stock_data: pd.DataFrame) -> pd.DataFrame:
    df = stock_data.copy()

    # Daily return
    df['Daily_Return'] = df.groupby('Ticker')['Adj Close'].pct_change()

    # Rolling volatility (20-day)
    df['Volatility_20d'] = df.groupby('Ticker')['Daily_Return'].rolling(window=20).std().reset_index(0, drop=True)

    # Moving averages (SMA 20 & 50)
    df['SMA_20'] = df.groupby('Ticker')['Adj Close'].transform(lambda x: x.rolling(20).mean())
    df['SMA_50'] = df.groupby('Ticker')['Adj Close'].transform(lambda x: x.rolling(50).mean())

    # Exponential moving average
    df['EMA_20'] = df.groupby('Ticker')['Adj Close'].transform(lambda x: x.ewm(span=20, adjust=False).mean())

    # Momentum indicator: change over 5 days
    df['Momentum_5d'] = df.groupby('Ticker')['Adj Close'].transform(lambda x: x - x.shift(5))

    # Bollinger Bands
    df['BB_upper'] = df['SMA_20'] + 2 * df['Volatility_20d']
    df['BB_lower'] = df['SMA_20'] - 2 * df['Volatility_20d']

    # Save metrics
    df.to_csv(storage_dir / "stock_metrics.csv", index=False)

    # Summary metadata
    metrics_summary = df.groupby('Ticker')['Daily_Return'].agg(
        mean_return='mean',
        std_return='std',
        max_return='max',
        min_return='min'
    ).reset_index()

    context.add_output_metadata({
        "num_tickers": df['Ticker'].nunique(),
        "num_rows": len(df),
        "metrics_summary": metrics_summary.to_dict(orient='records')
    })

    context.log.info(f"Calculated metrics for {df['Ticker'].nunique()} tickers, {len(df)} rows total")
    return df


# ============================================================================
# ASSETS - ANALYTICS LAYER
# ============================================================================

@asset(
    group_name="analytics",
    description="Rank stocks by risk-adjusted return"
)
def stock_factors(context: AssetExecutionContext, stock_metrics: pd.DataFrame) -> pd.DataFrame:
    df = stock_metrics.copy()
    df['Sharpe_Ratio'] = df['Daily_Return'] / df['Volatility_20d']
    df['Risk_Category'] = pd.cut(df['Volatility_20d'], bins=[-np.inf, 0.01, 0.02, np.inf],
                                 labels=['Low', 'Medium', 'High'])

    context.add_output_metadata({
        "num_tickers": df['Ticker'].nunique(),
        "avg_sharpe": df['Sharpe_Ratio'].mean()
    })

    df.to_csv(storage_dir / "stock_factors.csv", index=False)
    return df


# ============================================================================
# JOBS - Define asset groups
# ============================================================================

daily_etl_job = define_asset_job(
    name="daily_etl",
    selection=AssetSelection.groups("data_ingestion", "transformation", "analytics"),
    description="Daily ETL pipeline: download → metrics → factors"
)

# Quick refresh: only analytics/factors
analytics_refresh_job = define_asset_job(
    name="analytics_refresh",
    selection=AssetSelection.groups("analytics"),
    description="Refresh analytics assets only"
)

# ============================================================================
# SCHEDULES
# ============================================================================

daily_schedule = ScheduleDefinition(
    name="daily_pipeline",
    target=daily_etl_job,
    cron_schedule="0 2 * * *",  # 2 AM daily
    description="Run full pipeline daily"
)

# ============================================================================
# DEFINITIONS - Bring it all together
# ============================================================================

defs = Definitions(
    assets=[stock_data, stock_metrics, stock_factors],
    jobs=[daily_etl_job, analytics_refresh_job],
    schedules=[daily_schedule],
    resources={
        "io_manager": FilesystemIOManager(base_dir=str(storage_dir)),  # <-- cast to str
        "data_warehouse": DataWarehouse()
    }
)
