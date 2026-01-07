from dagster import (
    asset,
    Definitions,
    DailyPartitionsDefinition,
    AssetExecutionContext,
    IOManager,
)
import yfinance as yf
import pandas as pd
import sqlite3
from pathlib import Path
import logging

# -----------------------------
# SQLite I/O Manager with logging
# -----------------------------
class SQLiteIOManager(IOManager):
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def handle_output(self, context, obj: pd.DataFrame):
        table_name = context.asset_key.path[-1]
        partition = context.partition_key or "no_partition"

        context.log.info(f"Saving partition [{partition}] to SQLite table [{table_name}]...")

        with self._get_connection() as conn:
            obj.to_sql(
                name=table_name,
                con=conn,
                if_exists="replace",  # Replace table for simplicity
                index=True,
            )

        context.log.info(f"Saved partition [{partition}] to table [{table_name}] in SQLite")

    def load_input(self, context):
        table_name = context.asset_key.path[-1]

        context.log.info(f"Loading data from SQLite table [{table_name}]...")
        with self._get_connection() as conn:
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

        context.log.info(f"Loaded {len(df)} rows from table [{table_name}]")
        return df


# -----------------------------
# Partitions (one per day)
# -----------------------------
daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")


# -----------------------------
# Asset: Download AAPL stock data
# -----------------------------
@asset(partitions_def=daily_partitions)
def aapl_daily_price(context: AssetExecutionContext) -> pd.DataFrame:
    day = context.partition_key
    context.log.info(f"Starting download for partition: {day}")

    ticker = yf.Ticker("AAPL")

    df = ticker.history(
        start=day,
        end=pd.to_datetime(day) + pd.Timedelta(days=1),
    )

    context.log.info(f"Downloaded {len(df)} rows for {day}")
    return df


# -----------------------------
# Definitions
# -----------------------------
db_dir = Path(__file__).parent / "storage"
db_dir.mkdir(exist_ok=True)

defs = Definitions(
    assets=[aapl_daily_price],
    resources={
        "io_manager": SQLiteIOManager(
            db_path=str(db_dir / "market_data.db")
        )
    },
)
