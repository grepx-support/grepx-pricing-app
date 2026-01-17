"""Calculate volatility for tickers based on close prices."""
import logging
import math
from typing import Dict, Any, List
from datetime import datetime

from .base import CalculateTask
from .. import config

logger = logging.getLogger(__name__)


class CalculateVolatility(CalculateTask):
    """Calculate volatility for tickers."""

    def __init__(self):
        super().__init__(
            task_name="calculate_volatility",
            collection_name="volatility",
            storage_name=config.METRICS_STORAGE
        )

    def calculate(self, tickers: List[str] = None, period: int = 20, date: str = None) -> List[Dict[str, Any]]:
        """
        Calculate volatility for tickers.

        Volatility is calculated as the standard deviation of daily returns
        over the specified period.

        Args:
            tickers: List of tickers (default: fetch from active_stocks)
            period: Number of days for volatility calculation (default: 20)
            date: Date for the calculation (default: today)

        Returns:
            List of volatility records
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        # Get tickers from active_stocks if not provided
        if tickers is None:
            active_stocks = self.query_data(
                collection="active_stocks",
                storage=config.TICKER_STORAGE
            )
            tickers = [record.get("ticker") for record in active_stocks if record.get("ticker")]
            logger.info(f"Fetched {len(tickers)} tickers from active_stocks")

        if not tickers:
            logger.warning("No tickers to calculate volatility for")
            return []

        records = []

        for ticker in tickers:
            try:
                # Fetch close prices for the ticker
                close_prices = self.query_data(
                    query={"ticker": ticker},
                    collection="close_price",
                    storage=config.TICKER_STORAGE
                )

                if len(close_prices) < 2:
                    logger.warning(f"Not enough price data for {ticker}")
                    continue

                # Sort by date and get prices
                close_prices.sort(key=lambda x: x.get("date", ""))
                prices = [float(p.get("price", 0)) for p in close_prices[-period:] if p.get("price")]

                if len(prices) < 2:
                    logger.warning(f"Not enough valid prices for {ticker}")
                    continue

                # Calculate daily returns
                returns = []
                for i in range(1, len(prices)):
                    if prices[i - 1] > 0:
                        daily_return = (prices[i] - prices[i - 1]) / prices[i - 1]
                        returns.append(daily_return)

                if not returns:
                    continue

                # Calculate volatility (standard deviation of returns)
                mean_return = sum(returns) / len(returns)
                variance = sum((r - mean_return) ** 2 for r in returns) / len(returns)
                volatility = math.sqrt(variance)

                # Annualize volatility (assuming 252 trading days)
                annualized_volatility = volatility * math.sqrt(252)

                records.append({
                    "ticker": ticker,
                    "date": date,
                    "volatility": round(volatility, 6),
                    "annualized_volatility": round(annualized_volatility, 6),
                    "period": period,
                    "source": "calculated"
                })

                logger.debug(f"Calculated volatility for {ticker}: {volatility:.6f}")

            except Exception as e:
                logger.error(f"Failed to calculate volatility for {ticker}: {e}")
                continue

        logger.info(f"Calculated volatility for {len(records)} tickers")
        return records


# Task instance
_task_instance = CalculateVolatility()


def calculate_volatility(tickers: List[str] = None, period: int = 20, date: str = None) -> Dict[str, Any]:
    """
    Calculate volatility for tickers and store to database.

    Args:
        tickers: List of tickers (default: fetch from active_stocks)
        period: Number of days for volatility calculation (default: 20)
        date: Date for the calculation (default: today)

    Returns:
        Result dictionary with status and counts
    """
    return _task_instance.execute(tickers=tickers, period=period, date=date)
