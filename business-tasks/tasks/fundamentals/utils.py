"""Utilities for fundamental analysis - parameter extraction, metric calculation, and storage."""
import asyncio
import logging
import aiohttp
from typing import Dict, Any, Callable
from .. import config

logger = logging.getLogger(__name__)


class ParamExtractor:
    """Extracts and validates parameters from kwargs"""

    @staticmethod
    def extract_params(kwargs: Dict[str, Any]) -> tuple:
        """Extract year, quarter, and extracted_data from kwargs"""
        year = kwargs.get('year')
        quarter = kwargs.get('quarter')

        if not year or not quarter:
            raise ValueError("year and quarter parameters required")

        extracted_data = kwargs.get('extracted_fields')
        if not extracted_data:
            raise ValueError("extracted_fields is required")

        logger.info(f"Extracted params - year: {year}, quarter: {quarter}, tickers: {len(extracted_data)}")
        return year, quarter, extracted_data


class MetricStorage:
    """Handles saving metrics to MongoDB"""

    @staticmethod
    async def save_metric(ticker: str, metric_name: str, value: float, year: int, quarter: int):
        """Save a single metric to MongoDB with pattern: ticker_name_formula_name"""
        collection = f"{ticker.lower()}_{metric_name}"
        payload = {
            "storage_name": config.METRICS_STORAGE,
            "model_class_name": collection,
            "data": {
                "ticker": ticker,
                "metric_name": metric_name,
                "value": value,
                "year": year,
                "quarter": quarter
            }
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{config.API_URL}/write", json=payload) as response:
                    if response.status != 200:
                        logger.error(f"Failed to save {metric_name} for {ticker}: status {response.status}")
        except Exception as e:
            logger.error(f"Error saving metric for {ticker}: {str(e)}", exc_info=True)


class MetricCalculator:
    """Calculates metrics and stores results"""

    @staticmethod
    def calculate_metric(extracted_data: Dict, metric_name: str, year: int, quarter: int,
                        calculation_func: Callable) -> Dict[str, Any]:
        """Generic metric calculation and storage"""
        results = []

        for ticker, fields in extracted_data.items():
            try:
                value = calculation_func(fields)
                asyncio.run(MetricStorage.save_metric(ticker, metric_name, value, year, quarter))
                results.append({"ticker": ticker, "value": round(value, 2) if value is not None else None})
                logger.info(f"Calculated {metric_name} for {ticker}: {round(value, 2)}")
            except Exception as e:
                logger.error(f"Error calculating {metric_name} for {ticker}: {str(e)}", exc_info=True)

        return {
            "status": "success",
            "metric_name": metric_name,
            "results": results,
            "count": len(results)
        }


# Backward compatibility - keep old function names
def extract_params(kwargs: Dict[str, Any]) -> tuple:
    """Extract year, quarter, and extracted_data from kwargs"""
    return ParamExtractor.extract_params(kwargs)


def calculate_metric(extracted_data: Dict, metric_name: str, year: int, quarter: int,
                    calculation_func: Callable) -> Dict[str, Any]:
    """Generic metric calculation and storage"""
    return MetricCalculator.calculate_metric(extracted_data, metric_name, year, quarter, calculation_func)