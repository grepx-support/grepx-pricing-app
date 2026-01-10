"""Pricing tasks - Simple wrappers for Celery execution."""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from database.factory import get_database
from config.task_config import DEFAULT_DB_CONFIG


def download_and_store(ticker: str, start_date=None, end_date=None, period='1y', db_config=None):
    """Download and store price data."""
    from tasks.data_download.yahoo_finance_downloader import download_ticker_data
    
    db_config = db_config or DEFAULT_DB_CONFIG
    records = download_ticker_data(ticker, start_date, end_date, period)
    
    if records:
        db = get_database(db_config)
        db.connect()
        db.insert_many('price_data', records)
        db.disconnect()
    
    return {'ticker': ticker, 'count': len(records), 'status': 'success'}


def calculate_indicator(ticker: str, indicator_type: str = None, period: int = None,
                       fast: int = None, slow: int = None, signal: int = None,
                       std_dev: float = None, smooth_k: int = None, smooth_d: int = None,
                       start_date=None, end_date=None, db_config=None, **kwargs):
    """
    Generic indicator calculation that accepts all possible indicator parameters.

    This function handles all technical indicators by accepting their specific parameters
    as optional arguments and routing to the appropriate indicator class.

    Args:
        ticker: Stock ticker symbol
        indicator_type: Type of indicator (sma, ema, rsi, macd, bollinger_bands, atr, stochastic, adx, obv, vwap, cci, williams_r)
        period: Period for most indicators (default varies by indicator)
        fast, slow, signal: MACD-specific parameters
        std_dev: Bollinger Bands standard deviation
        smooth_k, smooth_d: Stochastic-specific smoothing parameters
        start_date, end_date: Date range for price data
        db_config: Database configuration
        **kwargs: Absorbs extra parameters from Dagster (like dependency results)

    Returns:
        Dictionary with ticker, indicator name, record count, and status
    """
    from tasks.indicators import sma, ema, rsi, macd, bollinger_bands, atr, stochastic, adx, obv, vwap, cci, williams_r

    db_config = db_config or DEFAULT_DB_CONFIG

    if not indicator_type:
        return {'error': 'indicator_type is required'}

    # Map indicator types to their classes and build parameters
    indicator_map = {
        'sma': (sma.SMA, {'period': period or 20}),
        'ema': (ema.EMA, {'period': period or 20}),
        'rsi': (rsi.RSI, {'period': period or 14}),
        'macd': (macd.MACD, {'fast_period': fast or 12, 'slow_period': slow or 26, 'signal_period': signal or 9}),
        'bollinger_bands': (bollinger_bands.BollingerBands, {'period': period or 20, 'std_dev': std_dev or 2.0}),
        'atr': (atr.ATR, {'period': period or 14}),
        'stochastic': (stochastic.Stochastic, {'period': period or 14, 'smooth_k': smooth_k or 3, 'smooth_d': smooth_d or 3}),
        'adx': (adx.ADX, {'period': period or 14}),
        'obv': (obv.OBV, {}),
        'vwap': (vwap.VWAP, {}),
        'cci': (cci.CCI, {'period': period or 20}),
        'williams_r': (williams_r.WilliamsR, {'period': period or 14})
    }

    if indicator_type not in indicator_map:
        return {'error': f'Unknown indicator: {indicator_type}'}

    cls, indicator_params = indicator_map[indicator_type]

    # Remove None values
    indicator_params = {k: v for k, v in indicator_params.items() if v is not None}

    # Get price data and calculate
    db = get_database(db_config)
    db.connect()
    price_data = db.get_price_data(ticker, start_date, end_date)

    indicator = cls(**indicator_params) if indicator_params else cls()
    records = indicator.calculate(price_data)

    if records:
        db.insert_many('indicators', records)

    db.disconnect()

    return {'ticker': ticker, 'indicator': indicator.name, 'count': len(records), 'status': 'success'}

