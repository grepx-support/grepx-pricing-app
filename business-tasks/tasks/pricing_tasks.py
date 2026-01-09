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


def calculate_indicator(ticker: str, indicator_type: str, params: dict = None, db_config=None):
    """Generic indicator calculation."""
    from tasks.indicators import sma, ema, rsi, macd, bollinger_bands, atr, stochastic, adx, obv, vwap, cci, williams_r
    
    db_config = db_config or DEFAULT_DB_CONFIG
    params = params or {}
    
    indicator_map = {
        'sma': (sma.SMA, ['period']),
        'ema': (ema.EMA, ['period']),
        'rsi': (rsi.RSI, ['period']),
        'macd': (macd.MACD, ['fast_period', 'slow_period', 'signal_period']),
        'bollinger_bands': (bollinger_bands.BollingerBands, ['period', 'std_dev']),
        'atr': (atr.ATR, ['period']),
        'stochastic': (stochastic.Stochastic, ['period', 'smooth_k', 'smooth_d']),
        'adx': (adx.ADX, ['period']),
        'obv': (obv.OBV, []),
        'vwap': (vwap.VWAP, []),
        'cci': (cci.CCI, ['period']),
        'williams_r': (williams_r.WilliamsR, ['period'])
    }
    
    if indicator_type not in indicator_map:
        return {'error': f'Unknown indicator: {indicator_type}'}
    
    cls, param_names = indicator_map[indicator_type]
    indicator_params = {k: params.get(k) for k in param_names if k in params}
    indicator_params = {k: v for k, v in indicator_params.items() if v is not None}
    
    db = get_database(db_config)
    db.connect()
    price_data = db.get_price_data(ticker, params.get('start_date'), params.get('end_date'))
    
    indicator = cls(**indicator_params) if indicator_params else cls()
    records = indicator.calculate(price_data)
    
    if records:
        db.insert_many('indicators', records)
    
    db.disconnect()
    
    return {'ticker': ticker, 'indicator': indicator.name, 'count': len(records), 'status': 'success'}

