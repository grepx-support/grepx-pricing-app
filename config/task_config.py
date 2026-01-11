"""Configuration for task execution and default values."""

DEFAULT_DB_CONFIG = {
    'type': 'mongodb',
    'connection_string': 'mongodb+srv://grepx_mongo_user:password@grepx.f0vzhpn.mongodb.net/stock_analysis_demo?appName=grepx',
    'database_name': 'stock_analysis_demo'
}

DEFAULT_TICKERS = ['AAPL', 'MSFT', 'GOOG']

DEFAULT_PERIODS = {
    'sma': 20,
    'ema': 20,
    'rsi': 14,
    'macd_fast': 12,
    'macd_slow': 26,
    'macd_signal': 9,
    'bollinger': 20,
    'bollinger_std': 2.0,
    'atr': 14,
    'stochastic': 14,
    'stochastic_k': 3,
    'stochastic_d': 3,
    'adx': 14,
    'cci': 20,
    'williams_r': 14
}

DOWNLOAD_PERIOD = '1y'

