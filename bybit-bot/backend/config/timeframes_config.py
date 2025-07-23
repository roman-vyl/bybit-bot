# config/timeframes_config.py

TIMEFRAMES_CONFIG = {
    "1m": {
        "interval_sec": 60,
        "allowed_history": 60 * 60 * 24 * 7,
        "initial_candles": 500,
    },  # 7 дней
    "5m": {
        "interval_sec": 300,
        "allowed_history": 60 * 60 * 24 * 60,
        "initial_candles": 500,
    },  # 60 дней
    "30m": {
        "interval_sec": 1800,
        "allowed_history": 60 * 60 * 24 * 180,
        "initial_candles": 400,
    },  # 180 дней
    "1h": {"interval_sec": 3600, "allowed_history": None, "initial_candles": 300},
    "4h": {"interval_sec": 14400, "allowed_history": None, "initial_candles": 200},
    "6h": {"interval_sec": 21600, "allowed_history": None, "initial_candles": 150},
    "12h": {"interval_sec": 43200, "allowed_history": None, "initial_candles": 120},
    "1d": {"interval_sec": 86400, "allowed_history": None, "initial_candles": 100},
    "1w": {"interval_sec": 604800, "allowed_history": None, "initial_candles": 20},
}
