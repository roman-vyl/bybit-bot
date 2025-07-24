"""
Запуск realtime загрузчика свечей с Bybit.
"""

import logging

logging.basicConfig(level=logging.INFO)
print("🚀 Bybit real-time loader started...")

from backend.bybit_realtime_data_loader.manager import Manager
from backend.config.timeframes_config import TIMEFRAMES_CONFIG


def convert_tf(tf: str) -> str:
    return {
        "1m": "1",
        "5m": "5",
        "15m": "15",
        "30m": "30",
        "1h": "60",
        "4h": "240",
        "1d": "D",
        "1w": "W",
        "6h": "360",
        "12h": "720",
    }.get(tf)


if __name__ == "__main__":
    symbols = ["BTCUSDT"]
    intervals = [convert_tf(tf) for tf in TIMEFRAMES_CONFIG if convert_tf(tf)]
    manager = Manager(symbols=symbols, intervals=intervals)
    manager.run()
