"""
data_backfill.py

Загружает до 200 исторических свечей для BTCUSDT с Bybit (realnet)
Сохраняет в таблицы: candles_1m, candles_5m, ..., candles_1w
"""

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[2]  # .../backend
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from config.timeframes_config import TIMEFRAMES_CONFIG

import sqlite3
from pybit.unified_trading import HTTP

# 📁 Универсальные пути
BASE_DIR = Path(__file__).resolve().parents[2]  # .../backend
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

PROJECT_ROOT = BASE_DIR.parent  # .../bybit-bot
DB_PATH = PROJECT_ROOT / "db" / "market_data.sqlite"

# 🔧 Настройки
SYMBOL = "BTCUSDT"
INTERVAL_MAP = {
    "1m": "1",
    "5m": "5",
    "30m": "30",
    "1h": "60",
    "4h": "240",
    "6h": "360",
    "12h": "720",
    "1d": "D",
    "1w": "W",
}

# 🟢 Старт API-сессии
session = HTTP(testnet=False)  # realnet
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

for tf in TIMEFRAMES_CONFIG.keys():
    if tf not in INTERVAL_MAP:
        print(f"⚠️ Таймфрейм {tf} не поддерживается в INTERVAL_MAP, пропущен")
        continue

    interval = INTERVAL_MAP[tf]
    print(f"📥 Загрузка {tf}...")
    try:
        response = session.get_kline(
            category="linear", symbol=SYMBOL, interval=interval, limit=200
        )
        candles = response["result"]["list"]
        table = f"candles_{tf}"

        for c in candles:
            ts = int(c[0]) // 1000
            ts_ns = int(c[0]) * 1_000_000
            open_, high, low, close, volume = map(float, c[1:6])
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {table}
                (symbol, timestamp, timestamp_ns, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (SYMBOL, ts, ts_ns, open_, high, low, close, volume),
            )

        conn.commit()
        print(f"✅ {len(candles)} свечей записано в {table}")

    except Exception as e:
        print(f"⚠️ Ошибка загрузки {tf}: {e}")

conn.close()
print("🏁 Готово.")
