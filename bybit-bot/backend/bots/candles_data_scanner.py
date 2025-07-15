"""
candles_data_scanner_autocreate.py

WebSocket-сканер свечей с Bybit (realnet) по BTCUSDT.
Автоматически создаёт таблицы candles_<tf>, если их нет.
Корректно завершает WebSocket при остановке по Ctrl+C.
"""

import os
import time
import logging
import sqlite3
from dotenv import load_dotenv
from pybit.unified_trading import WebSocket
from pathlib import Path
import threading

# --- Настройка логов ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

# --- Загрузка API ключей ---
load_dotenv()
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

# --- Таймфреймы и таблицы ---
INTERVALS = {
    "1": "1m",
    "5": "5m",
    "30": "30m",
    "60": "1h",
    "240": "4h",
    "360": "6h",
    "720": "12h",
    "D": "1d",
    "W": "1w",
}

# --- БД ---
BASE_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = BASE_DIR.parent  # bybit-bot/
DB_PATH = PROJECT_ROOT / "db" / "market_data.sqlite"
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()

# --- Автосоздание таблиц ---
for tf in INTERVALS.values():
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS candles_{tf} (
            symbol TEXT NOT NULL,
            timestamp INTEGER PRIMARY KEY,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL
        )
    """
    )
conn.commit()

# --- WebSocket ---
ws = WebSocket(
    testnet=False, channel_type="linear", api_key=API_KEY, api_secret=API_SECRET
)

# --- Глобальный флаг ---
running = True


# --- Обработка свечей ---
def make_handler(tf):
    def handle_kline(msg):
        if not running:
            return
        try:
            k = msg["data"][0]
            ts = int(k["start"])
            symbol = msg["topic"].split(".")[-1]
            open_, high, low, close, volume = map(
                float, [k["open"], k["high"], k["low"], k["close"], k["volume"]]
            )
            table = f"candles_{INTERVALS[tf]}"

            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {table}
                (symbol, timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (symbol, ts, open_, high, low, close, volume),
            )
            conn.commit()

            logging.info(f"[{INTERVALS[tf]}] {symbol} | ts: {ts} | close: {close}")

        except Exception as e:
            logging.warning(f"Ошибка обработки свечи [{tf}]: {e}")

    return handle_kline


# --- Подписка на все таймфреймы ---
for tf in INTERVALS:
    ws.kline_stream(callback=make_handler(tf), symbol="BTCUSDT", interval=tf)

# --- Основной цикл ---
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    running = False
    time.sleep(1.0)
    logging.info("🛑 Остановлено вручную.")
    conn.close()
