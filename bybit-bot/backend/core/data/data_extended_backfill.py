import sys
import sqlite3
from pathlib import Path
import time
from pybit.unified_trading import HTTP

sys.path.append(str(Path(__file__).resolve().parents[2]))
BASE_DIR = Path(__file__).resolve().parents[2]  # backend/
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))
from config.timeframes_config import TIMEFRAMES_CONFIG

PROJECT_ROOT = BASE_DIR.parent  # bybit-bot/
DB_PATH = PROJECT_ROOT / "db" / "market_data.sqlite"

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

session = HTTP(testnet=False)  # realnet


def find_missing_timestamps(tf):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    table = f"candles_{tf}"
    cursor.execute(f"SELECT timestamp FROM {table}")
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return []

    timestamps = sorted({row[0] for row in rows})
    interval = TIMEFRAMES_CONFIG[tf]["interval_sec"]
    expected = set(range(min(timestamps), max(timestamps) + interval, interval))
    missing = sorted(expected - set(timestamps))
    return missing


def insert_candles_bulk(tf, candles):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    table = f"candles_{tf}"
    data = []
    for c in candles:
        timestamp_ns = c["timestamp"] * 1000 * 1_000_000
        timestamp_ms = timestamp_ns // 1_000_000
        timestamp = timestamp_ms // 1000
        data.append(
            (
                SYMBOL,
                timestamp,
                timestamp_ns,
                timestamp_ms,
                c["open"],
                c["high"],
                c["low"],
                c["close"],
                c["volume"],
            )
        )
    cursor.executemany(
        f"""
        INSERT OR IGNORE INTO {table}
        (symbol, timestamp, timestamp_ns, timestamp_ms, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        data,
    )
    conn.commit()
    conn.close()


def fetch_candles_batch(tf, start_ts, end_ts):
    interval = INTERVAL_MAP[tf]
    all_candles = []
    current = start_ts
    step = TIMEFRAMES_CONFIG[tf]["interval_sec"] * 200

    while current < end_ts:
        try:
            response = session.get_kline(
                category="linear",
                symbol=SYMBOL,
                interval=interval,
                start=current * 1000,
                end=min(end_ts, current + step) * 1000,
                limit=200,
            )
            candles = response["result"].get("list", [])
            parsed = [
                {
                    "timestamp": int(c[0]) // 1000,
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": float(c[5]),
                }
                for c in candles
            ]
            all_candles.extend(parsed)
            print(f"📥 Загружено {len(parsed)} свечей @ {tf} {current}")
        except Exception as e:
            print(f"⚠️  Ошибка API: {e}")
        current += step
        time.sleep(0.05)
    return all_candles


def main():
    for tf in TIMEFRAMES_CONFIG:
        print(f"\n📦 Проверка и перезагрузка {tf}...")

        missing = find_missing_timestamps(tf)
        if not missing:
            print(f"✅ Пропусков нет в {tf}, ничего не делаем")
            continue

        interval = TIMEFRAMES_CONFIG[tf]["interval_sec"]
        margin = interval * 200
        from_ts = min(missing) - margin
        to_ts = max(missing) + margin

        print(f"🔍 Пропуски найдены. Перезагрузка диапазона {from_ts} → {to_ts}")

        candles = fetch_candles_batch(tf, from_ts, to_ts)
        insert_candles_bulk(tf, candles)
        print(f"✅ Обновлено {len(candles)} свечей в {tf}")


if __name__ == "__main__":
    main()
