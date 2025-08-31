import sys
import os
import sqlite3
import time
from pathlib import Path
from pybit.unified_trading import HTTP
from dotenv import load_dotenv

# === Добавляем корень проекта bybit-bot в sys.path ===
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[3]  # .../bybit-bot/
sys.path.insert(0, str(PROJECT_ROOT))

# === ВАЖНО: импорт из backend.config ===
from backend.config.timeframes_config import TIMEFRAMES_CONFIG
from backend.core.dim.ezdim import EzDIM

# === 🔐 Загрузка переменных окружения ===
load_dotenv()

SYMBOL = os.getenv("SYMBOL", "BTCUSDT")
DB_PATH = os.getenv(
    "DB_PATH", str(PROJECT_ROOT / "backend" / "db" / "market_data.sqlite")
)

# === 🌐 Настройка таймфреймов для Bybit API ===

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

session = HTTP(testnet=False)

# === Получение меток ===


def get_earliest_db_timestamp(tf):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT MIN(timestamp) FROM candles_{tf} WHERE symbol = ?", (SYMBOL,)
        )
        row = cursor.fetchone()
        earliest_db = row[0] if row and row[0] else None
        print(f"[DEBUG] earliest_db for {tf} = {earliest_db}")
        return earliest_db
    except sqlite3.OperationalError:
        return None
    finally:
        conn.close()


def get_sorted_timestamps(tf):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT timestamp FROM candles_{tf} WHERE symbol = ? ORDER BY timestamp",
            (SYMBOL,),
        )
        return [row[0] for row in cursor.fetchall()]
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


def find_missing_ranges(tf, required_start):
    timestamps = get_sorted_timestamps(tf)
    if len(timestamps) < 2:
        return []

    interval = TIMEFRAMES_CONFIG[tf]["interval_sec"]
    ranges = []

    prev = timestamps[0]
    for current in timestamps[1:]:
        if current > prev + interval:
            gap_start = prev + interval
            # Если gap_start уходит дальше allowed, корректируем
            if gap_start < required_start:
                gap_start = required_start
            if gap_start <= current - interval:
                ranges.append((gap_start, current - interval))
        prev = current

    return ranges


def get_missing_gaps(tf):
    now = int(time.time())
    interval = TIMEFRAMES_CONFIG[tf]["interval_sec"]
    history = TIMEFRAMES_CONFIG[tf].get("allowed_history")
    if history:
        required_start = now - history
    else:
        # Если не указано — грузим максимально возможную глубину
        default_years = 5  # по опыту API выдержит до 5 лет на крупных ТФ
        required_start = now - default_years * 365 * 86400
        print(f"⚠️ allowed_history не указан для {tf}, используем {default_years} лет")

    gaps = []

    earliest_db = get_earliest_db_timestamp(tf)
    if not earliest_db or earliest_db > required_start:
        gap_start = required_start
        gap_end = earliest_db - interval if earliest_db else now
        if gap_start <= gap_end:
            gaps.append((gap_start, gap_end))
            print(f"🔍 Пропуск в начале: {gap_start} → {gap_end}")

    internal = find_missing_ranges(tf, required_start)
    if internal:
        print(f"🔍 Найдено внутренних пропусков: {len(internal)}")
        gaps += internal

    # 🛡 Гарантируем, что все end в секундах, не миллисекундах
    gaps = [(start, end if end < 1e12 else end // 1000) for start, end in gaps]
    return gaps


# === Работа с API и БД ===


def fetch_candles_batch(tf, start_ts, end_ts):
    interval = INTERVAL_MAP[tf]
    step = TIMEFRAMES_CONFIG[tf]["interval_sec"] * 200
    all_candles = []
    current = start_ts

    while current <= end_ts:
        # Удалены debug print-ы
        try:
            res = session.get_kline(
                category="linear",
                symbol=SYMBOL,
                interval=interval,
                start=current * 1000,
                end=(min(end_ts, current + step)) * 1000,
                limit=200,
            )
            candles = res["result"].get("list", [])
            batch = [
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
            all_candles.extend(batch)
        except Exception as e:
            print(f"⚠️ API ошибка: {e}")
        current += step
        time.sleep(0.05)

    # Создаем DataFrame для валидации
    import pandas as pd

    df = pd.DataFrame(all_candles)

    # Валидация данных перед возвратом
    if not df.empty:
        EzDIM.preflight(
            df,
            required_cols=["timestamp", "open", "high", "low", "close"],
            min_rows=1,
            tf_sec=TIMEFRAMES_CONFIG[tf]["interval_sec"],
        )

    return all_candles


def insert_candles_bulk(tf, candles):
    if not candles:
        return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    table = f"candles_{tf}"
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            symbol TEXT NOT NULL,
            timestamp INTEGER PRIMARY KEY,
            timestamp_ns INTEGER NOT NULL,
            timestamp_ms INTEGER NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL
        )
    """
    )
    data = []
    for c in candles:
        ts = c["timestamp"]
        data.append(
            (
                SYMBOL,
                ts,
                ts * 1_000_000_000,
                ts * 1000,
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


# === Главный алгоритм ===


def main():
    print(f"🎯 Символ: {SYMBOL}")
    print(f"💾 БД: {DB_PATH}")

    for tf in TIMEFRAMES_CONFIG:
        print(f"\n📦 Обработка {tf}...")
        gaps = get_missing_gaps(tf)
        if not gaps:
            print(f"✅ Пропусков нет для {tf} (БД заполнена)")
            continue

        total = 0
        for start, end in gaps:
            print(f"⏳ Загрузка: {start} → {end}")
            candles = fetch_candles_batch(tf, start, end)
            insert_candles_bulk(tf, candles)
            if len(candles) > 0:
                print(f"✅ Загружено: {len(candles)} свечей")
            total += len(candles)

        print(f"🧮 Всего загружено: {total} в {tf}")


if __name__ == "__main__":
    main()
