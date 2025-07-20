import sys
import sqlite3
import os
from pathlib import Path
import time
from pybit.unified_trading import HTTP
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

sys.path.append(str(Path(__file__).resolve().parents[2]))
BASE_DIR = Path(__file__).resolve().parents[2]  # backend/
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))
from config.timeframes_config import TIMEFRAMES_CONFIG

PROJECT_ROOT = BASE_DIR.parent  # bybit-bot/

# Получение переменных из .env
SYMBOL = os.getenv("SYMBOL", "BTCUSDT")  # Fallback на BTCUSDT если не задано
DB_PATH = os.getenv("DB_PATH", str(PROJECT_ROOT / "db" / "market_data.sqlite"))

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


def get_earliest_db_timestamp(tf):
    """Получить самую раннюю метку времени из БД для таймфрейма"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    table = f"candles_{tf}"
    try:
        cursor.execute(
            f"SELECT MIN(timestamp) FROM {table} WHERE symbol = ?", (SYMBOL,)
        )
        result = cursor.fetchone()
        return result[0] if result and result[0] else None
    except sqlite3.OperationalError:
        # Таблица не существует
        return None
    finally:
        conn.close()


def get_earliest_bybit_timestamp(tf):
    """Получить самую раннюю доступную свечу на Bybit для символа и таймфрейма"""
    interval = INTERVAL_MAP[tf]
    try:
        # Запрашиваем самые старые данные (limit=1, без указания времени)
        response = session.get_kline(
            category="linear",
            symbol=SYMBOL,
            interval=interval,
            limit=1000,  # Берем больше для поиска самых ранних данных
        )
        candles = response["result"].get("list", [])
        if candles:
            # Bybit возвращает в порядке убывания времени, берем последнюю (самую раннюю)
            earliest_candle = candles[-1]
            return int(earliest_candle[0]) // 1000  # Конвертируем ms в seconds
        return None
    except Exception as e:
        print(f"⚠️ Ошибка при получении ранних данных с Bybit: {e}")
        return None


def find_missing_timestamps(tf):
    """Найти пропущенные временные метки в БД"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    table = f"candles_{tf}"
    try:
        cursor.execute(f"SELECT timestamp FROM {table} WHERE symbol = ?", (SYMBOL,))
        rows = cursor.fetchall()
    except sqlite3.OperationalError:
        # Таблица не существует
        return []
    finally:
        conn.close()

    if not rows:
        return []

    timestamps = sorted({row[0] for row in rows})
    interval = TIMEFRAMES_CONFIG[tf]["interval_sec"]
    expected = set(range(min(timestamps), max(timestamps) + interval, interval))
    missing = sorted(expected - set(timestamps))
    return missing


def get_missing_gaps(tf):
    """Определить пропущенные участки в начале и внутри данных"""
    # Получаем самую раннюю метку в БД
    earliest_db = get_earliest_db_timestamp(tf)
    if not earliest_db:
        print(f"📝 Таблица {tf} пуста или не существует")

        # Обработка случая пустой таблицы
        earliest_bybit = get_earliest_bybit_timestamp(tf)
        if not earliest_bybit:
            print(f"⚠️ Не удалось получить ранние данные с Bybit для {tf}")
            return []

        now = int(time.time())
        print(f"🔄 Пустая таблица - загружаем весь диапазон: {earliest_bybit} → {now}")
        return [(earliest_bybit, now)]

    # Получаем самую раннюю доступную метку на Bybit
    earliest_bybit = get_earliest_bybit_timestamp(tf)
    if not earliest_bybit:
        print(f"⚠️ Не удалось получить ранние данные с Bybit для {tf}")
        return []

    print(f"📊 {tf}: Bybit earliest={earliest_bybit}, DB earliest={earliest_db}")

    gaps = []
    interval = TIMEFRAMES_CONFIG[tf]["interval_sec"]

    # Пропуск в начале (до самых ранних данных в БД)
    if earliest_bybit < earliest_db:
        gap_start = earliest_bybit
        gap_end = earliest_db - interval
        if gap_start <= gap_end:
            gaps.append((gap_start, gap_end))
            print(f"🔍 Найден пропуск в начале: {gap_start} → {gap_end}")

    # Пропуски внутри существующих данных
    missing = find_missing_timestamps(tf)
    if missing:
        # Группируем последовательные пропуски в диапазоны
        current_start = missing[0]
        current_end = missing[0]

        for ts in missing[1:]:
            if ts == current_end + interval:
                current_end = ts
            else:
                gaps.append((current_start, current_end))
                current_start = ts
                current_end = ts
        gaps.append((current_start, current_end))

        print(f"🔍 Найдено пропусков внутри данных: {len(missing)}")

    return gaps


def insert_candles_bulk(tf, candles):
    """Массовая вставка свечей в БД"""
    if not candles:
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    table = f"candles_{tf}"

    # Создаем таблицу если не существует
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
        timestamp_ms = c["timestamp"] * 1000
        timestamp_ns = c["timestamp"] * 1_000_000_000
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
    """Загрузка свечей с Bybit батчами"""
    interval = INTERVAL_MAP[tf]
    all_candles = []
    current = start_ts
    step = TIMEFRAMES_CONFIG[tf]["interval_sec"] * 200

    while current <= end_ts:
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
            print(
                f"📥 Загружено {len(parsed)} свечей @ {tf} ({current} → {min(end_ts, current + step)})"
            )
        except Exception as e:
            print(f"⚠️  Ошибка API: {e}")
        current += step
        time.sleep(0.05)

    return all_candles


def main():
    print(f"🎯 Символ: {SYMBOL}")
    print(f"💾 БД: {DB_PATH}")

    for tf in TIMEFRAMES_CONFIG:
        print(f"\n📦 Проверка и дозагрузка {tf}...")

        gaps = get_missing_gaps(tf)
        if not gaps:
            print(f"✅ Пропусков нет в {tf}")
            continue

        total_loaded = 0
        for gap_start, gap_end in gaps:
            print(f"🔄 Загрузка пропуска: {gap_start} → {gap_end}")

            # Добавляем небольшой буфер для надежности
            interval = TIMEFRAMES_CONFIG[tf]["interval_sec"]
            margin = interval * 10
            from_ts = gap_start - margin
            to_ts = gap_end + margin

            candles = fetch_candles_batch(tf, from_ts, to_ts)
            if candles:
                insert_candles_bulk(tf, candles)
                total_loaded += len(candles)

        print(f"✅ Всего загружено {total_loaded} свечей в {tf}")


if __name__ == "__main__":
    main()
