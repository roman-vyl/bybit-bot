# sync_timeframes_and_emas.py
# ----------------------------------------------
# 📌 Назначение:
# Скрипт синхронизирует структуру SQLite-базы `market_data.sqlite`:
# 1. Создаёт недостающие таблицы `candles_<tf>` (таймфреймы из config/timeframes_config.py)
# 2. Добавляет недостающие столбцы `ema_<period>` в таблицы (из config/ema_periods.txt)
# 3. Удаляет лишние ema-столбцы, которых нет в актуальном списке
# 4. Добавляет поле timestamp_ns в таблицы candles_*
#
# 🧩 Используемые файлы:
# - db/market_data.sqlite (основная база данных)
# - config/timeframes_config.py (таймфреймы)
# - config/ema_periods.txt (список EMA периодов)
#
# ✅ Использование:
# python tools/sync_timeframes_and_emas.py
# ----------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
import sqlite3
from config.timeframes_config import TIMEFRAMES_CONFIG

BASE_DIR = Path(__file__).resolve().parents[1]  # backend/
PROJECT_ROOT = BASE_DIR.parent  # bybit-bot/
DB_PATH = PROJECT_ROOT / "db" / "market_data.sqlite"
EMA_PATH = PROJECT_ROOT / "backend" / "config" / "ema_periods.txt"


def load_ema_periods():
    with open(EMA_PATH, "r") as f:
        return sorted(set(line.strip() for line in f if line.strip().isdigit()))


def ensure_table_exists(cursor, tf):
    table_name = f"candles_{tf}"
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            symbol TEXT NOT NULL,
            timestamp INTEGER PRIMARY KEY,
            timestamp_ns INTEGER NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL
        )
    """
    )


def add_timestamp_ns_column(cursor, tf):
    """Добавляет поле timestamp_ns в таблицу, если его нет"""
    table = f"candles_{tf}"
    cursor.execute(f"PRAGMA table_info({table})")
    existing_columns = {row[1] for row in cursor.fetchall()}

    if "timestamp_ns" not in existing_columns:
        try:
            cursor.execute(
                f"ALTER TABLE {table} ADD COLUMN timestamp_ns INTEGER NOT NULL DEFAULT 0"
            )
            print(f"  [+] Добавлено поле timestamp_ns в {table}")
        except sqlite3.OperationalError as e:
            print(f"  [!] Ошибка при добавлении timestamp_ns в {table}: {e}")


def sync_ema_columns(cursor, tf, ema_periods):
    table = f"candles_{tf}"
    cursor.execute(f"PRAGMA table_info({table})")
    existing_columns = {row[1] for row in cursor.fetchall()}

    for period in ema_periods:
        col = f"ema{period}"
        if col not in existing_columns:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} REAL")
            print(f"  [+] Добавлен столбец {col} в {table}")

    allowed_ema_cols = {f"ema{p}" for p in ema_periods}
    extra_cols = {
        col
        for col in existing_columns
        if col.startswith("ema") and col not in allowed_ema_cols
    }
    if extra_cols:
        print(f"  [~] В таблице {table} будут удалены колонки: {', '.join(extra_cols)}")
        _drop_columns(cursor, table, extra_cols)


def _drop_columns(cursor, table, drop_cols):
    cursor.execute(f"PRAGMA table_info({table})")
    cols_info = cursor.fetchall()
    cols_to_keep = [col[1] for col in cols_info if col[1] not in drop_cols]

    cols_def = ", ".join(
        f"{col[1]} {col[2]}" for col in cols_info if col[1] in cols_to_keep
    )
    cols_str = ", ".join(cols_to_keep)

    tmp_table = f"{table}_tmp"

    cursor.execute(f"CREATE TABLE {tmp_table} ({cols_def})")
    cursor.execute(
        f"INSERT INTO {tmp_table} ({cols_str}) SELECT {cols_str} FROM {table}"
    )
    cursor.execute(f"DROP TABLE {table}")
    cursor.execute(f"ALTER TABLE {tmp_table} RENAME TO {table}")
    print(f"  [-] Удалены лишние колонки: {', '.join(drop_cols)}")


def main():
    ema_periods = load_ema_periods()
    print(f"[✓] Загружено EMA-периодов: {ema_periods}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for tf in TIMEFRAMES_CONFIG:
        print(f"\n[>] Обработка таймфрейма: {tf}")
        ensure_table_exists(cursor, tf)
        add_timestamp_ns_column(cursor, tf)
        sync_ema_columns(cursor, tf, ema_periods)

    conn.commit()
    conn.close()
    print("\n[✓] Синхронизация таблиц и EMA-колонок завершена.")


if __name__ == "__main__":
    main()
