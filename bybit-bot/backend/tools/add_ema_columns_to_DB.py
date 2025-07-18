"""
add_ema_columns.py

Обновлённый скрипт:
- Добавляет недостающие столбцы emaXX в таблицы вида candles_<tf>
- Удаляет столбцы emaXX, которых нет в ema_periods.txt
- Переименовывает столбцы типа EMAxx -> emaxx в БД (регистр)

Файл конфигурации: config/ema_periods.txt
"""

import sqlite3
from pathlib import Path

# --- Пути ---
BASE_DIR = Path(__file__).resolve().parents[1]  # backend/
PROJECT_ROOT = BASE_DIR.parent  # bybit-bot/
DB_PATH = PROJECT_ROOT / "db" / "market_data.sqlite"
EMA_FILE = PROJECT_ROOT / "backend" / "config" / "ema_periods.txt"

# --- Загрузка периодов EMA ---
if not EMA_FILE.exists():
    raise FileNotFoundError("Файл config/ema_periods.txt не найден")

with EMA_FILE.open("r") as f:
    ema_periods = [line.strip() for line in f if line.strip().isdigit()]

if not ema_periods:
    raise ValueError("Список EMA-периодов пуст")

desired_ema_cols = {f"ema{p}" for p in ema_periods}

# --- Обработка таблиц ---
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'candles_%'"
)
tables = [row[0] for row in cursor.fetchall()]

for table in tables:
    print(f"\n🔄 Обработка {table}...")
    cursor.execute(f"PRAGMA table_info({table})")
    raw_columns = [row[1] for row in cursor.fetchall()]
    columns = [c.lower() for c in raw_columns]  # сравнение в нижнем регистре

    base_columns = ["symbol", "timestamp", "open", "high", "low", "close", "volume"]
    current_ema_cols = [c for c in columns if c.startswith("ema")]

    # --- Ренейм: заменим EMAxx → emaxx ---
    rename_map = {
        old: old.lower()
        for old in raw_columns
        if old.startswith("EMA") and old.lower() in desired_ema_cols
    }
    if rename_map:
        print(f"   🔁 Переименование: {rename_map}")
        for old_col, new_col in rename_map.items():
            cursor.execute(f"ALTER TABLE {table} RENAME COLUMN {old_col} TO {new_col}")
        conn.commit()

    # --- Добавляем недостающие ---
    for col in sorted(desired_ema_cols):
        if col not in columns:
            try:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} REAL")
                print(f"   ➕ Добавлен столбец {col}")
            except Exception as e:
                print(f"   ⚠ Ошибка при добавлении {col}: {e}")
        else:
            print(f"   ✅ {col} уже существует")

    # --- Удаляем лишние EMA ---
    cols_to_remove = set(current_ema_cols) - desired_ema_cols
    if cols_to_remove:
        print(f"   🧹 Удаляем столбцы: {sorted(cols_to_remove)}")

        keep_cols = base_columns + sorted(desired_ema_cols)
        keep_cols_str = ", ".join(keep_cols)

        cursor.execute(
            f"CREATE TABLE temp_{table} AS SELECT {keep_cols_str} FROM {table}"
        )
        cursor.execute(f"DROP TABLE {table}")
        cursor.execute(f"ALTER TABLE temp_{table} RENAME TO {table}")
        conn.commit()

        print(f"   ✅ Лишние столбцы удалены")
    else:
        print(f"   🧼 Нечего удалять")

conn.close()
print("\n🏁 Обновление структуры БД завершено.")
