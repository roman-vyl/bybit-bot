"""
calculate_ema.py

Рассчитывает EMA по всем таймфреймам в базе данных,
используя периоды, указанные в файле config/ema_periods.txt

Формат: ema8, ema13, ..., ema233 (нижний регистр)
Обновляет только значения EMA, без изменения схемы таблиц
"""

import sys
from pathlib import Path

import sqlite3
import pandas as pd
import pandas_ta as ta
import subprocess
import os


BASE_DIR = Path(__file__).resolve().parents[2]  # backend/
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

PROJECT_ROOT = BASE_DIR.parent  # bybit-bot/
DB_PATH = PROJECT_ROOT / "db" / "market_data.sqlite"
EMA_FILE = BASE_DIR / "config" / "ema_periods.txt"

from config.timeframes_config import TIMEFRAMES_CONFIG

# --- Проверка пропущенных свечей ---
check_script = BASE_DIR / "tools" / "check_missing_candles.py"
print(f"\n🛠 Проверка пропущенных свечей: {check_script}")
subprocess.run(
    ["python", str(check_script)], env={**os.environ, "PYTHONPATH": str(BASE_DIR)}
)

# --- Загрузка периодов ---
if not EMA_FILE.exists():
    raise FileNotFoundError("Файл config/ema_periods.txt не найден")

with EMA_FILE.open("r") as f:
    EMA_PERIODS = [int(line.strip()) for line in f if line.strip().isdigit()]

if not EMA_PERIODS:
    raise ValueError("Список EMA-периодов пуст")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

for tf in TIMEFRAMES_CONFIG:
    table = f"candles_{tf}"
    print(f"\n⏳ Обработка {table}...")
    try:
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    except Exception as e:
        print(f"   ⚠ Ошибка чтения таблицы {table}: {e}")
        continue

    if len(df) < max(EMA_PERIODS):
        print("   ⚠ Недостаточно данных, пропуск")
        continue

    df.sort_values("timestamp", inplace=True)

    for period in EMA_PERIODS:
        col_name = f"ema{period}"
        if col_name in df.columns:
            df[col_name] = ta.ema(df["close"], length=period)
        else:
            print(f"   ⚠ Столбец {col_name} отсутствует в {table}, пропущен")

    # Обновляем только EMA-столбцы по одной строке
    update_cols = [f"ema{p}" for p in EMA_PERIODS if f"ema{p}" in df.columns]
    for _, row in df.iterrows():
        values = [row[c] for c in update_cols]
        set_clause = ", ".join([f"{col} = ?" for col in update_cols])
        cursor.execute(
            f"UPDATE {table} SET {set_clause} WHERE timestamp = ?",
            values + [row["timestamp"]],
        )

    conn.commit()
    print(f"   ✅ Обновлены значения EMA: {update_cols}")

conn.close()
print("\n✅ Все значения EMA обновлены (структура БД не изменена).")
