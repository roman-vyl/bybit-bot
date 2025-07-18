import argparse
import sqlite3
import pandas as pd
import pandas_ta as ta
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[2]  # backend/
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from config.timeframes_config import TIMEFRAMES_CONFIG

PROJECT_ROOT = BASE_DIR.parent
DB_PATH = PROJECT_ROOT / "db" / "market_data.sqlite"
EMA_FILE = BASE_DIR / "config" / "ema_periods.txt"

parser = argparse.ArgumentParser()
parser.add_argument(
    "--incremental", action="store_true", help="Рассчитывать только пустые EMA"
)
args = parser.parse_args()

# Загружаем периоды из файла
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
        print(f"   ⚠ Ошибка чтения таблицы: {e}")
        continue

    if len(df) < max(EMA_PERIODS):
        print("   ⚠ Недостаточно данных, пропуск")
        continue

    df.sort_values("timestamp", inplace=True)

    if args.incremental:
        ema_null_mask = pd.Series(False, index=df.index)
        for period in EMA_PERIODS:
            col = f"ema{period}"
            if col in df.columns:
                ema_null_mask |= df[col].isnull()
        df = df[ema_null_mask]
        if df.empty:
            print("   ⏭ Все EMA уже рассчитаны, пропуск")
            continue

    for period in EMA_PERIODS:
        col_name = f"ema{period}"
        if col_name in df.columns:
            df[col_name] = ta.ema(df["close"], length=period)
        else:
            print(f"   ⚠ Столбец {col_name} отсутствует, пропущен")

    update_cols = [f"ema{p}" for p in EMA_PERIODS if f"ema{p}" in df.columns]
    for _, row in df.iterrows():
        values = [row[c] for c in update_cols]
        set_clause = ", ".join([f"{col} = ?" for col in update_cols])
        cursor.execute(
            f"UPDATE {table} SET {set_clause} WHERE timestamp = ?",
            values + [row["timestamp"]],
        )

    conn.commit()
    print(f"   ✅ Обновлены EMA: {update_cols}")

conn.close()
print("\n✅ EMA расчет завершён.")
