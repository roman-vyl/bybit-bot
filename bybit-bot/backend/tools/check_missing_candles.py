import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))
PROJECT_ROOT = BASE_DIR.parent  # bybit-bot/
DB_PATH = PROJECT_ROOT / "db" / "market_data.sqlite"

import sqlite3
import pandas as pd
import os
from config.timeframes_config import TIMEFRAMES_CONFIG


def analyze_missing_timestamps(df, tf):
    Path("logs").mkdir(parents=True, exist_ok=True)
    interval = TIMEFRAMES_CONFIG[tf]["interval_sec"]
    df = df.sort_values("timestamp")
    df["diff"] = df["timestamp"].diff()
    gaps = df[df["diff"] > interval]
    if not gaps.empty:
        earliest_missing = int(gaps.iloc[0]["timestamp"] - interval)
        print(f"   🔎 earliest missing = {earliest_missing}")
        with open("logs/earliest_missing_ts.txt", "w") as f:
            f.write(str(earliest_missing))
    else:
        print("   ✅ Пропусков нет")


def check_table(table):
    tf = table.replace("candles_", "")
    conn = sqlite3.connect(DB_PATH)
    query = f"SELECT DISTINCT timestamp FROM {table} ORDER BY timestamp DESC LIMIT 1500"
    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        print(f"   ⚠ Таблица пуста: {table}")
        return

    print(f"🔍 Проверка {table}.")
    analyze_missing_timestamps(df, tf)


def main():
    for tf in TIMEFRAMES_CONFIG:
        check_table(f"candles_{tf}")


if __name__ == "__main__":
    main()
