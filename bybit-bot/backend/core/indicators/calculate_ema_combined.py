import os
import sqlite3
import pandas as pd
import pandas_ta as ta
from pathlib import Path
from typing import List

from backend.config.timeframes_config import TIMEFRAMES_CONFIG


PROJECT_ROOT = Path(__file__).resolve().parents[3]
EMA_FILE = PROJECT_ROOT / "backend" / "config" / "ema_periods.txt"
DB_PATH = PROJECT_ROOT / "db" / "market_data.sqlite"


# Загружаем периоды из файла
with EMA_FILE.open("r") as f:
    EMA_PERIODS = [int(line.strip()) for line in f if line.strip().isdigit()]
if not EMA_PERIODS:
    raise ValueError("Список EMA-периодов пуст")


def calculate_ema(df: pd.DataFrame, periods: List[int]) -> pd.DataFrame:
    for period in periods:
        col = f"ema{period}"
        if col in df.columns:
            df[col] = ta.ema(df["close"], length=period)
    return df


def run_ema_incremental(symbol: str, timeframe: str):
    table = f"candles_{timeframe}"

    with sqlite3.connect(str(DB_PATH)) as conn:
        df = pd.read_sql_query(
            f"SELECT * FROM {table} WHERE symbol = ?", conn, params=(symbol,)
        )

        if len(df) < max(EMA_PERIODS):
            print(f"   ⚠ Недостаточно данных для {symbol} {timeframe}, пропуск")
            return

        df.sort_values("timestamp", inplace=True)

        # фильтруем строки без EMA
        ema_null_mask = pd.Series(False, index=df.index)
        for period in EMA_PERIODS:
            col = f"ema{period}"
            if col in df.columns:
                ema_null_mask |= df[col].isnull()
        df = df[ema_null_mask]
        if df.empty:
            print(f"   ⏭ EMA уже рассчитаны: {symbol} {timeframe}")
            return

        df = calculate_ema(df, EMA_PERIODS)

        update_cols = [f"ema{p}" for p in EMA_PERIODS if f"ema{p}" in df.columns]
        with conn:
            for _, row in df.iterrows():
                values = [row[c] for c in update_cols]
                set_clause = ", ".join([f"{col} = ?" for col in update_cols])
                conn.execute(
                    f"UPDATE {table} SET {set_clause} WHERE symbol = ? AND timestamp = ?",
                    values + [symbol, row["timestamp"]],
                )

        print(f"   ✅ EMA обновлены: {symbol} {timeframe}")


if __name__ == "__main__":
    with sqlite3.connect(str(DB_PATH)) as conn:
        for tf in TIMEFRAMES_CONFIG:
            table = f"candles_{tf}"
            print(f"\n⏳ Обработка {table}...")
            run_ema_incremental(symbol="BTCUSDT", timeframe=tf)

    print("\n✅ EMA расчет завершён.")
