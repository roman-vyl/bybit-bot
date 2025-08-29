"""
Расчёт EMA с валидацией данных

Улучшенная версия расчёта EMA, которая использует модуль валидации
для проверки качества данных перед расчётом индикаторов.
"""

import os
import sqlite3
import pandas as pd
import pandas_ta as ta
from pathlib import Path
from typing import List

from backend.config.timeframes_config import TIMEFRAMES_CONFIG
from backend.core.validation.data_integrity import validate_for_indicator


PROJECT_ROOT = Path(__file__).resolve().parents[3]
EMA_FILE = PROJECT_ROOT / "backend" / "config" / "ema_periods.txt"
DB_PATH = PROJECT_ROOT / "db" / "market_data.sqlite"


# Загружаем периоды из файла
with EMA_FILE.open("r") as f:
    EMA_PERIODS = [int(line.strip()) for line in f if line.strip().isdigit()]
if not EMA_PERIODS:
    raise ValueError("Список EMA-периодов пуст")


def calculate_ema(df: pd.DataFrame, periods: List[int], tf_sec: int) -> pd.DataFrame:
    """Расчёт EMA для указанных периодов с улучшенной валидацией"""
    for period in periods:
        col = f"ema{period}"
        if col not in df.columns:
            continue

        df_null = df[df[col].isnull()].copy()
        if df_null.empty:
            continue

        # Улучшенная валидация с проверкой непрерывности
        is_valid, reason = validate_for_indicator(df_null, period, tf_sec)
        if not is_valid:
            print(f"⚠ EMA{period}: {reason} — пишем -1")
            df.loc[df_null.index, col] = -1
            continue

        # Рассчитываем EMA только для валидных данных
        ema_series = ta.ema(df_null["close"], length=period)
        if ema_series.dropna().shape[0] < period:
            print(f"⚠ EMA{period}: нестабильная EMA — пишем -1")
            df.loc[df_null.index, col] = -1
        else:
            df.loc[df_null.index, col] = ema_series
    return df


def validate_data_before_ema(df: pd.DataFrame, timeframe: str) -> bool:
    """
    Валидация данных перед расчётом EMA

    Args:
        df: DataFrame с данными свечей
        timeframe: Таймфрейм

    Returns:
        True если данные валидны, False иначе
    """
    if df.empty:
        print(f"   ❌ DataFrame пуст")
        return False

    # Проверяем наличие необходимых колонок
    required_columns = ["timestamp", "open", "high", "low", "close", "volume"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"   ❌ Отсутствуют колонки: {missing_columns}")
        return False

    # Проверяем достаточность данных для EMA
    max_period = max(EMA_PERIODS)
    if len(df) < max_period:
        print(f"   ❌ Недостаточно данных: {len(df)} < {max_period}")
        return False

    print(f"   ✅ Данные прошли валидацию ({len(df)} точек)")
    return True


def run_ema_incremental(symbol: str, timeframe: str):
    """
    Расчёт EMA с валидацией данных

    Args:
        symbol: Торговый символ
        timeframe: Таймфрейм
    """
    table = f"candles_{timeframe}"

    with sqlite3.connect(str(DB_PATH)) as conn:
        # Загружаем данные
        df = pd.read_sql_query(
            f"SELECT * FROM {table} WHERE symbol = ?", conn, params=(symbol,)
        )

        if df.empty:
            print(f"   ⚠ Нет данных для {symbol} {timeframe}")
            return

        # Сортируем по timestamp
        df.sort_values("timestamp", inplace=True)

        # Валидируем данные
        if not validate_data_before_ema(df, timeframe):
            print(f"   ⏭ Пропуск {symbol} {timeframe} из-за проблем с данными")
            return

        # Фильтруем строки без EMA
        ema_null_mask = pd.Series(False, index=df.index)
        for period in EMA_PERIODS:
            col = f"ema{period}"
            if col in df.columns:
                ema_null_mask |= df[col].isnull()

        df_to_update = df[ema_null_mask]

        if df_to_update.empty:
            print(f"   ⏭ EMA уже рассчитаны: {symbol} {timeframe}")
            return

        # Расчёт EMA с улучшенной валидацией
        tf_sec = TIMEFRAMES_CONFIG[timeframe]["interval_sec"]
        df_to_update = calculate_ema(df_to_update, EMA_PERIODS, tf_sec)

        # Обновление в БД
        update_cols = [
            f"ema{p}" for p in EMA_PERIODS if f"ema{p}" in df_to_update.columns
        ]

        if update_cols:
            with conn:
                for _, row in df_to_update.iterrows():
                    values = [row[c] for c in update_cols]
                    set_clause = ", ".join([f"{col} = ?" for col in update_cols])
                    conn.execute(
                        f"UPDATE {table} SET {set_clause} WHERE symbol = ? AND timestamp = ?",
                        values + [symbol, row["timestamp"]],
                    )

            print(
                f"   ✅ EMA обновлены: {symbol} {timeframe} ({len(df_to_update)} строк)"
            )


def main():
    """Главная функция"""
    import argparse

    parser = argparse.ArgumentParser(description="Расчёт EMA с валидацией данных")
    parser.add_argument("--symbol", default="BTCUSDT", help="Символ для обработки")
    parser.add_argument("--timeframe", help="Конкретный таймфрейм")

    args = parser.parse_args()

    if args.timeframe:
        timeframes = [args.timeframe]
    else:
        timeframes = list(TIMEFRAMES_CONFIG.keys())

    print(f"🚀 Запуск расчёта EMA с валидацией для {args.symbol}")
    print("=" * 60)

    for tf in timeframes:
        table = f"candles_{tf}"
        print(f"\n⏳ Обработка {table}...")
        run_ema_incremental(symbol=args.symbol, timeframe=tf)

    print("\n✅ Расчёт EMA завершён.")


if __name__ == "__main__":
    main()
