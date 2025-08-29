"""
recalculate_ema_minus_one.py

Скрипт для пересчета только значений -1 в EMA колонках.
Находит все строки с emaX = -1 и пересчитывает их с новой валидацией.
"""

import sqlite3
import pandas as pd
from pathlib import Path
import sys

# Добавляем backend в путь
BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from config.timeframes_config import TIMEFRAMES_CONFIG
from core.validation.data_integrity import validate_for_indicator

PROJECT_ROOT = BASE_DIR.parent
DB_PATH = PROJECT_ROOT / "db" / "market_data.sqlite"

# Загружаем периоды EMA
EMA_FILE = BASE_DIR / "config" / "ema_periods.txt"
with EMA_FILE.open("r") as f:
    EMA_PERIODS = [int(line.strip()) for line in f if line.strip().isdigit()]


def find_minus_one_rows(table: str, period: int) -> pd.DataFrame:
    """Находит строки с emaX = -1"""
    col = f"ema{period}"

    with sqlite3.connect(DB_PATH) as conn:
        query = f"""
        SELECT * FROM {table} 
        WHERE {col} = -1 
        ORDER BY timestamp
        """
        df = pd.read_sql_query(query, conn)

    return df


def recalculate_ema_for_table(table: str, timeframe: str, symbol: str = "BTCUSDT"):
    """Пересчитывает EMA = -1 для конкретной таблицы"""
    print(f"\n🔄 Пересчет EMA = -1 в {table} ({timeframe})...")

    tf_sec = TIMEFRAMES_CONFIG[timeframe]["interval_sec"]
    total_recalculated = 0

    for period in EMA_PERIODS:
        col = f"ema{period}"

        # Проверяем, есть ли колонка
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [row[1] for row in cursor.fetchall()]

            if col not in columns:
                print(f"   ⏭ Колонка {col} не существует")
                continue

        # Находим строки с -1
        df_minus_one = find_minus_one_rows(table, period)

        if df_minus_one.empty:
            print(f"   ✅ {col}: нет значений -1")
            continue

        print(f"   🔍 {col}: найдено {len(df_minus_one)} значений -1")

        # Для каждой строки с -1 берем окно данных и пересчитываем
        recalculated_count = 0

        with sqlite3.connect(DB_PATH) as conn:
            for idx, row in df_minus_one.iterrows():
                timestamp = row["timestamp"]

                # Берем окно данных для расчета EMA
                window_query = f"""
                SELECT * FROM {table} 
                WHERE symbol = ? 
                AND timestamp <= ? 
                ORDER BY timestamp DESC 
                LIMIT {period * 2}  -- Берем больше данных для надежности
                """

                window_df = pd.read_sql_query(
                    window_query, conn, params=(symbol, timestamp)
                )

                if len(window_df) < period:
                    print(
                        f"      ⚠ {col} {timestamp}: недостаточно данных ({len(window_df)} < {period})"
                    )
                    continue

                # Сортируем по возрастанию timestamp
                window_df = window_df.sort_values("timestamp")

                # Валидируем данные
                is_valid, reason = validate_for_indicator(window_df, period, tf_sec)

                if not is_valid:
                    print(f"      ⚠ {col} {timestamp}: {reason}")
                    continue

                # Рассчитываем EMA
                import pandas_ta as ta

                ema_series = ta.ema(window_df["close"], length=period)

                if pd.isna(ema_series.iloc[-1]):
                    print(f"      ⚠ {col} {timestamp}: EMA не рассчиталась")
                    continue

                # Обновляем значение в БД
                cursor = conn.cursor()
                cursor.execute(
                    f"UPDATE {table} SET {col} = ? WHERE symbol = ? AND timestamp = ?",
                    (float(ema_series.iloc[-1]), symbol, timestamp),
                )

                recalculated_count += 1
                print(f"      ✅ {col} {timestamp}: {ema_series.iloc[-1]:.2f}")

            conn.commit()

        total_recalculated += recalculated_count
        print(
            f"   📊 {col}: пересчитано {recalculated_count}/{len(df_minus_one)} значений"
        )

    print(f"   🎯 Итого пересчитано: {total_recalculated} значений")
    return total_recalculated


def main():
    """Главная функция"""
    print("🚀 Пересчет EMA = -1 с новой валидацией")
    print("=" * 50)

    total_recalculated = 0

    for timeframe in TIMEFRAMES_CONFIG:
        table = f"candles_{timeframe}"

        # Проверяем, существует ли таблица
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
                (table,),
            )
            if not cursor.fetchone():
                print(f"⏭ Таблица {table} не существует")
                continue

        recalculated = recalculate_ema_for_table(table, timeframe)
        total_recalculated += recalculated

    print("\n" + "=" * 50)
    print(f"🏁 Пересчет завершен. Всего обновлено: {total_recalculated} значений")


if __name__ == "__main__":
    main()
