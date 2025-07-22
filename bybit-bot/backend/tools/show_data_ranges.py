"""
show_data_ranges.py

Показывает точные диапазоны хранящихся данных в каждой таблице свечей:
- Количество записей
- Самая ранняя и поздняя свеча (timestamp + дата)
- Пропуски в данных (если есть)
- Покрытие данных в процентах
"""

import sys
import sqlite3
from pathlib import Path
from datetime import datetime
import pandas as pd

# Настройка путей
BASE_DIR = Path(__file__).resolve().parents[1]  # backend/
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

PROJECT_ROOT = BASE_DIR.parent  # bybit-bot/
DB_PATH = PROJECT_ROOT / "db" / "market_data.sqlite"

from config.timeframes_config import TIMEFRAMES_CONFIG


def format_timestamp(ts):
    """Конвертирует Unix timestamp в читаемый формат"""
    if ts is None:
        return "N/A"
    return f"{ts} ({datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')})"


def analyze_table(table_name, tf_config):
    """Анализирует конкретную таблицу свечей"""
    conn = sqlite3.connect(DB_PATH)

    try:
        # Базовая статистика
        query_stats = f"""
        SELECT 
            COUNT(*) as total_records,
            MIN(timestamp) as earliest,
            MAX(timestamp) as latest,
            symbol
        FROM {table_name}
        GROUP BY symbol
        """

        df_stats = pd.read_sql_query(query_stats, conn)

        if df_stats.empty:
            return {"table": table_name, "status": "empty", "records": 0, "symbols": []}

        results = []

        for _, row in df_stats.iterrows():
            symbol = row["symbol"]
            total = row["total_records"]
            earliest = row["earliest"]
            latest = row["latest"]

            # Анализ пропусков
            interval_sec = tf_config["interval_sec"]
            expected_records = (
                int((latest - earliest) / interval_sec) + 1
                if latest and earliest
                else 0
            )
            coverage = (total / expected_records * 100) if expected_records > 0 else 0
            missing = expected_records - total

            # Поиск пропусков в данных
            gaps_query = f"""
            SELECT timestamp FROM {table_name} 
            WHERE symbol = '{symbol}' 
            ORDER BY timestamp
            """
            timestamps_df = pd.read_sql_query(gaps_query, conn)
            gaps = []

            if len(timestamps_df) > 1:
                timestamps = timestamps_df["timestamp"].tolist()
                for i in range(1, len(timestamps)):
                    expected_next = timestamps[i - 1] + interval_sec
                    if timestamps[i] > expected_next:
                        gap_start = expected_next
                        gap_end = timestamps[i] - interval_sec
                        gap_duration = (gap_end - gap_start) // interval_sec + 1
                        gaps.append(
                            {
                                "start": gap_start,
                                "end": gap_end,
                                "duration_candles": gap_duration,
                                "start_date": datetime.fromtimestamp(
                                    gap_start
                                ).strftime("%Y-%m-%d %H:%M"),
                                "end_date": datetime.fromtimestamp(gap_end).strftime(
                                    "%Y-%m-%d %H:%M"
                                ),
                            }
                        )

            results.append(
                {
                    "symbol": symbol,
                    "total_records": total,
                    "earliest": earliest,
                    "latest": latest,
                    "expected_records": expected_records,
                    "missing": missing,
                    "coverage": coverage,
                    "gaps": gaps,
                }
            )

        return {"table": table_name, "status": "has_data", "results": results}

    except sqlite3.OperationalError as e:
        return {"table": table_name, "status": "error", "error": str(e)}
    finally:
        conn.close()


def main():
    print("=" * 80)
    print("📊 АНАЛИЗ ДИАПАЗОНОВ ДАННЫХ В БД СВЕЧЕЙ")
    print("=" * 80)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Получаем список всех таблиц свечей
    cursor.execute(
        """
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name LIKE 'candles_%'
        ORDER BY name
    """
    )
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()

    if not tables:
        print("⚠️ Таблицы со свечами не найдены!")
        return

    print(f"📋 Найдено таблиц: {len(tables)}\n")

    for table in tables:
        # Извлекаем таймфрейм из имени таблицы
        tf = table.replace("candles_", "")
        tf_config = TIMEFRAMES_CONFIG.get(
            tf, {"interval_sec": 60}
        )  # fallback для неизвестных TF

        print(f"🔍 {table.upper()}")
        print("-" * 60)

        analysis = analyze_table(table, tf_config)

        if analysis["status"] == "empty":
            print("   📭 Таблица пуста\n")
            continue
        elif analysis["status"] == "error":
            print(f"   ❌ Ошибка: {analysis['error']}\n")
            continue

        for result in analysis["results"]:
            symbol = result["symbol"]
            total = result["total_records"]
            earliest = result["earliest"]
            latest = result["latest"]
            coverage = result["coverage"]
            missing = result["missing"]
            gaps = result["gaps"]

            print(f"   📈 Символ: {symbol}")
            print(f"   📊 Всего записей: {total:,}")
            print(f"   📅 Диапазон данных:")
            print(f"      🟢 Начало: {format_timestamp(earliest)}")
            print(f"      🔴 Конец:  {format_timestamp(latest)}")

            if earliest and latest:
                duration_days = (latest - earliest) / (24 * 3600)
                print(f"      ⏱️  Период: {duration_days:.1f} дней")

            print(f"   📈 Покрытие: {coverage:.1f}% ({missing:,} пропущенных свечей)")

            if gaps:
                print(f"   🕳️  Найдено пропусков: {len(gaps)}")
                for i, gap in enumerate(
                    gaps[:5]
                ):  # Показываем только первые 5 пропусков
                    print(
                        f"      {i+1}. {gap['start_date']} - {gap['end_date']} ({gap['duration_candles']} свечей)"
                    )
                if len(gaps) > 5:
                    print(f"      ... и ещё {len(gaps) - 5} пропусков")
            else:
                print("   ✅ Пропусков в данных не найдено")

        print()

    print("=" * 80)
    print("🏁 Анализ завершён")


if __name__ == "__main__":
    main()
