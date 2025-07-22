#!/usr/bin/env python3
"""
show_ranges_brief.py

Краткий просмотр диапазонов данных в таблицах свечей
Показывает только основную информацию без детального анализа пропусков
"""

import sys
import sqlite3
from pathlib import Path
from datetime import datetime

# Настройка путей
BASE_DIR = Path(__file__).resolve().parents[1]  # backend/
PROJECT_ROOT = BASE_DIR.parent  # bybit-bot/
DB_PATH = PROJECT_ROOT / "db" / "market_data.sqlite"


def main():
    print("📊 КРАТКИЙ ОБЗОР ДАННЫХ В БД")
    print("=" * 50)

    conn = sqlite3.connect(DB_PATH)

    # Получаем список всех таблиц свечей
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name LIKE 'candles_%'
        ORDER BY name
    """
    )
    tables = [row[0] for row in cursor.fetchall()]

    if not tables:
        print("⚠️ Таблицы со свечами не найдены!")
        conn.close()
        return

    print(f"Найдено таблиц: {len(tables)}\n")

    # Заголовок таблицы
    print(
        f"{'Таймфрейм':<12} {'Записей':<10} {'Начало данных':<20} {'Конец данных':<20} {'Дней':<8}"
    )
    print("-" * 80)

    for table in tables:
        tf = table.replace("candles_", "")

        try:
            cursor.execute(
                f"""
                SELECT 
                    COUNT(*) as total,
                    MIN(timestamp) as earliest,
                    MAX(timestamp) as latest
                FROM {table}
                WHERE timestamp > 0
            """
            )

            result = cursor.fetchone()
            total, earliest, latest = result

            if total == 0:
                print(f"{tf:<12} {'0':<10} {'(пусто)':<20} {'(пусто)':<20} {'0':<8}")
                continue

            start_date = datetime.fromtimestamp(earliest).strftime("%Y-%m-%d %H:%M")
            end_date = datetime.fromtimestamp(latest).strftime("%Y-%m-%d %H:%M")
            days = round((latest - earliest) / 86400, 1)

            print(f"{tf:<12} {total:<10,} {start_date:<20} {end_date:<20} {days:<8}")

        except sqlite3.OperationalError as e:
            print(f"{tf:<12} {'ERROR':<10} {str(e)[:40]:<40}")

    conn.close()
    print("\n✅ Готово")


if __name__ == "__main__":
    main()
