"""
Скрипт валидации данных свечей

Использует модуль data_integrity для комплексной проверки качества данных
в таблицах свечей всех таймфреймов.
"""

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))
PROJECT_ROOT = BASE_DIR.parent  # bybit-bot/
DB_PATH = PROJECT_ROOT / "db" / "market_data.sqlite"

import sqlite3
import pandas as pd
from backend.config.timeframes_config import TIMEFRAMES_CONFIG
from backend.core.validation import CandleDataValidator, validate_candle_dataframe


def validate_table_data(
    table_name: str, timeframe: str, symbol: str = "BTCUSDT"
) -> dict:
    """
    Валидация данных в конкретной таблице

    Args:
        table_name: Имя таблицы
        timeframe: Таймфрейм
        symbol: Символ для проверки

    Returns:
        Результаты валидации
    """
    print(f"\n🔍 Валидация {table_name} ({timeframe}) для {symbol}...")

    try:
        with sqlite3.connect(DB_PATH) as conn:
            # Загружаем данные
            query = f"SELECT * FROM {table_name} WHERE symbol = ? ORDER BY timestamp DESC LIMIT 2000"
            df = pd.read_sql_query(query, conn, params=(symbol,))

        if df.empty:
            print(f"   ⚠ Таблица пуста или нет данных для {symbol}")
            return {}

        # Создаём валидатор
        validator = CandleDataValidator(timeframe)

        # Выполняем комплексную валидацию
        results = validator.comprehensive_validation(df)

        # Выводим резюме
        summary = validator.get_validation_summary(results)
        print(summary)

        # Дополнительная информация
        print(f"   📅 Диапазон: {df['timestamp'].min()} - {df['timestamp'].max()}")
        print(f"   ⏱️  Интервал: {TIMEFRAMES_CONFIG[timeframe]['interval_sec']} сек")

        return results

    except Exception as e:
        print(f"   ❌ Ошибка при валидации {table_name}: {e}")
        return {}


def validate_all_timeframes(symbol: str = "BTCUSDT"):
    """
    Валидация данных по всем таймфреймам

    Args:
        symbol: Символ для проверки
    """
    print(f"🚀 Запуск валидации данных для {symbol}")
    print("=" * 60)

    all_results = {}

    for timeframe in TIMEFRAMES_CONFIG:
        table_name = f"candles_{timeframe}"
        results = validate_table_data(table_name, timeframe, symbol)
        if results:
            all_results[timeframe] = results

    # Общий отчёт
    print("\n" + "=" * 60)
    print("📊 ОБЩИЙ ОТЧЁТ ВАЛИДАЦИИ")
    print("=" * 60)

    total_tables = len(all_results)
    valid_tables = 0

    for timeframe, results in all_results.items():
        is_valid = (
            results["timestamp_continuity"].is_valid
            and results["ohlc_validity"].is_valid
            and results["data_sufficiency"].is_valid
        )

        status = "✅" if is_valid else "❌"
        print(f"{status} {timeframe}: {status}")

        if is_valid:
            valid_tables += 1

    print(f"\n📈 Результат: {valid_tables}/{total_tables} таблиц валидны")

    if valid_tables == total_tables:
        print("🎉 Все данные прошли валидацию!")
    else:
        print("⚠️  Обнаружены проблемы в данных")


def validate_specific_timeframe(timeframe: str, symbol: str = "BTCUSDT"):
    """
    Валидация конкретного таймфрейма

    Args:
        timeframe: Таймфрейм для проверки
        symbol: Символ для проверки
    """
    if timeframe not in TIMEFRAMES_CONFIG:
        print(f"❌ Неизвестный таймфрейм: {timeframe}")
        print(f"Доступные: {list(TIMEFRAMES_CONFIG.keys())}")
        return

    table_name = f"candles_{timeframe}"
    validate_table_data(table_name, timeframe, symbol)


def main():
    """Главная функция"""
    import argparse

    parser = argparse.ArgumentParser(description="Валидация данных свечей")
    parser.add_argument("--symbol", default="BTCUSDT", help="Символ для проверки")
    parser.add_argument("--timeframe", help="Конкретный таймфрейм для проверки")

    args = parser.parse_args()

    if args.timeframe:
        validate_specific_timeframe(args.timeframe, args.symbol)
    else:
        validate_all_timeframes(args.symbol)


if __name__ == "__main__":
    main()
