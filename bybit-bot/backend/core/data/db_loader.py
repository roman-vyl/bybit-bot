import sqlite3
import pandas as pd
from typing import List, Dict, Optional, Any
from backend.config.timeframes_config import TIMEFRAMES_CONFIG
from pathlib import Path
import os
import sys
from dotenv import load_dotenv

# Абсолютный путь к backend/
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=BASE_DIR / ".env")

# Путь до корня проекта
PROJECT_ROOT = BASE_DIR.parent

# Чтение пути из переменной окружения, с fallback
DB_PATH = Path(
    os.getenv("DB_PATH", PROJECT_ROOT / "db" / "market_data.sqlite")
).resolve()

if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))


def extract_ema_from_candles(
    candles: List[dict], periods: List[str]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Извлекает EMA данные из загруженных свечей для указанных периодов.
    Фильтрует значения -1 (маркеры проблемных данных).

    Args:
        candles: Список свечей с EMA колонками
        periods: Список периодов EMA (например, ['20', '50', '200'])

    Returns:
        Словарь {period: [{'time': timestamp, 'value': ema_value}, ...]}
    """
    if not candles:
        return {}

    result = {}

    for period in periods:
        ema_key = f"ema{period}"
        if ema_key in candles[0]:
            ema_points = []
            for candle in candles:
                ema_value = candle.get(ema_key)
                # Фильтруем None, -1 и отрицательные значения
                if ema_value is not None and ema_value >= 0:
                    ema_points.append(
                        {"time": candle["timestamp"], "value": float(ema_value)}
                    )
            result[period] = ema_points

    return result


def get_candles_from_db(
    symbol: str, timeframe: str, start: int, end: int
) -> List[dict]:
    """Загружает свечи из SQLite по символу, таймфрейму и диапазону времени."""
    candles_data, _ = get_candles_with_ema_from_db(
        symbol, timeframe, start, end, include_ema=False
    )
    return candles_data


def get_candles_with_ema_from_db(
    symbol: str,
    timeframe: str,
    start: int,
    end: int,
    include_ema: bool = False,
    ema_periods: Optional[List[str]] = None,
) -> tuple[List[dict], Dict[str, List[Dict[str, Any]]]]:
    """
    Загружает свечи из SQLite по символу, таймфрейму и диапазону времени.

    Returns:
        Tuple (candles_data, ema_data) где:
        - candles_data: OHLCV данные свечей
        - ema_data: EMA данные по периодам (если include_ema=True)
    """

    # Валидация timeframe
    if timeframe not in TIMEFRAMES_CONFIG:
        raise ValueError(
            f"Недопустимый timeframe: {timeframe}. Допустимые значения: {list(TIMEFRAMES_CONFIG.keys())}"
        )

    # Динамическое имя таблицы
    table = f"candles_{timeframe}"

    # Безопасный SQL-запрос с параметрами
    query = """
        SELECT * FROM {}
        WHERE symbol = ?
        AND timestamp BETWEEN ? AND ?
        ORDER BY timestamp ASC
    """.format(
        table
    )  # Имя таблицы не может быть параметром, но timeframe уже проверен

    try:
        with sqlite3.connect(DB_PATH) as conn:
            df = pd.read_sql(query, conn, params=(symbol, start, end))
            assert isinstance(df, pd.DataFrame)

        print("[DEBUG] SQL df shape:", df.shape)

        if df.empty:
            print("⚠️ DataFrame пуст — свечи не найдены по запросу.")
            return [], {}

        print("[DEBUG] SQL df head:\n", df.head(3))

        # Фильтрация по OHLC колонкам
        ohlc_cols = ["open", "high", "low", "close"]
        df = df[df[ohlc_cols].notna().all(axis=1)]
        assert isinstance(df, pd.DataFrame)

        print("[DEBUG] После фильтрации OHLC:", df.shape)
        print("[DEBUG] Пример:\n", df.head(3))

        # Преобразование timestamp в int
        df["timestamp"] = df["timestamp"].astype(int)

        # Конвертируем в список словарей
        all_data = df.to_dict(orient="records")

        # Разделяем OHLCV и EMA данные
        ohlcv_cols = [
            "symbol",
            "timestamp",
            "timestamp_ns",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        candles_data = []

        for record in all_data:
            candle_record = {
                col: record.get(col) for col in ohlcv_cols if col in record
            }
            candles_data.append(candle_record)

        # Извлекаем EMA данные если нужно
        ema_data = {}
        if include_ema and ema_periods:
            ema_data = extract_ema_from_candles(all_data, ema_periods)

        return candles_data, ema_data

    except Exception as e:
        print(f"Ошибка при выполнении запроса к базе данных: {e}")
        return [], {}


def get_ema_data_multi_timeframe(
    symbol: str, timeframes: List[str], start: int, end: int, periods: List[str]
) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """
    Загружает EMA данные для нескольких таймфреймов.

    Returns:
        {timeframe: {period: [{'time': timestamp, 'value': ema_value}]}}
    """
    result = {}

    for tf in timeframes:
        if tf not in TIMEFRAMES_CONFIG:
            continue

        try:
            # Получаем только EMA данные
            _, ema_data = get_candles_with_ema_from_db(
                symbol, tf, start, end, include_ema=True, ema_periods=periods
            )
            if ema_data:
                result[tf] = ema_data
        except Exception as e:
            print(f"Ошибка при загрузке EMA для {tf}: {e}")
            continue

    return result
