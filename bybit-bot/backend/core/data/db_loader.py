import sqlite3
import pandas as pd
from typing import List
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


def get_candles_from_db(
    symbol: str, timeframe: str, start: int, end: int
) -> List[dict]:
    """Загружает свечи из SQLite по символу, таймфрейму и диапазону времени."""

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
            return []

        print("[DEBUG] SQL df head:\n", df.head(3))

        # Фильтрация по OHLC колонкам
        ohlc_cols = ["open", "high", "low", "close"]
        df = df[df[ohlc_cols].notna().all(axis=1)]
        assert isinstance(df, pd.DataFrame)

        print("[DEBUG] После фильтрации OHLC:", df.shape)
        print("[DEBUG] Пример:\n", df.head(3))

        # Преобразование timestamp в int
        df["timestamp"] = df["timestamp"].astype(int)

        return df.to_dict(orient="records")

    except Exception as e:
        print(f"Ошибка при выполнении запроса к базе данных: {e}")
        return []
