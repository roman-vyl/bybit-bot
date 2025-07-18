import sqlite3
import pandas as pd
from typing import List


def get_candles_from_db(
    symbol: str, timeframe: str, start: int, end: int
) -> List[dict]:
    """Загружает свечи из SQLite по символу, таймфрейму и диапазону времени."""
    db_path = "db/market_data.sqlite"
    table = f"candles_{timeframe}"

    query = f"""
        SELECT * FROM {table}
        WHERE symbol = ?
        AND timestamp BETWEEN ? AND ?
        ORDER BY timestamp ASC
    """

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql(query, conn, params=(symbol, start, end))
        assert isinstance(df, pd.DataFrame)
        print("[DEBUG] SQL df shape:", df.shape)
        print("[DEBUG] SQL df head:\n", df.head(3))

        ohlc_cols = ["open", "high", "low", "close"]
        df = df[df[ohlc_cols].notna().all(axis=1)]
        assert isinstance(df, pd.DataFrame)

        print("[DEBUG] После фильтрации OHLC:", df.shape)
        print("[DEBUG] Пример:\n", df.head(3))

    df["timestamp"] = df["timestamp"].astype(int)

    df.replace([float("inf"), float("-inf")], None, inplace=True)

    return df.to_dict(orient="records")
