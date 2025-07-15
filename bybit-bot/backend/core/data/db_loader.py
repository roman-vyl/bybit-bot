import sqlite3
import pandas as pd
import orjson
from typing import List
from datetime import datetime, timezone
from backend.config.timeframes_config import TIMEFRAMES_CONFIG


def get_unix_timestamp(dt_str: str) -> int:
    """Преобразует ISO-строку в UNIX timestamp (UTC)."""
    if dt_str.endswith("Z"):
        dt_str = dt_str[:-1]
    dt = datetime.fromisoformat(dt_str).replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def fill_missing_candles(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    df = df.drop_duplicates(subset="timestamp", keep="last")
    if df.empty or "timestamp" not in df.columns:
        return df

    # Преобразуем в datetime (UTC)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)

    interval_sec = TIMEFRAMES_CONFIG[timeframe]["interval_sec"]
    start = df["timestamp"].min()
    end = df["timestamp"].max()

    # Строим полную временную ось
    full_range = pd.date_range(start=start, end=end, freq=f"{interval_sec}s")

    df = df.set_index("timestamp")
    df = df[~df.index.duplicated(keep="last")]
    df_full = df.reindex(full_range)

    df_full.index.name = "timestamp"
    df_full = df_full.reset_index()

    # Преобразуем обратно в int timestamp
    df_full["timestamp"] = df_full["timestamp"].apply(lambda dt: int(dt.timestamp()))

    return df_full


def get_candles_from_db(
    symbol: str, timeframe: str, start: str, end: str
) -> List[dict]:
    """Загружает свечи из SQLite по символу, таймфрейму и диапазону времени."""
    db_path = "db/market_data.sqlite"
    table = f"candles_{timeframe}"

    start_ts = get_unix_timestamp(start)
    end_ts = get_unix_timestamp(end)
    print(f"[DEBUG] get_candles_from_db: raw start={start}, end={end}")
    print(f"[DEBUG] get_candles_from_db: parsed start_ts={start_ts}, end_ts={end_ts}")

    query = f"""
        SELECT * FROM {table}
        WHERE symbol = ?
        AND timestamp BETWEEN ? AND ?
        ORDER BY timestamp ASC
    """

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql(query, conn, params=(symbol, start_ts, end_ts))
        print("[DEBUG] SQL df shape:", df.shape)
        print("[DEBUG] SQL df head:\n", df.head(3))

        df = fill_missing_candles(df, timeframe)
        # Удаляем свечи без всех OHLC значений (это "вставленные" пустые строки)
        ohlc_cols = ["open", "high", "low", "close"]
        before = df.shape[0]
        df = df[df[ohlc_cols].notna().all(axis=1)]
        after = df.shape[0]
        print(f"[DEBUG] Удалено пустых свечей: {before - after}, осталось: {after}")

        print("[DEBUG] После fill_missing_candles:", df.shape)
        print("[DEBUG] Пример после fill:\n", df.head(3))

    # фильтрация: только строки с полным OHLC
    df = df[
        df["open"].notna()
        & df["high"].notna()
        & df["low"].notna()
        & df["close"].notna()
    ]
    print("[DEBUG] После фильтрации OHLC:", df.shape)

    # ⏱ timestamp → ISO 8601
    if "timestamp" in df.columns:
        df["timestamp"] = df["timestamp"].apply(
            lambda ts: datetime.utcfromtimestamp(int(ts)).isoformat()
        )

    df.replace([float("inf"), float("-inf")], None, inplace=True)
    df = df.where(pd.notna(df), None)
    # 🔍 Отфильтровать строки без OHLC
    ohlc_cols = ["open", "high", "low", "close"]
    df = df[df[ohlc_cols].notna().all(axis=1)]
    print(f"[DEBUG] После фильтрации OHLC: {df.shape}")

    return orjson.loads(orjson.dumps(df.to_dict(orient="records")))
