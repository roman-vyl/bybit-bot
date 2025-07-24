import sqlite3
from pathlib import Path

# Исправленный путь к базе данных
DB_PATH = Path(__file__).resolve().parent.parent.parent / "db" / "market_data.sqlite"

# Список таймфреймов
timeframes = ["1m", "5m", "30m", "1h", "4h", "6h", "12h", "1d", "1w"]

# Базовая структура таблицы
table_template = """
DROP TABLE IF EXISTS candles_{tf};
CREATE TABLE candles_{tf} (
    symbol TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    timestamp_ms INTEGER,
    timestamp_ns INTEGER,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    PRIMARY KEY (symbol, timestamp)
);
"""

# Генерация полного SQL
sql = "\n".join([table_template.format(tf=tf) for tf in timeframes])


def recreate_tables():
    print(f"🚀 Recreating candle tables in: {DB_PATH}")
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(sql)
    print("✅ All candle tables recreated successfully.")


if __name__ == "__main__":
    recreate_tables()
