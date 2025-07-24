import sqlite3

# Путь к базе данных
DB_PATH = "bybit-bot/db/market_data.sqlite"

# Список таблиц, которые нужно очистить
CANDLES_TABLES = [
    "candles_1m",
    "candles_5m",
    "candles_30m",
    "candles_1h",
    "candles_4h",
    "candles_6h",
    "candles_12h",
    "candles_1d",
    "candles_1w",
]


def clear_tables():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for table in CANDLES_TABLES:
        print(f"Очищаю таблицу {table}...")
        cur.execute(f"DELETE FROM {table}")
    conn.commit()
    conn.close()
    print("Готово!")


if __name__ == "__main__":
    clear_tables()
