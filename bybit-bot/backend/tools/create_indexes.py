import sqlite3
import re

DB_PATH = "D:/_project_bybit_bot/bybit-bot/market_data.sqlite"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Получаем все таблицы, начинающиеся с 'candles_'
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [row[0] for row in cursor.fetchall() if re.match(r"candles_\w+", row[0])]

for table in tables:
    index_name = f"idx_{table}_symbol_timestamp"
    try:
        cursor.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {index_name}
            ON {table} (symbol, timestamp)
        """
        )
        print(f"✅ Index created or exists: {index_name}")
    except Exception as e:
        print(f"❌ Failed to create index for {table}: {e}")

conn.commit()
conn.close()
