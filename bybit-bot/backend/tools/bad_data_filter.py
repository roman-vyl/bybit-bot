"""
bad_data_filter.py

Удаляет строки с некорректными timestamp из таблиц candles_<tf>,
если они выходят за диапазон Unix timestamp: [1_000_000_000; 2_000_000_000]
"""

import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]  # backend/
PROJECT_ROOT = BASE_DIR.parent  # bybit-bot/
DB_PATH = PROJECT_ROOT / "db" / "market_data.sqlite"


conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'candles_%'"
)
tables = [row[0] for row in cursor.fetchall()]

for table in tables:
    print(f"\n🧹 Проверка {table}...")

    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    total = cursor.fetchone()[0]

    cursor.execute(
        f"""
        DELETE FROM {table}
        WHERE timestamp < 1000000000 OR timestamp > 2000000000
    """
    )
    conn.commit()

    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    after = cursor.fetchone()[0]

    removed = total - after
    if removed == 0:
        print("   ✅ Плохих данных не найдено")
    else:
        print(f"   ⚠ Удалено строк: {removed}")

conn.close()
print("\n🏁 Фильтрация завершена.")
