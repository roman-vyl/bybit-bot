"""
duplicate_cleaner.py

Удаляет дубликаты свечей из таблиц candles_<tf> в базе данных,
оставляя только самую «последнюю» запись по каждому timestamp.
Использует MAX(rowid), чтобы не удалять загруженные исторические свечи
из data_extended_backfill.py
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
    print(f"\n🔍 Проверка таблицы: {table}")

    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    total = cursor.fetchone()[0]

    cursor.execute(f"SELECT COUNT(DISTINCT timestamp) FROM {table}")
    unique = cursor.fetchone()[0]

    duplicates = total - unique
    if duplicates <= 0:
        print("   ✅ Дубликатов нет")
        continue

    print(f"   ⚠ Найдено дубликатов: {duplicates}")

    # Используем MAX(rowid), чтобы сохранить последние вставленные записи (из backfill)
    cursor.execute(
        f"""
        DELETE FROM {table}
        WHERE rowid NOT IN (
            SELECT MAX(rowid) FROM {table} GROUP BY timestamp
        )
    """
    )
    conn.commit()

    print(f"   🧹 Удалено: {duplicates} записей")

conn.close()
print("\n🏁 Очистка дублей завершена безопасно.")
