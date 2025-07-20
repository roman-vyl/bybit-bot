import sqlite3
from pathlib import Path

# Абсолютный путь до корня проекта
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "db" / "market_data.sqlite"

# Подключение к базе
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Находим все таблицы вида candles_*
cursor.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'candles_%'"
)
tables = [row[0] for row in cursor.fetchall()]

for table in tables:
    print(f"\n📥 Обработка {table}...")

    # Проверяем наличие нужных колонок
    cursor.execute(f"PRAGMA table_info({table})")
    columns = {col[1] for col in cursor.fetchall()}

    if not {"timestamp", "timestamp_ms", "timestamp_ns"}.issubset(columns):
        print(f"⚠️ Пропущено: нет всех полей timestamp_* в {table}")
        continue

    # Пересчитываем и обновляем значения timestamp_ms и timestamp_ns
    cursor.execute(
        f"""
        UPDATE {table}
        SET 
            timestamp_ms = timestamp * 1000,
            timestamp_ns = timestamp * 1000000000
        WHERE timestamp IS NOT NULL
    """
    )

    conn.commit()
    print(f"✅ Обновлены timestamp_ms и timestamp_ns в {table}")

conn.close()
print("\n🏁 Завершено.")
