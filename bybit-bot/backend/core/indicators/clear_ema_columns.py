import sqlite3

DB_PATH = r"D:\_project_bybit_bot\bybit-bot\db\market_data.sqlite"

# какие EMA чистим
EMA_COLS = ["ema20", "ema50", "ema100", "ema200", "ema500"]


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # находим все таблицы, начинающиеся с candles_
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'candles_%'"
    )
    tables = [row[0] for row in cursor.fetchall()]

    print(f"🔍 Найдены таблицы: {tables}")

    for table in tables:
        for col in EMA_COLS:
            try:
                cursor.execute(f"UPDATE {table} SET {col} = NULL")
                print(f"✅ Очищен столбец {col} в {table}")
            except sqlite3.OperationalError:
                print(f"⚠️  Столбца {col} нет в {table}, пропускаем")

    conn.commit()
    conn.close()
    print("🏁 Очистка EMA завершена.")


if __name__ == "__main__":
    main()
