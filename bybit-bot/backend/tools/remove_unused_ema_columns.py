import sqlite3
import os
import re

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.normpath(
    os.path.join(SCRIPT_DIR, "..", "..", "db", "market_data.sqlite")
)
EMA_CONFIG_PATH = os.path.normpath(
    os.path.join(SCRIPT_DIR, "..", "..", "backend", "config", "ema_periods.txt")
)


def get_required_ema_columns():
    with open(EMA_CONFIG_PATH, "r") as f:
        return {f"ema{line.strip()}" for line in f if line.strip().isdigit()}


def get_existing_tables(conn):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'candles_%';"
    )
    return [row[0] for row in cursor.fetchall()]


def get_table_columns(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name});")
    return [row[1] for row in cursor.fetchall()]


def recreate_table_without_unused_emas(conn, table_name, allowed_ema_columns):
    print(f"🔧 Rebuilding table: {table_name}")
    cursor = conn.cursor()

    existing_cols = get_table_columns(conn, table_name)
    base_cols = [col for col in existing_cols if not col.startswith("ema")]
    valid_emas = [col for col in existing_cols if col in allowed_ema_columns]
    columns_to_keep = base_cols + valid_emas

    def get_column_def(col):
        if col == "timestamp":
            return f"{col} INTEGER PRIMARY KEY"
        elif col == "timestamp_ns":
            return f"{col} INTEGER"
        else:
            return f"{col} REAL"

    column_defs = ",\n    ".join(get_column_def(col) for col in columns_to_keep)
    tmp_table = f"{table_name}_tmp"

    cursor.execute(f"DROP TABLE IF EXISTS {tmp_table}")
    cursor.execute(f"CREATE TABLE {tmp_table} (\n    {column_defs}\n)")
    col_list = ", ".join(columns_to_keep)
    cursor.execute(
        f"INSERT INTO {tmp_table} ({col_list}) SELECT {col_list} FROM {table_name}"
    )
    cursor.execute(f"DROP TABLE {table_name}")
    cursor.execute(f"ALTER TABLE {tmp_table} RENAME TO {table_name}")
    conn.commit()


def clean_unused_ema_columns():
    if not os.path.exists(DB_PATH):
        print(f"❌ DB not found: {DB_PATH}")
        return

    allowed_emas = get_required_ema_columns()

    with sqlite3.connect(DB_PATH) as conn:
        tables = get_existing_tables(conn)
        for table in tables:
            cols = get_table_columns(conn, table)
            current_emas = {col for col in cols if re.match(r"^ema\d+$", col)}
            unused = current_emas - allowed_emas
            if unused:
                print(f"🗑️  Removing unused columns from {table}: {sorted(unused)}")
                recreate_table_without_unused_emas(conn, table, allowed_emas)

    print("✅ Удаление неиспользуемых EMA-колонок завершено.")


if __name__ == "__main__":
    clean_unused_ema_columns()
