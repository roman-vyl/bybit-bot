import subprocess
import sys
import os
from pathlib import Path
import hashlib

BASE_DIR = Path(__file__).resolve().parent
PYTHON = sys.executable


def run(label, path):
    print(f"\n🔧 {label}")
    env = os.environ.copy()
    env["PYTHONPATH"] = str(BASE_DIR.parent)
    result = subprocess.run([PYTHON, str(path)], env=env)
    if result.returncode != 0:
        print(f"❌ Ошибка при запуске {path}")
        sys.exit(result.returncode)


def hash_configs():
    files = [
        BASE_DIR / "config" / "ema_periods.txt",
        BASE_DIR / "config" / "timeframes_config.py",
    ]
    hasher = hashlib.sha256()
    for file in files:
        if file.exists():
            with open(file, "rb") as f:
                hasher.update(f.read())
    return hasher.hexdigest()


def main():
    print("🚀 Синхронизация базы данных и индикаторов")
    config_hash_file = BASE_DIR / "config" / ".sync_hash"

    current_hash = hash_configs()
    previous_hash = (
        config_hash_file.read_text().strip() if config_hash_file.exists() else ""
    )

    # Шаг 1. Загрузка свежих данных
    run("Шаг 1: Загрузка последних свечей", BASE_DIR / "core/data/data_backfill.py")

    # Шаг 2. Дозагрузка пропущенных
    run(
        "Шаг 2: Поиск и загрузка пропущенных свечей",
        BASE_DIR / "core/data/data_extended_backfill.py",
    )

    # Шаг 3. Пересчёт EMA
    if current_hash != previous_hash:
        print("🔁 Конфиги изменились — выполняем ПОЛНЫЙ пересчёт EMA")
        run(
            "Шаг 3: Полный пересчёт EMA",
            BASE_DIR / "core/indicators/calculate_ema_combined.py",
        )
        config_hash_file.write_text(current_hash)
    else:
        run(
            "Шаг 3: Инкрементальный пересчёт EMA",
            BASE_DIR / "core/indicators/calculate_ema_combined.py",
        )

    # Шаг 4. Обновление структуры таблиц
    run("Шаг 4: Синхронизация схемы таблиц", BASE_DIR / "tools/update_db_structure.py")

    # Шаг 5. Логирование пропущенных свечей
    run(
        "Шаг 5: Проверка пропусков в данных",
        BASE_DIR / "tools/check_missing_candles.py",
    )

    print("\n✅ Полная синхронизация завершена")

    # Шаг 6. Фильтрация плохих timestamp
    run(
        "Шаг 6: Очистка некорректных timestamp",
        BASE_DIR / "tools/bad_data_filter.py",
    )

    # Шаг 7. Удаление дубликатов
    run(
        "Шаг 7: Удаление дубликатов по timestamp",
        BASE_DIR / "tools/duplicate_cleaner.py",
    )

    # Шаг 8. Запуск realtime data loader
    run(
        "Шаг 8: Запуск realtime data loader",
        BASE_DIR / "bybit_realtime_data_loader" / "run_bybit_loader.py",
    )


if __name__ == "__main__":
    main()
