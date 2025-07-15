@echo off
setlocal

echo 🐍 Активируем виртуальное окружение...
call venv\Scripts\activate.bat

echo 🔍 Проверяем недостающие свечи...
python backend\tools\check_missing_candles.py

echo 📥 Загружаем последние данные...
python backend\core\data\data_backfill.py

echo 🔧 Дозагружаем пропущенные свечи...
python backend\core\data\data_extended_backfill.py

echo 🏗️ Синхронизируем структуру базы данных...
python backend\tools\update_db_structure.py

echo 🚀 Запускаем FastAPI backend на http://localhost:8000 ...
uvicorn backend.api.main:app --reload --host 0.0.0.0 --port 8000

endlocal
