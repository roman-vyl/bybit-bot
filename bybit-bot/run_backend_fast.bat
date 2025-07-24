@echo off
cd /d D:\_project_bybit_bot\bybit-bot

call venv\Scripts\activate.bat
set PYTHONPATH=%CD%

:: 🚀 Запуск FastAPI
start cmd /k "uvicorn backend.api.main:app --reload --port 8000"

:: 🧮 Перерасчёт EMA и RSI (недостающих)
python backend/indicators/calculate_ema_combined.py --incremental

:: 🔁 Запуск realtime loader
start cmd /k "python -m backend.bybit_realtime_data_loader.run_bybit_loader"

exit