@echo off
cd /d D:\_project_bybit_bot\bybit-bot

REM ✅ Активируем окружение
call venv\Scripts\activate.bat

REM ✅ Устанавливаем PYTHONPATH
set PYTHONPATH=%CD%\backend

REM ✅ Запускаем бекенд (в фоне)
start cmd /k uvicorn backend.api.main:app --reload --port 8000

REM ✅ Запускаем sync_all (ожидаемо, не в фоне)
python backend\sync_all.py

pause
