@echo off
cd /d D:\_project_bybit_bot\bybit-bot

REM ✅ Активируем окружение
call venv\Scripts\activate.bat

REM ✅ Устанавливаем PYTHONPATH
set PYTHONPATH=%CD%\backend

REM ✅ Запускаем пересчет EMA = -1
echo 🚀 Запуск пересчета EMA = -1 с новой валидацией...
python backend\tools\recalculate_ema_minus_one.py

pause 