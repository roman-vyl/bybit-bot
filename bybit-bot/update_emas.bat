@echo off
echo 🔄 Обновление структуры таблиц...
python backend/tools/update_db_structure.py

echo 🔄 Перерасчёт EMA...
python backend/core/indicators/calculate_ema.py

pause
