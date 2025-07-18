@echo off
cd /d D:\_project_bybit_bot\bybit-bot

echo 🔁 Активация виртуального окружения...
call venv\Scripts\activate

echo 🧹 Проверка целостности данных...
python backend\tools\check_missing_candles.py

echo 🧹 Фильтрация битых timestamps...
python backend\tools\bad_data_filter.py

echo [📊] Расчёт недостающих EMA...
python backend\core\indicators\calculate_ema_combined.py --incremental

echo 🧹 Очистка дубликатов из БД...
python backend\tools\duplicate_cleaner.py

echo 📈 Запуск визуализации EMA (Streamlit)...
streamlit run backend\visual/plot_ema_streamlit.py
