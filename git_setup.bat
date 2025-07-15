@echo off
echo 📦 Настройка Git репозитория для Bybit Bot

echo ⚙️ Настройка Git конфигурации...
git config user.name "Bybit Bot Developer"
git config user.email "developer@bybit-bot.local"

echo 📄 Добавляем файлы в staging...
git add .

echo 💾 Создаем первый коммит...
git commit -m "Initial commit: Bybit Trading Bot

- FastAPI backend with EMA indicators
- Next.js frontend with interactive charts
- SQLite database for historical data
- Automated data loading scripts
- Support for multiple timeframes (1m-1w)
- Working API endpoints for candles and indicators
- Responsive web interface with chart visualization"

echo 🌟 Создаем ветку main...
git branch -M main

echo 🚀 Инструкция для подключения к GitHub:
echo.
echo 1. Создайте новый репозиторий на GitHub
echo 2. Выполните команду:
echo    git remote add origin https://github.com/YOUR_USERNAME/bybit-bot.git
echo 3. Запуште код:
echo    git push -u origin main
echo.
echo ✅ Репозиторий готов к загрузке на GitHub!

pause 