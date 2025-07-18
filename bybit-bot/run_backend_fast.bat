@echo off
echo ⚡ Быстрый запуск API без обновлений

cd backend
uvicorn api.main:app --reload
