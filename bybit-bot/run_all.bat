@echo off
echo 🌀 Полная синхронизация и запуск backend

REM 🔁 Сначала синхронизация базы
python backend\sync_all.py

REM 🚀 Потом запуск FastAPI
cd backend
uvicorn api.main:app --reload
