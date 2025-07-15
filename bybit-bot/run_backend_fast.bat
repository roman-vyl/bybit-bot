@echo off
setlocal

echo 🐍 Активируем виртуальное окружение...
call venv\Scripts\activate.bat

echo 🚀 Запускаем FastAPI backend на http://localhost:8000 ...
uvicorn backend.api.main:app --reload --host 0.0.0.0 --port 8000

endlocal 