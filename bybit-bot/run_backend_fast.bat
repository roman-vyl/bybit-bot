
@echo off
set PYTHONPATH=%CD%\backend
cd backend
uvicorn api.main:app --reload
cd /d %~dp0
call backend\..\venv\Scripts\activate
set PYTHONPATH=%cd%
uvicorn backend.api.main:app --reload --port 8000
pause
