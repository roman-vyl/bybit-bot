@echo off
REM Запуск frontend (Next.js)

cd /d "%~dp0frontend"
echo Текущая директория: %cd%

echo Устанавливаем переменные окружения...
set NODE_ENV=development

echo Запускаем Next.js dev-сервер...
call npm run dev

pause
