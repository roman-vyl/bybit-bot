@echo off

echo 🌐 Запускаем frontend на http://localhost:3000 ...
cd frontend
call npm run dev:fast

echo ✅ Всё запущено. Закройте это окно, если хотите завершить все процессы вручную.

endlocal