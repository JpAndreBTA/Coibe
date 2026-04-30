@echo off
setlocal
cd /d "%~dp0"

echo.
echo =====================================
echo   COIBE.IA - Backend + PostgreSQL
echo =====================================
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start-coibe-backend.ps1"

echo.
echo Backend finalizado.
pause
