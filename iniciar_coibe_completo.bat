@echo off
setlocal
cd /d "%~dp0"

rem Ajustes principais
set BACKEND_PORT=8000
set FRONTEND_PORT=5174
set MONITOR_INTERVAL_MINUTES=0
set MONITOR_PAGES=4
set MONITOR_PAGE_SIZE=50

if not exist logs mkdir logs

echo.
echo ==========================================
echo   COIBE.IA - Plataforma + Backend + Coleta
echo ==========================================
echo.
echo Backend:  http://127.0.0.1:%BACKEND_PORT%/docs
echo Frontend: http://127.0.0.1:%FRONTEND_PORT%/
if "%MONITOR_INTERVAL_MINUTES%"=="0" (
  echo Coleta:   constante, sem pausa entre ciclos
) else (
  echo Coleta:   a cada %MONITOR_INTERVAL_MINUTES% minutos
)
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run_coibe_local.ps1" -MonitorIntervalMinutes %MONITOR_INTERVAL_MINUTES% -MonitorPages %MONITOR_PAGES% -MonitorPageSize %MONITOR_PAGE_SIZE% -BackendPort %BACKEND_PORT% -FrontendPort %FRONTEND_PORT%

echo.
echo Tudo iniciado.
echo Abra: http://127.0.0.1:%FRONTEND_PORT%/
echo.
echo Para parar, feche esta janela e finalize os processos nas portas usadas, se necessario.
echo.
pause
