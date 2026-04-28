@echo off
setlocal
cd /d "%~dp0"

where py >nul 2>nul
if %ERRORLEVEL% EQU 0 (
  py -3.10 coibe_prompt_monitor.py --interval-minutes 0 --pages 4 --page-size 50
) else (
  python coibe_prompt_monitor.py --interval-minutes 0 --pages 4 --page-size 50
)

pause
