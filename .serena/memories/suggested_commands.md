# Suggested Commands

Windows/PowerShell is the expected shell.

Setup backend dependencies:
```powershell
py -3.10 -m pip install -r requirements.txt
```

Run backend API locally:
```powershell
py -3.10 -m uvicorn main:app --reload
```
API docs: http://127.0.0.1:8000/docs
Healthcheck: http://127.0.0.1:8000/health

Setup frontend dependencies:
```powershell
npm install
```

Run frontend dev server:
```powershell
npm run dev
```
Default Vite URL is usually http://127.0.0.1:5173/ unless overridden.

Build frontend:
```powershell
npm run build
```

Preview built frontend:
```powershell
npm run preview
```

Run monitor once:
```powershell
py -3.10 local_monitor.py --once --pages 10 --page-size 50
```

Run continuous monitor:
```powershell
py -3.10 local_monitor.py --interval-minutes 0 --pages 10 --page-size 50
```
Use a larger interval such as `--interval-minutes 60` to reduce public API load.

Run terminal dashboard flow:
```powershell
.\abrir_coibe_prompt.cmd
```

Start backend, frontend, and automatic analysis via PowerShell:
```powershell
.\run_coibe_local.ps1
```
This defaults to backend port 8000 and frontend port 5174, writes logs to `logs/`, and stops existing processes on those ports first.

Alternative full startup in separate windows:
```powershell
.\iniciar_coibe_completo.bat
```

Useful Windows/PowerShell commands:
```powershell
Get-ChildItem -Force
rg --files
rg "pattern" path
Get-Content path -TotalCount 100
Get-Content path | Select-Object -Skip 100 -First 100
Get-Process
Get-NetTCPConnection -LocalPort 8000
Stop-Process -Id <pid> -Force
```

Git commands:
```powershell
git status --short
git diff -- path
git log --oneline -5
```
