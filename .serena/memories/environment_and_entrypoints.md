# Environment and Entrypoints

Environment:
- Windows project path: `C:\Users\jpzin\Downloads\Coibe`.
- PowerShell is the working shell.
- Backend expects `.env` with at least Portal da Transparencia settings when using that connector:
  - `PORTAL_TRANSPARENCIA_API_KEY`
  - `PORTAL_TRANSPARENCIA_EMAIL`
  - `PORTAL_TRANSPARENCIA_BASE_URL` (default in docs: `https://api.portaldatransparencia.gov.br/api-de-dados`)
- The backend sends the Portal da Transparencia key in the official `chave-api-dados` header.

Backend entrypoint:
- `main.py` exposes FastAPI `app`.
- Run with `py -3.10 -m uvicorn main:app --reload`.
- Important docs/endpoints from README include `/health`, `/api/sources`, `/api/search`, `/api/search/index`, `/api/search/autocomplete`, `/api/monitoring/feed`, `/api/monitoring/map`, `/api/monitoring/state-map`, `/api/public-data/ibge/states-geojson`, `/api/analyze-cnpj/{cnpj}`, `/api/analyze-contract`, `/api/analyze-superpricing`, `/api/analyze-spatial-risk`, `/api/pipeline/readiness`, `/api/scrape/public-page`.

Frontend entrypoint:
- `src/main.jsx` mounts `CoibeApp` from root `app.jsx`.
- Vite scripts are in `package.json`: `dev`, `build`, `preview`.
- `app.jsx` tries API bases `http://127.0.0.1:8000` and `http://127.0.0.1:8001`.

Operational scripts:
- `run_coibe_local.ps1`: starts backend and frontend, defaults backend 8000/frontend 5174, writes logs to `logs/`, can stop processes on those ports.
- `iniciar_coibe_completo.bat`: alternative startup with separate windows.
- `abrir_coibe_prompt.cmd`: terminal/prompt dashboard flow using backend and monitor logic.
- `local_monitor.py`: collection/analysis pipeline; can run once or continuously.
