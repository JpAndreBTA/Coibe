# COIBE.IA Project Overview

COIBE.IA is a local platform for public-data search, risk analysis, and detection of possible overpricing/anomalies in Brazilian government contracting data.

Main stack:
- Backend: Python, FastAPI, Uvicorn, Pydantic v2, httpx, python-dotenv.
- Data/ML: pandas and scikit-learn when available; statistical fallbacks are used for some anomaly paths.
- Frontend: Vite, React 18, Tailwind CSS, Lucide React.
- Runtime/data: local JSON/JSONL files under `data/`, logs under `logs/`.

Main responsibilities:
- `main.py`: FastAPI app, API endpoints, Pydantic models, connector calls, risk rules, public search, monitoring feed/map endpoints, local file persistence helpers.
- `app.jsx`: React single-page frontend for monitoring dashboard, search, map, alert details, and analysis panel.
- `src/main.jsx` and `src/index.css`: frontend entrypoint and Tailwind/global CSS.
- `local_monitor.py`: continuous/one-shot collection pipeline; collects public data, merges/deduplicates local databases, applies risk rules and adaptive ML/statistics, writes snapshots/analysis/alerts.
- `coibe_prompt_monitor.py`: terminal dashboard/monitor flow.
- `detect_price_anomalies.py`: small standalone anomaly example using pandas/scikit-learn.

Important generated or runtime directories:
- `data/raw`: raw snapshots.
- `data/processed`: accumulated monitoring/public records and latest analysis.
- `data/alerts`: per-alert JSON files.
- `data/library`: incremental JSONL library and indexes/codes.
- `logs`: process logs when launched by PowerShell scripts.
- `dist`, `node_modules`, `__pycache__`: generated/dependency/cache directories.

Pipeline order documented in README:
1. `connectors_and_scrapers`: public APIs/connectors including Compras.gov.br, IBGE, Camara, Senado, Brasil API, Querido Diario, Portal da Transparencia status.
2. `raw_database_merge`: write raw snapshots and deduplicate accumulated `monitoring_items.json` and `public_api_records.json`.
3. `risk_rules_and_ml`: apply COIBE risk rules and statistical/ML analysis only after collection/merge.
