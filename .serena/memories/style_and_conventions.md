# Style and Conventions

Python style observed:
- Plain module-level functions and Pydantic `BaseModel` classes in `main.py`.
- Type hints are used often, including modern unions like `str | None`, `dict[str, Any]`, `list[MonitoringItem]`.
- Async functions are used for API calls and FastAPI endpoints.
- Naming is snake_case for Python functions/variables and PascalCase for Pydantic models.
- JSON persistence uses `pathlib.Path`, local helper functions like `write_json`, and explicit JSON default handling for dates/decimals.
- Risk findings are intentionally phrased as factors of attention / potential risk / atypical variation, not as formal accusations of fraud or corruption.

Frontend style observed:
- React function components and hooks (`useState`, `useEffect`, `useMemo`, `useRef`).
- `app.jsx` contains the main app and helpers; `src/main.jsx` imports and mounts it.
- Tailwind utility classes are used heavily. Visual language is dark/black neutral UI with red accents and compact dashboard panels.
- Icons come from `lucide-react`.
- JavaScript uses ES modules and no TypeScript.

Repository conventions:
- Generated/runtime directories (`node_modules`, `dist`, `data`, `logs`, `__pycache__`) should generally be avoided for source edits unless the task specifically concerns runtime data/logs.
- `.env` is ignored and contains sensitive/local configuration; do not print or commit secrets.
- `.gitignore` excludes `.env`, `.venv/`, `__pycache__/`, `*.pyc`, `node_modules/`, `dist/`, `data/`, `data_test/`.

Testing/linting conventions:
- No explicit pytest, ruff, black, mypy, eslint, prettier, or vitest config/scripts were found during onboarding.
- Prefer project-local patterns over introducing a new formatter/linter unless requested.
