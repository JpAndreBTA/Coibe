# Task Completion Checklist

When code changes are made:
1. Check `git status --short` before/after to understand touched files and avoid reverting unrelated user changes.
2. For backend changes, at minimum run a syntax/import-oriented check when feasible:
```powershell
py -3.10 -m compileall main.py local_monitor.py coibe_prompt_monitor.py detect_price_anomalies.py
```
If the touched file set is smaller, compile only the changed Python files.
3. For frontend changes, run:
```powershell
npm run build
```
This is the main available frontend validation because no lint/test script is defined.
4. For API behavior changes, run the backend and check relevant endpoint(s), for example:
```powershell
py -3.10 -m uvicorn main:app --reload
```
Then open `/docs` or call `/health` and the changed endpoint.
5. For monitor/pipeline changes, run a small one-shot cycle when appropriate:
```powershell
py -3.10 local_monitor.py --once --pages 1 --page-size 10
```
Be mindful that this can contact public APIs and update `data/`.
6. Do not commit generated/runtime outputs unless explicitly requested: `dist/`, `data/`, `logs/`, `__pycache__/`, `node_modules/`.
7. Keep risk language legally cautious: use terms like factor de atencao, risco potencial, variacao atipica, evidencia, criterio calculado; avoid formal accusations.

If verification cannot be run, state exactly what was skipped and why.
