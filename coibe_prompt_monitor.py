import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from urllib.error import URLError
from urllib.request import urlopen

from local_monitor import DEFAULT_FEED_PAGE_SIZE, DEFAULT_SEARCH_TERMS, ensure_dirs, run_once


DEFAULT_API_BASE = "http://127.0.0.1:8000"


def supports_color() -> bool:
    return bool(os.environ.get("WT_SESSION") or os.environ.get("ANSICON") or sys.stdout.isatty())


COLOR = supports_color()
UNICODE = (sys.stdout.encoding or "").lower().replace("-", "") in {"utf8", "utf8sig"}


def paint(text: str, code: str) -> str:
    if not COLOR:
        return text
    return f"\033[{code}m{text}\033[0m"


def money(value) -> str:
    try:
        number = float(value or 0)
    except Exception:
        number = 0.0
    return f"R$ {number:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")


def pct_bar(value: float, max_value: float, width: int = 24) -> str:
    if max_value <= 0:
        fill = 0
    else:
        fill = min(width, max(0, int((value / max_value) * width)))
    filled = "█" if UNICODE else "#"
    empty = "░" if UNICODE else "-"
    return paint(filled * fill, "31") + paint(empty * (width - fill), "90")


def clear_screen() -> None:
    os.system("cls" if os.name == "nt" else "clear")


def api_ready(api_base: str) -> bool:
    try:
        with urlopen(f"{api_base}/health", timeout=2) as response:
            return response.status < 500
    except (OSError, URLError):
        return False


def start_backend(api_base: str, root: Path, log_dir: Path) -> subprocess.Popen | None:
    if api_ready(api_base):
        return None

    port = api_base.rstrip("/").split(":")[-1]
    python_exe = sys.executable
    stdout = (log_dir / "prompt_backend.out.log").open("a", encoding="utf-8")
    stderr = (log_dir / "prompt_backend.err.log").open("a", encoding="utf-8")
    process = subprocess.Popen(
        [
            python_exe,
            "-m",
            "uvicorn",
            "main:app",
            "--host",
            "127.0.0.1",
            "--port",
            port,
        ],
        cwd=root,
        stdout=stdout,
        stderr=stderr,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )

    for _ in range(45):
        if api_ready(api_base):
            return process
        time.sleep(1)

    return process


def load_latest(data_dir: Path) -> dict:
    path = data_dir / "processed" / "latest_analysis.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def risk_counts(items: list[dict]) -> dict[str, int]:
    counts = {"alto": 0, "médio": 0, "baixo": 0, "indeterminado": 0}
    for item in items:
        risk = str(item.get("risk_level") or "indeterminado").lower()
        if risk == "medio":
            risk = "médio"
        counts[risk if risk in counts else "indeterminado"] += 1
    return counts


def connector_status(connectors: list[dict]) -> tuple[int, int]:
    ok = sum(1 for connector in connectors if connector.get("status") == "ok")
    return ok, max(len(connectors) - ok, 0)


def top_states(analysis: dict, limit: int = 5) -> list[dict]:
    states = ((analysis.get("state_map") or {}).get("states") or [])
    return sorted(states, key=lambda row: (row.get("risk_score") or 0, row.get("total_value") or 0), reverse=True)[:limit]


def top_alerts(analysis: dict, limit: int = 8) -> list[dict]:
    alerts = analysis.get("alerts") or []
    return sorted(alerts, key=lambda row: (row.get("risk_score") or 0, float(row.get("value") or 0)), reverse=True)[:limit]


def interval_label(interval_minutes: float) -> str:
    return "imediata" if interval_minutes <= 0 else f"{interval_minutes:g} min"


def render_dashboard(analysis: dict, cycle: int, interval_minutes: float, api_base: str) -> None:
    clear_screen()
    items = analysis.get("items") or []
    alerts = analysis.get("alerts") or []
    connectors = analysis.get("connectors") or []
    counts = risk_counts(items)
    ok_connectors, failed_connectors = connector_status(connectors)
    total_value = sum(float(item.get("value") or 0) for item in items)
    total_variation = sum(float(item.get("estimated_variation") or 0) for item in items)
    generated_at = analysis.get("generated_at")
    generated_text = generated_at or "aguardando primeira análise"

    header = " COIBE.IA | Monitor automático no prompt "
    print(paint(header.center(96, "="), "1;37;41"))
    print(f"Ciclo: {cycle} | Backend interno: {api_base} | Proxima rodada: {interval_label(interval_minutes)}")
    print(f"Última análise: {generated_text}")
    print()

    print(paint("MÉTRICAS", "1;31"))
    print(f"  Itens analisados      {str(analysis.get('items_analyzed') or len(items)).rjust(10)}")
    print(f"  Alertas priorizados   {str(analysis.get('alerts_count') or len(alerts)).rjust(10)}")
    print(f"  Conectores OK/erro    {str(ok_connectors).rjust(5)} / {failed_connectors}")
    print(f"  Valor monitorado      {money(total_value).rjust(16)}")
    print(f"  Risco estimado ML     {money(total_variation).rjust(16)}")
    print(f"  Biblioteca local      {str((analysis.get('library') or {}).get('library_records_count') or 0).rjust(10)} registros")
    print()

    max_risk = max(counts.values()) if counts else 1
    print(paint("DISTRIBUIÇÃO DE RISCO", "1;31"))
    for label in ("alto", "médio", "baixo", "indeterminado"):
        amount = counts.get(label, 0)
        print(f"  {label.upper().ljust(14)} {pct_bar(amount, max_risk)} {amount}")
    print()

    states = top_states(analysis)
    if states:
        max_state_score = max(float(state.get("risk_score") or 0) for state in states) or 1
        print(paint("UFs COM MAIOR PRIORIDADE", "1;31"))
        for state in states:
            score = float(state.get("risk_score") or 0)
            name = str(state.get("state_name") or state.get("uf") or "")[:24]
            print(
                f"  {str(state.get('uf') or '').ljust(2)} {name.ljust(24)} "
                f"{pct_bar(score, max_state_score, 18)} score {int(score):>3} | {money(state.get('total_value'))}"
            )
        print()

    print(paint("ALERTAS EM DESTAQUE", "1;31"))
    selected_alerts = top_alerts(analysis)
    if not selected_alerts:
        print("  Nenhum alerta alto nesta rodada. Monitoramento preventivo em andamento.")
    for alert in selected_alerts:
        title = " ".join(str(alert.get("title") or "").split())[:72]
        entity = " ".join(str(alert.get("entity") or "").split())[:54]
        print(
            f"  [{str(alert.get('risk_level') or '?').upper():^6}] "
            f"score {int(alert.get('risk_score') or 0):>3} | {money(alert.get('value')).rjust(14)} | {title}"
        )
        print(paint(f"          {entity}", "90"))
    print()

    errors = analysis.get("collection_errors") or []
    if errors:
        print(paint("ERROS DE COLETA", "1;33"))
        for error in errors[:4]:
            print(f"  {error.get('connector')}: {error.get('error')}")
        print()

    print(paint("Arquivos atualizados em data/raw, data/processed, data/alerts e data/library.", "90"))
    print(paint("Pressione Ctrl+C para parar o monitor.", "90"))


async def monitor_loop(args: argparse.Namespace) -> None:
    root = Path(__file__).resolve().parent
    data_dir = root / args.data_dir
    paths = ensure_dirs(data_dir)
    backend = start_backend(args.api_base, root, paths["logs"])

    if backend and not api_ready(args.api_base):
        print("Backend interno ainda não respondeu. Veja logs/prompt_backend.err.log.")

    cycle = 1
    try:
        while True:
            run_args = SimpleNamespace(
                api_base=args.api_base,
                data_dir=str(data_dir),
                interval_minutes=args.interval_minutes,
                pages=args.pages,
                page_size=args.page_size,
                startup_delay_seconds=0,
                once=True,
                search_terms=args.search_terms,
            )
            try:
                await run_once(run_args, paths)
            except Exception as exc:
                clear_screen()
                print(paint("COIBE.IA | erro na rodada de análise", "1;37;41"))
                print(str(exc))
                print("Tentando novamente no próximo ciclo.")

            render_dashboard(load_latest(data_dir), cycle, args.interval_minutes, args.api_base)
            if args.once:
                break
            cycle += 1
            if args.interval_minutes > 0:
                await asyncio.sleep(args.interval_minutes * 60)
    finally:
        if backend and backend.poll() is None:
            backend.terminate()
            try:
                backend.wait(timeout=8)
            except subprocess.TimeoutExpired:
                backend.kill()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="COIBE.IA prompt monitor")
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--interval-minutes", type=float, default=0)
    parser.add_argument("--pages", type=int, default=4)
    parser.add_argument("--page-size", type=int, default=DEFAULT_FEED_PAGE_SIZE)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--search-terms", nargs="*", default=DEFAULT_SEARCH_TERMS)
    return parser.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(monitor_loop(parse_args()))
    except KeyboardInterrupt:
        print("\nMonitor COIBE.IA encerrado.")
