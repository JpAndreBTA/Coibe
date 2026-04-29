import argparse
import asyncio
import hashlib
import json
import re
import unicodedata
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from statistics import mean, pstdev
from typing import Any
from zoneinfo import ZoneInfo

import httpx


DEFAULT_API_BASE = "http://127.0.0.1:8000"
BRASILIA_TZ = ZoneInfo("America/Sao_Paulo")
DEFAULT_FEED_PAGE_SIZE = 50
DEFAULT_SEARCH_TERMS = [
    "contrato",
    "computador",
    "combustível",
    "pavimentação",
    "merenda",
    "medicamento",
    "Tiririca",
    "São Paulo",
    "00000000000191",
]


def now_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def brasilia_now() -> datetime:
    return datetime.now(BRASILIA_TZ)


def safe_filename_part(value: Any) -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "sem-id")).strip("._-")
    return (text or "sem-id")[:120]


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def normalize_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = re.sub(r"[^A-Za-z0-9]+", " ", text)
    return " ".join(text.upper().split())


def normalize_decimal(value: Any) -> str:
    try:
        decimal_value = Decimal(str(value or 0))
    except Exception:
        decimal_value = Decimal("0")
    return str(decimal_value.quantize(Decimal("0.01")))


def record_hash(cnpj: Any, event_date: Any, value: Any) -> str:
    raw = "|".join(
        [
            "".join(char for char in str(cnpj or "") if char.isdigit()),
            str(event_date or ""),
            normalize_decimal(value),
        ]
    )
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def ensure_dirs(base_dir: Path) -> dict[str, Path]:
    paths = {
        "raw": base_dir / "raw",
        "processed": base_dir / "processed",
        "alerts": base_dir / "alerts",
        "logs": base_dir / "logs",
        "state": base_dir / "state",
        "library": base_dir / "library",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


async def get_json(client: httpx.AsyncClient, path: str) -> Any:
    response = await client.get(path)
    response.raise_for_status()
    return response.json()


async def collect_connector(
    client: httpx.AsyncClient,
    snapshot: dict[str, Any],
    name: str,
    path: str,
    kind: str,
) -> Any:
    started_at = brasilia_now()
    try:
        payload = await get_json(client, path)
        record_count = 0
        if isinstance(payload, list):
            record_count = len(payload)
        elif isinstance(payload, dict):
            for key in ("items", "results", "states", "points", "dados", "resultado"):
                value = payload.get(key)
                if isinstance(value, list):
                    record_count = len(value)
                    break

        snapshot["connectors"].append(
            {
                "name": name,
                "kind": kind,
                "path": path,
                "status": "ok",
                "record_count": record_count,
                "started_at": started_at.isoformat(),
                "finished_at": brasilia_now().isoformat(),
            }
        )
        return payload
    except Exception as exc:
        snapshot["connectors"].append(
            {
                "name": name,
                "kind": kind,
                "path": path,
                "status": "error",
                "record_count": 0,
                "error": str(exc),
                "started_at": started_at.isoformat(),
                "finished_at": brasilia_now().isoformat(),
            }
        )
        snapshot["errors"].append({"connector": name, "error": str(exc)})
        return None


async def collect_snapshot(api_base: str, search_terms: list[str], pages: int, page_size: int, start_page: int = 1) -> dict[str, Any]:
    timeout = httpx.Timeout(90.0, connect=10.0)
    async with httpx.AsyncClient(base_url=api_base, timeout=timeout) as client:
        snapshot: dict[str, Any] = {
            "collected_at": brasilia_now().isoformat(),
            "api_base": api_base,
            "pipeline_order": ["connectors_and_scrapers", "raw_database_merge", "risk_rules_and_ml"],
            "connectors": [],
            "sources": [],
            "portal_transparencia": {},
            "feed_pages": [],
            "state_map": {},
            "searches": {},
            "errors": [],
        }

        sources = await collect_connector(client, snapshot, "sources", "/api/sources", "connector_registry")
        if sources is not None:
            snapshot["sources"] = sources

        portal_status = await collect_connector(
            client,
            snapshot,
            "portal_transparencia_status",
            "/api/public-data/portal-transparencia/status",
            "public_api_status",
        )
        if portal_status is not None:
            snapshot["portal_transparencia"] = portal_status

        readiness = await collect_connector(
            client,
            snapshot,
            "pipeline_readiness",
            "/api/pipeline/readiness",
            "architecture_status",
        )
        if readiness is not None:
            snapshot["pipeline_readiness"] = readiness

        state_map = await collect_connector(
            client,
            snapshot,
            "state_map_live",
            "/api/monitoring/state-map?page_size=80&source=live",
            "public_api_aggregation",
        )
        if state_map is not None:
            snapshot["state_map"] = state_map

        start_page = max(start_page, 1)
        end_page = start_page + pages
        for page in range(start_page, end_page):
            feed_page = await collect_connector(
                client,
                snapshot,
                f"public_contracts_feed_page_{page}",
                f"/api/monitoring/feed?page={page}&page_size={page_size}&source=live",
                "public_api_feed",
            )
            if feed_page is not None:
                snapshot["feed_pages"].append(feed_page)
            else:
                break

        for term in search_terms:
            encoded = httpx.QueryParams({"q": term})
            search_result = await collect_connector(
                client,
                snapshot,
                f"universal_search:{term}",
                f"/api/search?{encoded}",
                "public_api_search",
            )
            snapshot["searches"][term] = search_result if search_result is not None else {"error": "connector failed", "results": []}

    return snapshot


def flatten_feed(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for page in snapshot.get("feed_pages", []):
        for item in page.get("items", []):
            key = f"{item.get('id')}:{item.get('date')}"
            if key in seen:
                continue
            seen.add(key)
            items.append(item)
    return items


def flatten_public_records(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    collected_at = snapshot.get("collected_at")

    for source in snapshot.get("sources", []):
        if not isinstance(source, dict):
            continue
        records.append(
            {
                "record_key": f"source:{source.get('name')}:{source.get('kind')}",
                "record_type": "connector_source",
                "collected_at": collected_at,
                "source": source.get("name"),
                "title": source.get("name"),
                "subtitle": source.get("coverage"),
                "payload": source,
            }
        )

    portal_status = snapshot.get("portal_transparencia")
    if isinstance(portal_status, dict):
        records.append(
            {
                "record_key": "portal_transparencia:status",
                "record_type": "connector_status",
                "collected_at": collected_at,
                "source": "Portal da Transparencia CGU",
                "title": "Portal da Transparencia status",
                "subtitle": "configured" if portal_status.get("configured") else "missing_api_key",
                "payload": portal_status,
            }
        )

    readiness = snapshot.get("pipeline_readiness")
    if isinstance(readiness, dict):
        records.append(
            {
                "record_key": "pipeline:readiness",
                "record_type": "architecture_status",
                "collected_at": collected_at,
                "source": "COIBE.IA Backend",
                "title": "Pipeline readiness",
                "subtitle": readiness.get("storage_mode"),
                "payload": readiness,
            }
        )

    state_map = snapshot.get("state_map", {})
    for state in state_map.get("states", []) if isinstance(state_map, dict) else []:
        records.append(
            {
                "record_key": f"state:{state.get('uf')}",
                "record_type": "state_risk",
                "collected_at": collected_at,
                "source": "IBGE/Compras.gov.br/PNCP",
                "title": state.get("state_name") or state.get("uf"),
                "subtitle": f"UF {state.get('uf')} score {state.get('risk_score')}",
                "payload": state,
            }
        )

    for item in flatten_feed(snapshot):
        report = item.get("report", {}) if isinstance(item, dict) else {}
        if not isinstance(report, dict):
            continue
        item_id = item.get("id") or item_key(item)
        for index, evidence in enumerate(report.get("public_evidence", []) or []):
            if not isinstance(evidence, dict):
                continue
            record_type = evidence.get("record_type") or "public_evidence"
            records.append(
                {
                    "record_key": f"evidence:{item_id}:{record_type}:{index}",
                    "record_type": record_type,
                    "collected_at": collected_at,
                    "source": evidence.get("source"),
                    "title": evidence.get("title") or item.get("title"),
                    "subtitle": f"{evidence.get('matches_count', 0)} registro(s) vinculados ao item",
                    "url": evidence.get("url"),
                    "payload": {"item": item, "evidence": evidence},
                }
            )

    for term, response in snapshot.get("searches", {}).items():
        if not isinstance(response, dict):
            continue
        for result in response.get("results", []):
            if not isinstance(result, dict):
                continue
            payload = result.get("payload") or {}
            stable_id = (
                payload.get("id")
                or payload.get("idCompra")
                or payload.get("cnpj")
                or payload.get("sigla")
                or result.get("url")
                or result.get("title")
            )
            records.append(
                {
                    "record_key": f"search:{term}:{result.get('type')}:{stable_id}",
                    "record_type": "universal_search_result",
                    "query": term,
                    "collected_at": collected_at,
                    "source": result.get("source"),
                    "title": result.get("title"),
                    "subtitle": result.get("subtitle"),
                    "url": result.get("url"),
                    "risk_level": result.get("risk_level"),
                    "payload": result,
                }
            )

    return records


def ml_value_anomalies(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    values = [float(item.get("value") or 0) for item in items]
    if len(values) < 6:
        return {}

    try:
        import pandas as pd
        from sklearn.ensemble import IsolationForest

        frame = pd.DataFrame({"value": values})
        model = IsolationForest(n_estimators=150, contamination=0.15, random_state=42)
        predictions = model.fit_predict(frame[["value"]])
        scores = model.decision_function(frame[["value"]])
        model_name = "IsolationForest"
    except Exception:
        avg = mean(values)
        deviation = pstdev(values) or 1
        scores = [(value - avg) / deviation for value in values]
        predictions = [-1 if score > 1.7 else 1 for score in scores]
        model_name = "z-score fallback"

    output = {}
    for item, prediction, score in zip(items, predictions, scores):
        value = float(item.get("value") or 0)
        baseline = mean(values)
        estimated_variation = max(0.0, value - baseline)
        uncertainty_floor = False
        if estimated_variation == 0 and value > 0:
            estimated_variation = value * 0.03
            uncertainty_floor = True
        output[item["id"]] = {
            "model": model_name,
            "score": float(score),
            "baseline": round(baseline, 2),
            "estimated_variation": round(estimated_variation, 2),
            "is_uncertainty_floor": uncertainty_floor,
            "is_anomaly": int(prediction) == -1,
            "reason": (
                "Valor contratual fora da curva no lote coletado."
                if int(prediction) == -1
                else "Estimativa calculada contra a média nacional do lote."
            ),
        }
    return output


def item_key(item: dict[str, Any]) -> str:
    return f"{item.get('id')}:{item.get('date')}"


def family_key(item: dict[str, Any]) -> str:
    text = normalize_text(item.get("title") or item.get("object"))
    stopwords = {"AQUISICAO", "CONTRATACAO", "SERVICO", "SERVICOS", "MATERIAL", "CONSUMO", "PARA", "DE", "DA", "DO", "DAS", "DOS", "COM"}
    tokens = [token for token in text.split() if token not in stopwords and len(token) > 2]
    return " ".join(tokens[:4]) or "GERAL"


def risk_points(flags: list[dict[str, Any]]) -> tuple[int, str]:
    points = 0
    for flag in flags:
        risk = normalize_text(flag.get("risk_level")).lower()
        if risk == "alto":
            points += 40
        elif risk == "medio":
            points += 20
        elif risk == "indeterminado":
            points += 5
    score = min(points, 100)
    if score >= 70:
        return score, "alto"
    if score >= 35:
        return score, "médio"
    return score, "baixo"


def append_attention_flag(item: dict[str, Any], flag: dict[str, Any]) -> None:
    report = item.setdefault("report", {})
    flags = report.setdefault("red_flags", [])
    existing_codes = {str(existing.get("code")) for existing in flags if isinstance(existing, dict)}
    if flag["code"] in existing_codes:
        return
    flags.append(flag)
    score, level = risk_points(flags)
    item["risk_score"] = max(int(item.get("risk_score") or 0), score)
    item["risk_level"] = level if score >= int(item.get("risk_score") or 0) else item.get("risk_level", level)
    report["risk_score"] = item["risk_score"]
    report["risk_level"] = item["risk_level"]


def filter_legacy_generic_flags(item: dict[str, Any]) -> None:
    report = item.setdefault("report", {})
    flags = report.get("red_flags", [])
    if not isinstance(flags, list):
        report["red_flags"] = []
        return
    filtered = []
    for flag in flags:
        if not isinstance(flag, dict):
            continue
        code = str(flag.get("code") or "")
        title = normalize_text(flag.get("title"))
        if code in {"RFVALOR", "RFALTOIMPACTO"}:
            continue
        if code == "RFOBJETO" and flag.get("has_risk") is True and "OBJETO SENSIVEL" in title:
            filtered.append(
                {
                    **flag,
                    "title": "Objeto Elegivel para Comparacao Estatistica",
                    "has_risk": False,
                    "risk_level": "baixo",
                    "message": "Objeto com termo monitorado; a atencao depende de comparacao real por categoria/regiao.",
                    "criteria": {"rule": "termo sensivel encontrado; sem irregularidade presumida sem baseline comparavel"},
                }
            )
            continue
        filtered.append(flag)
    report["red_flags"] = filtered


def adaptive_ml_attention_flags(items: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {item_key(item): [] for item in items}
    if len(items) < 8:
        return output

    groups: dict[tuple[str, str], list[float]] = {}
    supplier_totals: dict[str, float] = {}
    supplier_counts: dict[str, int] = {}
    entity_supplier_dates: dict[tuple[str, str], list[dict[str, Any]]] = {}

    for item in items:
        value = float(item.get("value") or 0)
        uf = str(item.get("uf") or "BR").upper()
        family = family_key(item)
        supplier = "".join(ch for ch in str(item.get("supplier_cnpj") or "") if ch.isdigit()) or normalize_text(item.get("supplier_name"))
        entity = normalize_text(item.get("entity"))
        groups.setdefault((uf, family), []).append(value)
        supplier_totals[supplier] = supplier_totals.get(supplier, 0.0) + value
        supplier_counts[supplier] = supplier_counts.get(supplier, 0) + 1
        entity_supplier_dates.setdefault((entity, supplier), []).append(item)

    supplier_total_values = list(supplier_totals.values())
    supplier_total_avg = mean(supplier_total_values)
    supplier_total_dev = pstdev(supplier_total_values) or 1

    for item in items:
        key = item_key(item)
        value = float(item.get("value") or 0)
        uf = str(item.get("uf") or "BR").upper()
        family = family_key(item)
        comparable = groups.get((uf, family), [])
        if len(comparable) >= 5:
            avg = mean(comparable)
            dev = pstdev(comparable) or 1
            z_score = (value - avg) / dev
            if z_score > 3.0:
                output[key].append(
                    {
                        "code": "ML-GUIDE-01",
                        "title": "Variação Atípica de Preço por Grupo Regional",
                        "has_risk": True,
                        "risk_level": "alto",
                        "message": "Machine learning agrupou contratos semelhantes por UF/família textual e encontrou valor acima de 3 desvios padrão.",
                        "evidence": {
                            "uf": uf,
                            "family": family,
                            "sample_size": len(comparable),
                            "baseline": round(avg, 2),
                            "standard_deviation": round(dev, 2),
                            "z_score": round(z_score, 4),
                            "percent_above_baseline": round(((value - avg) / avg) * 100, 2) if avg else 0,
                        },
                        "criteria": {"rule": "z_score > 3.0 por UF e família textual; aproxima embeddings enquanto pgvector não está ativo"},
                    }
                )

        supplier = "".join(ch for ch in str(item.get("supplier_cnpj") or "") if ch.isdigit()) or normalize_text(item.get("supplier_name"))
        supplier_z = (supplier_totals.get(supplier, 0.0) - supplier_total_avg) / supplier_total_dev
        if supplier_counts.get(supplier, 0) >= 4 and supplier_z > 2.5:
            output[key].append(
                {
                    "code": "ML-NEW-SUPPLIER-CONCENTRATION",
                    "title": "Fator Emergente: Concentração Atípica por Fornecedor",
                    "has_risk": True,
                    "risk_level": "médio",
                    "message": "A plataforma identificou concentração estatisticamente alta de valor e recorrência para o mesmo fornecedor.",
                    "evidence": {
                        "supplier": supplier,
                        "supplier_contracts": supplier_counts.get(supplier, 0),
                        "supplier_total_value": round(supplier_totals.get(supplier, 0.0), 2),
                        "supplier_total_z_score": round(supplier_z, 4),
                    },
                    "criteria": {"rule": "contratos_do_fornecedor >= 4 E z_score_total_fornecedor > 2.5"},
                }
            )

    for (entity, supplier), related in entity_supplier_dates.items():
        if len(related) < 3:
            continue
        related_sorted = sorted(related, key=lambda item: str(item.get("date") or ""))
        for current in related_sorted:
            current_date = parse_date_safe(current.get("date"))
            if not current_date:
                continue
            window = [
                item
                for item in related_sorted
                if parse_date_safe(item.get("date"))
                and 0 <= (current_date - parse_date_safe(item.get("date"))).days <= 60
            ]
            if len(window) >= 3:
                output[item_key(current)].append(
                    {
                        "code": "ML-NEW-RECURRENCE-WINDOW",
                        "title": "Fator Emergente: Recorrência Atípica em Janela Curta",
                        "has_risk": True,
                        "risk_level": "médio",
                        "message": "O mesmo fornecedor aparece repetidamente para o mesmo órgão em intervalo curto.",
                        "evidence": {
                            "entity": entity,
                            "supplier": supplier,
                            "window_days": 60,
                            "contracts_in_window": len(window),
                            "window_total_value": round(sum(float(item.get("value") or 0) for item in window), 2),
                        },
                        "criteria": {"rule": "mesmo fornecedor + mesmo órgão + >= 3 contratos em 60 dias"},
                    }
                )
    return output


def parse_date_safe(value: Any):
    try:
        return datetime.fromisoformat(str(value)).date()
    except Exception:
        return None


def analyze_items(snapshot: dict[str, Any], items: list[dict[str, Any]], connector_records: list[dict[str, Any]]) -> dict[str, Any]:
    anomalies = ml_value_anomalies(items)
    adaptive_flags = adaptive_ml_attention_flags(items)
    alerts = []

    for item in items:
        filter_legacy_generic_flags(item)
        red_flags = item.get("report", {}).get("red_flags", [])
        for flag in adaptive_flags.get(item_key(item), []):
            append_attention_flag(item, flag)
        red_flags = item.get("report", {}).get("red_flags", [])
        high_flags = [flag for flag in red_flags if flag.get("risk_level") == "alto"]
        anomaly = anomalies.get(item.get("id"))
        is_anomaly = bool(anomaly and anomaly.get("is_anomaly"))
        if high_flags or is_anomaly or item.get("risk_score", 0) >= 35:
            alerts.append(
                {
                    "id": item.get("id"),
                    "date": item.get("date"),
                    "title": item.get("title"),
                    "entity": item.get("entity"),
                    "supplier_name": item.get("supplier_name"),
                    "supplier_cnpj": item.get("supplier_cnpj"),
                    "value": item.get("value"),
                    "risk_score": item.get("risk_score"),
                    "risk_level": item.get("risk_level"),
                    "red_flags": red_flags,
                    "ml_analysis": anomaly,
                    "official_sources": item.get("report", {}).get("official_sources", []),
                    "public_evidence": item.get("report", {}).get("public_evidence", []),
                }
            )

    return {
        "generated_at": brasilia_now().isoformat(),
        "pipeline_order": snapshot.get("pipeline_order", []),
        "connectors": snapshot.get("connectors", []),
        "connector_records_count": len(connector_records),
        "items_analyzed": len(items),
        "alerts_count": len(alerts),
        "alerts": alerts,
        "items": items,
        "public_records": connector_records,
        "state_map": snapshot.get("state_map"),
        "collection_errors": snapshot.get("errors", []),
    }


def cached_alerts_from_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    for item in items:
        report = item.get("report", {}) if isinstance(item, dict) else {}
        red_flags = report.get("red_flags", []) if isinstance(report, dict) else []
        high_flags = [
            flag for flag in red_flags
            if isinstance(flag, dict) and flag.get("risk_level") == "alto"
        ]
        if high_flags or item.get("risk_score", 0) >= 35:
            alerts.append(
                {
                    "id": item.get("id"),
                    "date": item.get("date"),
                    "title": item.get("title"),
                    "entity": item.get("entity"),
                    "supplier_name": item.get("supplier_name"),
                    "supplier_cnpj": item.get("supplier_cnpj"),
                    "value": item.get("value"),
                    "risk_score": item.get("risk_score"),
                    "risk_level": item.get("risk_level"),
                    "red_flags": red_flags,
                    "ml_analysis": None,
                    "official_sources": report.get("official_sources", []) if isinstance(report, dict) else [],
                    "public_evidence": report.get("public_evidence", []) if isinstance(report, dict) else [],
                }
            )
    return alerts


def merge_cached_alerts(previous_alerts: list[dict[str, Any]], new_alerts: list[dict[str, Any]], fallback_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for alert in previous_alerts:
        if not isinstance(alert, dict):
            continue
        key = f"{alert.get('id')}:{alert.get('date')}"
        if key.strip(":"):
            merged[key] = alert
    for alert in new_alerts:
        if not isinstance(alert, dict):
            continue
        key = f"{alert.get('id')}:{alert.get('date')}"
        if key.strip(":"):
            merged[key] = alert
    if not merged:
        return cached_alerts_from_items(fallback_items)
    return list(merged.values())


def analyze_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    items = flatten_feed(snapshot)
    connector_records = flatten_public_records(snapshot)
    return analyze_items(snapshot, items, connector_records)


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")


def merge_monitoring_database(paths: dict[str, Path], items: list[dict[str, Any]]) -> int:
    database_path = paths["processed"] / "monitoring_items.json"
    existing: list[dict[str, Any]] = []
    if database_path.exists():
        try:
            loaded = json.loads(database_path.read_text(encoding="utf-8"))
            if isinstance(loaded, list):
                existing = [item for item in loaded if isinstance(item, dict)]
        except Exception:
            existing = []

    by_key: dict[str, dict[str, Any]] = {}
    for item in existing + items:
        item["normalized_title"] = normalize_text(item.get("title"))
        item["normalized_entity"] = normalize_text(item.get("entity"))
        item["normalized_supplier"] = normalize_text(item.get("supplier_name") or item.get("supplier_cnpj"))
        item["coibe_dedup_hash"] = record_hash(item.get("supplier_cnpj"), item.get("date"), item.get("value"))
        key = f"{item.get('id')}:{item.get('date')}"
        if key.strip(":") == "":
            continue
        by_key[key] = item

    merged = list(by_key.values())
    merged.sort(key=lambda item: (str(item.get("date") or ""), str(item.get("id") or "")), reverse=True)
    write_json(database_path, merged)
    return len(merged)


def load_monitoring_database(paths: dict[str, Path]) -> list[dict[str, Any]]:
    database_path = paths["processed"] / "monitoring_items.json"
    if not database_path.exists():
        return []
    try:
        loaded = json.loads(database_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    return [item for item in loaded if isinstance(item, dict)] if isinstance(loaded, list) else []


def load_collection_state(paths: dict[str, Path]) -> dict[str, Any]:
    state_path = paths["state"] / "collector_state.json"
    if not state_path.exists():
        return {"next_feed_page": 1}
    try:
        loaded = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {"next_feed_page": 1}
    return loaded if isinstance(loaded, dict) else {"next_feed_page": 1}


def save_collection_state(paths: dict[str, Path], state: dict[str, Any]) -> None:
    write_json(paths["state"] / "collector_state.json", state)


def merge_public_records_database(paths: dict[str, Path], records: list[dict[str, Any]]) -> int:
    database_path = paths["processed"] / "public_api_records.json"
    existing: list[dict[str, Any]] = []
    if database_path.exists():
        try:
            loaded = json.loads(database_path.read_text(encoding="utf-8"))
            if isinstance(loaded, list):
                existing = [item for item in loaded if isinstance(item, dict)]
        except Exception:
            existing = []

    by_key: dict[str, dict[str, Any]] = {}
    for record in existing + records:
        record["normalized_title"] = normalize_text(record.get("title"))
        record["normalized_source"] = normalize_text(record.get("source"))
        key = str(record.get("record_key") or "")
        if not key:
            continue
        by_key[key] = record

    merged = list(by_key.values())
    merged.sort(key=lambda item: (str(item.get("collected_at") or ""), str(item.get("record_key") or "")), reverse=True)
    write_json(database_path, merged)
    return len(merged)


def public_codes_from_item(item: dict[str, Any]) -> dict[str, str]:
    return {
        "cnpj": "".join(char for char in str(item.get("supplier_cnpj") or "") if char.isdigit()),
        "uf": str(item.get("uf") or ""),
        "city": str(item.get("city") or ""),
        "entity": str(item.get("entity") or ""),
    }


def public_codes_from_record(record: dict[str, Any]) -> dict[str, str]:
    payload = record.get("payload") or {}
    if isinstance(payload, dict) and isinstance(payload.get("payload"), dict):
        payload = payload["payload"]
    return {
        "record_type": str(record.get("record_type") or ""),
        "source": str(record.get("source") or ""),
        "query": str(record.get("query") or ""),
        "url": str(record.get("url") or ""),
        "cnpj": "".join(char for char in str(payload.get("cnpj") or payload.get("niFornecedor") or "") if char.isdigit()),
        "uf": str(payload.get("sigla") or payload.get("uf") or payload.get("siglaUf") or ""),
    }


def build_library_records(
    snapshot: dict[str, Any],
    items: list[dict[str, Any]],
    public_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    collected_at = snapshot.get("collected_at") or brasilia_now().isoformat()
    records: list[dict[str, Any]] = []

    for connector in snapshot.get("connectors", []):
        if not isinstance(connector, dict):
            continue
        records.append(
            {
                "library_key": f"connector:{connector.get('name')}:{connector.get('path')}",
                "library_type": "public_api_connector",
                "collected_at": collected_at,
                "source": connector.get("name"),
                "title": connector.get("name"),
                "url": connector.get("path"),
                "public_codes": {"connector_kind": str(connector.get("kind") or "")},
                "payload": connector,
            }
        )

    for item in items:
        records.append(
            {
                "library_key": f"monitoring:{item.get('id')}:{item.get('date')}",
                "library_type": "monitoring_item",
                "collected_at": collected_at,
                "source": "Compras.gov.br Dados Abertos",
                "title": item.get("title"),
                "url": (item.get("report", {}).get("official_sources") or [{}])[0].get("url"),
                "public_codes": public_codes_from_item(item),
                "payload": item,
            }
        )

    for record in public_records:
        records.append(
            {
                "library_key": f"public:{record.get('record_key')}",
                "library_type": record.get("record_type") or "public_api_record",
                "collected_at": collected_at,
                "source": record.get("source"),
                "title": record.get("title"),
                "url": record.get("url"),
                "public_codes": public_codes_from_record(record),
                "payload": record,
            }
        )

    return records


def update_public_codes(paths: dict[str, Path], library_records: list[dict[str, Any]]) -> int:
    codes_path = paths["library"] / "public_codes.json"
    codes: dict[str, set[str]] = {
        "cnpjs": set(),
        "ufs": set(),
        "cities": set(),
        "entities": set(),
        "sources": set(),
        "record_types": set(),
        "queries": set(),
        "urls": set(),
    }
    if codes_path.exists():
        try:
            loaded = json.loads(codes_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                for key in codes:
                    codes[key].update(str(value) for value in loaded.get(key, []) if value)
        except Exception:
            pass

    for record in library_records:
        public_codes = record.get("public_codes") or {}
        if public_codes.get("cnpj"):
            codes["cnpjs"].add(public_codes["cnpj"])
        if public_codes.get("uf"):
            codes["ufs"].add(public_codes["uf"])
        if public_codes.get("city"):
            codes["cities"].add(public_codes["city"])
        if public_codes.get("entity"):
            codes["entities"].add(public_codes["entity"])
        if record.get("source"):
            codes["sources"].add(str(record["source"]))
        if record.get("library_type"):
            codes["record_types"].add(str(record["library_type"]))
        if public_codes.get("query"):
            codes["queries"].add(public_codes["query"])
        if record.get("url"):
            codes["urls"].add(str(record["url"]))

    serializable = {key: sorted(value) for key, value in codes.items()}
    serializable["updated_at"] = brasilia_now().isoformat()
    write_json(codes_path, serializable)
    return sum(len(value) for key, value in codes.items())


def append_platform_library(paths: dict[str, Path], library_records: list[dict[str, Any]]) -> dict[str, int]:
    library_path = paths["library"] / "library_records.jsonl"
    index_path = paths["library"] / "library_index.json"
    index: dict[str, Any] = {"keys": {}, "total_records": 0}
    if index_path.exists():
        try:
            loaded = json.loads(index_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                index = loaded
                index.setdefault("keys", {})
                index.setdefault("total_records", len(index["keys"]))
        except Exception:
            pass

    added = 0
    now = brasilia_now().isoformat()
    with library_path.open("a", encoding="utf-8") as library_file:
        for record in library_records:
            key = str(record.get("library_key") or "")
            if not key:
                continue
            if key in index["keys"]:
                index["keys"][key]["latest_seen_at"] = now
                continue
            added += 1
            library_id = f"LIB-{index['total_records'] + added:012d}"
            record["library_id"] = library_id
            record["first_seen_at"] = now
            record["latest_seen_at"] = now
            record["normalized_title"] = normalize_text(record.get("title"))
            library_file.write(json.dumps(record, ensure_ascii=False, default=json_default) + "\n")
            index["keys"][key] = {
                "library_id": library_id,
                "library_type": record.get("library_type"),
                "first_seen_at": now,
                "latest_seen_at": now,
            }

    index["total_records"] = len(index["keys"])
    index["updated_at"] = now
    write_json(index_path, index)
    public_codes_count = update_public_codes(paths, library_records)
    return {
        "library_records_count": int(index["total_records"]),
        "library_records_added": added,
        "public_codes_count": public_codes_count,
    }


async def run_once(args: argparse.Namespace, paths: dict[str, Path]) -> None:
    stamp = now_slug()
    state = load_collection_state(paths)
    start_page = max(int(state.get("next_feed_page") or 1), 1)
    snapshot = await collect_snapshot(args.api_base, args.search_terms, args.pages, args.page_size, start_page=start_page)
    raw_path = paths["raw"] / f"snapshot-{stamp}.json"
    write_json(raw_path, snapshot)

    existing_items = load_monitoring_database(paths)
    existing_keys = {item_key(item) for item in existing_items}
    feed_items = flatten_feed(snapshot)
    new_feed_items = [item for item in feed_items if item_key(item) not in existing_keys]
    connector_records = flatten_public_records(snapshot)

    analysis = analyze_items(snapshot, new_feed_items, connector_records)
    new_alerts = list(analysis["alerts"])
    database_count = merge_monitoring_database(paths, analysis["items"])
    public_records_count = merge_public_records_database(paths, analysis["public_records"])
    accumulated_items = load_monitoring_database(paths)
    latest_path = paths["processed"] / "latest_analysis.json"
    previous_alerts: list[dict[str, Any]] = []
    if latest_path.exists():
        try:
            previous_latest = json.loads(latest_path.read_text(encoding="utf-8"))
            if isinstance(previous_latest, dict) and isinstance(previous_latest.get("alerts"), list):
                previous_alerts = [alert for alert in previous_latest["alerts"] if isinstance(alert, dict)]
        except Exception:
            previous_alerts = []
    cached_alerts = merge_cached_alerts(previous_alerts, new_alerts, accumulated_items)
    analysis["items_analyzed"] = len(accumulated_items)
    analysis["items_cached"] = len(existing_items)
    analysis["new_items_analyzed"] = len(new_feed_items)
    analysis["alerts"] = cached_alerts
    analysis["alerts_count"] = len(cached_alerts)
    library_records = build_library_records(snapshot, new_feed_items, analysis["public_records"])
    library_status = append_platform_library(paths, library_records)
    feed_items_collected = sum(
        len(page.get("items") or [])
        for page in snapshot.get("feed_pages", [])
        if isinstance(page, dict)
    )
    feed_pages_failed = sum(
        1
        for connector in snapshot.get("connectors", [])
        if "_feed_page_" in str(connector.get("name") or "")
        and connector.get("status") != "ok"
    )
    next_feed_page = start_page + args.pages
    reset_reason = None
    if feed_items_collected == 0:
        next_feed_page = 1
        reset_reason = "feed publico sem itens neste ciclo; voltando ao inicio da janela"
    analysis["database_items_count"] = database_count
    analysis["database_path"] = str(paths["processed"] / "monitoring_items.json")
    analysis["public_records_count"] = public_records_count
    analysis["public_records_path"] = str(paths["processed"] / "public_api_records.json")
    analysis["library"] = {
        **library_status,
        "library_path": str(paths["library"] / "library_records.jsonl"),
        "index_path": str(paths["library"] / "library_index.json"),
        "public_codes_path": str(paths["library"] / "public_codes.json"),
    }
    analysis["collector_state"] = {
        "feed_page_start": start_page,
        "feed_page_end": start_page + args.pages - 1,
        "next_feed_page": next_feed_page,
        "feed_items_collected": feed_items_collected,
        "new_items_analyzed": len(new_feed_items),
        "items_cached": len(existing_items),
        "feed_pages_failed": feed_pages_failed,
        "reset_reason": reset_reason,
    }
    processed_path = paths["processed"] / f"analysis-{stamp}.json"
    write_json(processed_path, analysis)
    write_json(latest_path, analysis)
    save_collection_state(paths, {"next_feed_page": next_feed_page, "updated_at": analysis["generated_at"]})

    for alert in new_alerts:
        alert_path = paths["alerts"] / f"{stamp}-{safe_filename_part(alert.get('id'))}.json"
        write_json(alert_path, alert)

    log_line = (
        f"{brasilia_now().isoformat()} "
        f"snapshot={raw_path.name} connectors={len(analysis['connectors'])} items={analysis['items_analyzed']} "
        f"db_items={database_count} public_records={public_records_count} "
        f"feed_items={feed_items_collected} new_items={len(new_feed_items)} cached={len(existing_items)} "
        f"feed_errors={feed_pages_failed} next_page={next_feed_page} "
        f"library={library_status['library_records_count']} added={library_status['library_records_added']} "
        f"alerts={analysis['alerts_count']}\n"
    )
    with (paths["logs"] / "monitor.log").open("a", encoding="utf-8") as log:
        log.write(log_line)

    print(log_line.strip())


async def main() -> None:
    parser = argparse.ArgumentParser(description="COIBE.IA collector and risk monitor")
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--interval-minutes", type=float, default=0)
    parser.add_argument("--pages", type=int, default=10)
    parser.add_argument("--page-size", type=int, default=DEFAULT_FEED_PAGE_SIZE)
    parser.add_argument("--startup-delay-seconds", type=float, default=0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--search-terms", nargs="*", default=DEFAULT_SEARCH_TERMS)
    args = parser.parse_args()

    paths = ensure_dirs(Path(args.data_dir))
    if args.startup_delay_seconds > 0:
        await asyncio.sleep(args.startup_delay_seconds)

    while True:
        try:
            await run_once(args, paths)
        except Exception as exc:
            error_line = f"{brasilia_now().isoformat()} ERROR {exc}\n"
            with (paths["logs"] / "monitor.log").open("a", encoding="utf-8") as log:
                log.write(error_line)
            print(error_line.strip())

        if args.once:
            break
        if args.interval_minutes > 0:
            await asyncio.sleep(args.interval_minutes * 60)


if __name__ == "__main__":
    asyncio.run(main())
