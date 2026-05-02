import argparse
import asyncio
import hashlib
import ipaddress
import json
import os
import re
import site
import socket
import subprocess
import unicodedata
import warnings
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from statistics import mean, pstdev
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse
from zoneinfo import ZoneInfo

import httpx

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

warnings.filterwarnings(
    "ignore",
    message=r"CUDA path could not be detected.*",
    category=UserWarning,
    module=r"cupy\._environment",
)


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
    "São Paulo",
    "00000000000191",
]
MODELS_DIR = Path(os.getenv("COIBE_MODELS_DIR", "Models"))
MONITOR_MODEL_STATE_PATH = MODELS_DIR / "monitor_model_state.json"
MONITOR_MODEL_TRAINING_PATH = MODELS_DIR / "monitor_training_history.jsonl"
MONITOR_MODEL_MEMORY_PATH = MODELS_DIR / "coibe_adaptive_memory.jsonl"
MONITOR_MODEL_CONFIG_PATH = MODELS_DIR / "monitor_config.json"
MONITOR_MODEL_REGISTRY_PATH = MODELS_DIR / "model_registry.json"
MONITOR_DEEP_MODEL_PATH = MODELS_DIR / "coibe_adaptive_deep_model.joblib"
MONITOR_DEEP_JSON_MODEL_PATH = MODELS_DIR / "coibe_adaptive_deep_model.ai.json"
MONITOR_ONNX_MANIFEST_PATH = MODELS_DIR / "coibe_adaptive_deep_model.onnx.json"
MONITOR_QUANT_MANIFEST_PATH = MODELS_DIR / "coibe_adaptive_deep_model.quant.json"
ML_USE_GPU = os.getenv("COIBE_ML_USE_GPU", "false").lower() in {"1", "true", "yes"}
GPU_MEMORY_LIMIT_MB = int(os.getenv("COIBE_GPU_MEMORY_LIMIT_MB", "2048"))
USE_SHARED_MEMORY = os.getenv("COIBE_ML_USE_SHARED_MEMORY", "true").lower() in {"1", "true", "yes"}
SHARED_MEMORY_LIMIT_MB = int(os.getenv("COIBE_SHARED_MEMORY_LIMIT_MB", "4096"))
RESEARCH_TIMEOUT_SECONDS = int(os.getenv("COIBE_RESEARCH_TIMEOUT_SECONDS", "90"))
MAX_LEARNED_TERMS_PER_CYCLE = int(os.getenv("COIBE_MODEL_LEARNED_TERMS_PER_CYCLE", "12"))
MONITOR_SEARCH_TERMS_PER_CYCLE = int(os.getenv("COIBE_MONITOR_SEARCH_TERMS_PER_CYCLE", "8"))
MONITOR_SEARCH_DELAY_SECONDS = float(os.getenv("COIBE_MONITOR_SEARCH_DELAY_SECONDS", "2.0"))
MONITOR_PRIORITY_FEED_QUERIES_PER_CYCLE = int(os.getenv("COIBE_MONITOR_PRIORITY_FEED_QUERIES_PER_CYCLE", "4"))
MONITOR_INTERNET_SWEEP_ENABLED = os.getenv("COIBE_MONITOR_INTERNET_SWEEP_ENABLED", "true").lower() in {"1", "true", "yes"}
MONITOR_WEB_FALLBACK_ENABLED = os.getenv("COIBE_MONITOR_WEB_FALLBACK_ENABLED", "true").lower() in {"1", "true", "yes"}
MONITOR_INTERNET_SWEEP_PAGES_PER_CYCLE = int(os.getenv("COIBE_MONITOR_INTERNET_SWEEP_PAGES_PER_CYCLE", "6"))
MONITOR_INTERNET_SWEEP_TIMEOUT_SECONDS = int(os.getenv("COIBE_MONITOR_INTERNET_SWEEP_TIMEOUT_SECONDS", "12"))
MONITOR_SCAN_PROFILE = os.getenv("COIBE_MONITOR_SCAN_PROFILE", "balanced")
MONITOR_MAX_CONCURRENT_REQUESTS = int(os.getenv("COIBE_MONITOR_MAX_CONCURRENT_REQUESTS", "4"))
MONITOR_PUBLIC_API_CONCURRENCY = int(os.getenv("COIBE_MONITOR_PUBLIC_API_CONCURRENCY", "3"))
MONITOR_PUBLIC_API_SOURCE_MODE = os.getenv("COIBE_MONITOR_PUBLIC_API_SOURCE_MODE", "hybrid")
MONITOR_INTERNET_SWEEP_CONCURRENCY = int(os.getenv("COIBE_MONITOR_INTERNET_SWEEP_CONCURRENCY", "4"))
MONITOR_GPU_ACCELERATION_LEVEL = os.getenv("COIBE_MONITOR_GPU_ACCELERATION_LEVEL", "balanced")
MONITOR_TRAINING_SAMPLE_LIMIT = int(os.getenv("COIBE_MONITOR_TRAINING_SAMPLE_LIMIT", "1000"))
MONITOR_ANALYSIS_BATCH_SIZE = int(os.getenv("COIBE_MONITOR_ANALYSIS_BATCH_SIZE", "256"))
MONITOR_LEARNING_BATCH_SIZE = int(os.getenv("COIBE_MONITOR_LEARNING_BATCH_SIZE", "512"))
MONITOR_SELECTED_MODEL_ID = os.getenv("COIBE_MONITOR_SELECTED_MODEL_ID", "coibe-adaptive-default")
MONITOR_DEEP_LEARNING_ENABLED = os.getenv("COIBE_MONITOR_DEEP_LEARNING_ENABLED", "true").lower() in {"1", "true", "yes"}
MONITOR_QUANTIZATION_MODE = os.getenv("COIBE_MONITOR_QUANTIZATION_MODE", "dynamic-int8")
POLITICAL_PARTY_SCAN_LIMIT = int(os.getenv("COIBE_POLITICAL_PARTY_SCAN_LIMIT", "12"))
POLITICAL_PEOPLE_SCAN_LIMIT = int(os.getenv("COIBE_POLITICAL_PEOPLE_SCAN_LIMIT", "24"))
CUDA_DLL_HANDLES: list[Any] = []
LOW_PRIORITY_REPEATED_TERMS = {"TIRIRICA"}
PUBLIC_WEB_SEED_URLS = [
    "https://www.gov.br/cgu/pt-br",
    "https://portal.tcu.gov.br",
    "https://www.gov.br/pncp/pt-br",
    "https://www.open-contracting.org/resources/red-flags-integrity-giving-green-light-open-data-solutions/",
]

HIGH_RISK_FEED_QUERIES = [
    "superfaturamento",
    "sobrepreco",
    "dispensa licitacao",
    "contrato emergencial",
    "fornecedor recorrente",
    "valor unitario",
    "merenda",
    "combustivel",
    "pavimentacao",
]

BOOTSTRAP_MODEL_TERMS = [
    "single bidder contract",
    "short tender period",
    "contract amendment cost overrun",
    "delivery delay contract execution",
    "supplier concentration buyer spending",
    "beneficial ownership supplier",
    "blacklisted supplier sanctions",
    "CEIS CNEP fornecedor sancionado",
    "PNCP contratos dados abertos",
    "Portal da Transparencia despesas contratos",
    "TCU dados abertos acordaos controle externo",
    "CGU integridade publica dados abertos",
    "Open Contracting red flags procurement",
    "OCDS planning tender award contract implementation",
    "Benford law procurement values",
    "threshold splitting procurement",
    "political connections campaign financing",
    "network anomaly supplier public official",
    "positive unlabeled learning sanctioned suppliers",
    "graph fraud procurement contracts",
    "quasi real time procurement red flags",
    "price reference public procurement",
    "supplier tax haven registry",
    "unrealistic procurement timeline",
    "single source tender risk",
    "unjustified cost increase",
    "repeated emergency procurement",
    "related supplier repeated payments",
    "campaign donation supplier contract",
    "family proximity public money flow",
]


def rotate_list(values: list[str], offset: int) -> list[str]:
    if not values:
        return []
    start = max(int(offset or 0), 0) % len(values)
    return [*values[start:], *values[:start]]


def batched(values: list[Any], batch_size: int) -> list[list[Any]]:
    size = max(1, int(batch_size or 1))
    return [values[index:index + size] for index in range(0, len(values), size)]


def now_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def brasilia_now() -> datetime:
    return datetime.now(BRASILIA_TZ)


def monitor_print(message: str) -> None:
    print(f"[{brasilia_now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def records_per_second(records: int, elapsed_seconds: float) -> float:
    if elapsed_seconds <= 0:
        return float(records)
    return round(records / elapsed_seconds, 2)


def prepare_cuda_dll_paths() -> None:
    if os.name != "nt":
        return
    candidates: list[Path] = []
    for base in site.getsitepackages():
        site_path = Path(base)
        candidates.extend(
            [
                site_path / "nvidia" / "cuda_nvrtc" / "bin",
                site_path / "nvidia" / "cuda_runtime" / "bin",
            ]
        )
    for candidate in candidates:
        if not candidate.exists():
            continue
        candidate_text = str(candidate)
        if candidate_text.lower() not in os.environ.get("PATH", "").lower():
            os.environ["PATH"] = candidate_text + os.pathsep + os.environ.get("PATH", "")
        try:
            CUDA_DLL_HANDLES.append(os.add_dll_directory(candidate_text))
        except Exception:
            pass


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


def stable_hash(parts: list[Any]) -> str:
    raw = "|".join(normalize_text(part) for part in parts)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def is_coibe_source_label(value: Any) -> bool:
    return "COIBE" in normalize_text(value)


def public_source_label_from_url(url: str, record_type: str = "", fallback: str = "Dados publicos relacionados") -> str:
    parsed = urlparse(str(url or ""))
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    normalized_type = normalize_text(record_type).lower()
    if "dadosabertos.compras.gov.br" in host or "compras.gov.br" in host:
        return "Compras.gov.br Dados Abertos"
    if "pncp.gov.br" in host:
        return "PNCP - Portal Nacional de Contratacoes Publicas"
    if "camara.leg.br" in host:
        return "Camara dos Deputados - partidos e membros" if "partid" in normalized_type or "partidos" in path else "Camara dos Deputados - Dados Abertos"
    if "senado.leg.br" in host:
        return "Senado Federal - Dados Abertos"
    if "stf.jus.br" in host:
        return "STF - consulta publica"
    if "tse.jus.br" in host or "divulgacandcontas" in host:
        return "TSE - dados eleitorais oficiais"
    if "tcu.gov.br" in host:
        return "TCU - pesquisa de processos"
    if "portaldatransparencia.gov.br" in host:
        return "Portal da Transparencia - CGU"
    if "ibge.gov.br" in host:
        return "IBGE - dados publicos"
    return fallback if not is_coibe_source_label(fallback) else "Dados publicos relacionados"


def source_url_from_context(context: dict[str, Any]) -> str:
    for key in ("url", "document_url", "api_url"):
        value = context.get(key)
        if value:
            return str(value)
    return ""


def sanitize_source_text(value: Any, context: dict[str, Any] | None = None, record_type: str = "") -> str:
    text = str(value or "").strip()
    if not is_coibe_source_label(text):
        return text
    url = source_url_from_context(context or {})
    return public_source_label_from_url(url, record_type, "Dados publicos relacionados")


def sanitize_source_list(values: Any, context: dict[str, Any] | None = None, record_type: str = "") -> list[Any]:
    if not isinstance(values, list):
        return []
    output: list[Any] = []
    seen: set[str] = set()
    for value in values:
        sanitized = sanitize_source_text(value, context, record_type) if isinstance(value, str) else value
        key = json.dumps(sanitized, ensure_ascii=False, sort_keys=True, default=json_default)
        if key in seen:
            continue
        seen.add(key)
        output.append(sanitized)
    return output


def sanitize_public_evidence_record(evidence: dict[str, Any]) -> dict[str, Any]:
    sanitized = dict(evidence)
    record_type = str(sanitized.get("record_type") or sanitized.get("type") or "")
    if "source" in sanitized:
        sanitized["source"] = sanitize_source_text(sanitized.get("source"), sanitized, record_type)
    if isinstance(sanitized.get("sources"), list):
        sanitized["sources"] = sanitize_source_list(sanitized.get("sources"), sanitized, record_type)
    return sanitized


def sanitize_nested_source_fields(value: Any, context: dict[str, Any] | None = None, record_type: str = "") -> Any:
    if isinstance(value, dict):
        next_value: dict[str, Any] = {}
        next_context = {**(context or {}), **value}
        for key, item in value.items():
            if key in {"source", "label", "kind"} and isinstance(item, str):
                next_value[key] = sanitize_source_text(item, next_context, record_type)
            elif key == "sources" and isinstance(item, list):
                next_value[key] = sanitize_source_list(item, next_context, record_type)
            else:
                next_value[key] = sanitize_nested_source_fields(item, next_context, record_type)
        return next_value
    if isinstance(value, list):
        return [sanitize_nested_source_fields(item, context, record_type) for item in value]
    return value


def sanitize_official_sources(sources: Any) -> list[dict[str, Any]]:
    if not isinstance(sources, list):
        return []
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for source in sources:
        if not isinstance(source, dict):
            continue
        sanitized = dict(source)
        url = str(sanitized.get("url") or "")
        record_type = str(sanitized.get("kind") or "")
        sanitized["label"] = sanitize_source_text(sanitized.get("label"), sanitized, record_type) or public_source_label_from_url(url, record_type)
        sanitized["kind"] = sanitize_source_text(sanitized.get("kind"), sanitized, record_type) or "fonte_publica"
        key = "|".join([str(sanitized.get("url") or ""), str(sanitized.get("label") or ""), str(sanitized.get("kind") or "")])
        if not key.strip("|") or key in seen:
            continue
        seen.add(key)
        output.append(sanitized)
    return output


def sanitize_public_evidence_list(values: Any) -> list[dict[str, Any]]:
    if not isinstance(values, list):
        return []
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for evidence in values:
        if not isinstance(evidence, dict):
            continue
        sanitized = sanitize_public_evidence_record(evidence)
        key = stable_hash([
            sanitized.get("url") or sanitized.get("document_url"),
            sanitized.get("record_type") or sanitized.get("type"),
            sanitized.get("title"),
            sanitized.get("value"),
            sanitized.get("estimated_variation"),
        ])
        if key in seen:
            continue
        seen.add(key)
        output.append(sanitized)
    return output


def sanitize_monitoring_item_record(item: dict[str, Any]) -> dict[str, Any]:
    sanitized = dict(item)
    report = dict(sanitized.get("report")) if isinstance(sanitized.get("report"), dict) else {}
    report["official_sources"] = sanitize_official_sources(report.get("official_sources"))
    report["public_evidence"] = sanitize_public_evidence_list(report.get("public_evidence"))
    red_flags: list[dict[str, Any]] = []
    for flag in report.get("red_flags", []) if isinstance(report.get("red_flags"), list) else []:
        if not isinstance(flag, dict):
            continue
        next_flag = dict(flag)
        if isinstance(next_flag.get("evidence"), dict):
            next_flag["evidence"] = sanitize_public_evidence_record(next_flag["evidence"])
        red_flags.append(next_flag)
    report["red_flags"] = red_flags
    sanitized["report"] = report
    sanitized["public_evidence"] = sanitize_public_evidence_list(sanitized.get("public_evidence"))

    related_label = ""
    if report["official_sources"]:
        related_label = str(report["official_sources"][0].get("label") or "")
    if not related_label:
        related_label = next((str(evidence.get("source") or "") for evidence in report["public_evidence"] if evidence.get("source")), "")
    related_label = related_label or "Dados publicos relacionados"
    for key in ("source", "entity", "location"):
        if key in sanitized and is_coibe_source_label(sanitized.get(key)):
            sanitized[key] = related_label

    insights: list[dict[str, Any]] = []
    for insight in sanitized.get("agent_insights", []) if isinstance(sanitized.get("agent_insights"), list) else []:
        if not isinstance(insight, dict):
            continue
        next_insight = dict(insight)
        evidence = dict(next_insight.get("evidence")) if isinstance(next_insight.get("evidence"), dict) else {}
        if isinstance(evidence.get("sources"), list):
            evidence["sources"] = sanitize_source_list(evidence.get("sources"), evidence)
        if "source" in evidence:
            evidence["source"] = sanitize_source_text(evidence.get("source"), evidence)
        next_insight["evidence"] = evidence
        insights.append(next_insight)
    if insights:
        sanitized["agent_insights"] = insights
    return sanitized


def sanitize_public_record_row(record: dict[str, Any]) -> dict[str, Any]:
    sanitized = dict(record)
    record_type = str(sanitized.get("record_type") or "")
    sanitized["source"] = sanitize_source_text(sanitized.get("source"), sanitized, record_type)
    payload = dict(sanitized.get("payload")) if isinstance(sanitized.get("payload"), dict) else sanitized.get("payload")
    if isinstance(payload, dict):
        payload = sanitize_nested_source_fields(payload, sanitized, record_type)
        if "source" in payload:
            payload["source"] = sanitize_source_text(payload.get("source"), payload, record_type)
        if isinstance(payload.get("sources"), list):
            payload["sources"] = sanitize_official_sources(payload.get("sources"))
        if isinstance(payload.get("item"), dict):
            payload["item"] = sanitize_monitoring_item_record(payload["item"])
        if isinstance(payload.get("evidence"), dict):
            payload["evidence"] = sanitize_public_evidence_record(payload["evidence"])
        sanitized["payload"] = payload
    return sanitized


def monitoring_case_dedup_key(item: dict[str, Any]) -> str:
    report = item.get("report") if isinstance(item.get("report"), dict) else {}
    urls: list[str] = []
    for source in report.get("official_sources") if isinstance(report.get("official_sources"), list) else []:
        if isinstance(source, dict) and source.get("url"):
            urls.append(str(source["url"]))
    for evidence in report.get("public_evidence") if isinstance(report.get("public_evidence"), list) else []:
        if isinstance(evidence, dict) and (evidence.get("url") or evidence.get("document_url")):
            urls.append(str(evidence.get("url") or evidence.get("document_url")))
    for url in urls:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        contract_keys = [
            query.get("idCompra", [""])[0],
            query.get("numeroContrato", [""])[0],
            query.get("niFornecedor", [""])[0],
            query.get("codigoUnidadeGestora", [""])[0],
            query.get("dataVigenciaInicialMin", [""])[0],
            query.get("dataVigenciaInicialMax", [""])[0],
        ]
        if any(contract_keys) and ("compras.gov.br" in parsed.netloc.lower() or "pncp.gov.br" in parsed.netloc.lower()):
            return stable_hash(["case-public-url", parsed.netloc.lower(), parsed.path, *contract_keys])
    value = normalize_decimal(item.get("value"))
    variation = normalize_decimal(item.get("estimated_variation"))
    title = str(item.get("title") or "")
    political_match = re.match(r"Risco de Superfaturamento\s*-\s*(.*?)\s*-\s*R\$", title, flags=re.IGNORECASE)
    if political_match and Decimal(value) > 0:
        return stable_hash(["case-political-money", political_match.group(1), item.get("date"), value, variation])
    supplier = "".join(char for char in str(item.get("supplier_cnpj") or "") if char.isdigit())
    return stable_hash(["case", supplier, item.get("date"), value, normalize_text(item.get("entity"))[:80], normalize_text(item.get("object") or item.get("title"))[:120]])


def merge_monitoring_case_records(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    existing_value = Decimal(normalize_decimal(existing.get("value")))
    incoming_value = Decimal(normalize_decimal(incoming.get("value")))
    existing_variation = Decimal(normalize_decimal(existing.get("estimated_variation")))
    incoming_variation = Decimal(normalize_decimal(incoming.get("estimated_variation")))
    primary = incoming if (incoming_variation, incoming_value, int(incoming.get("risk_score") or 0)) >= (existing_variation, existing_value, int(existing.get("risk_score") or 0)) else existing
    secondary = existing if primary is incoming else incoming
    merged = dict(primary)
    primary_report = primary.get("report") if isinstance(primary.get("report"), dict) else {}
    secondary_report = secondary.get("report") if isinstance(secondary.get("report"), dict) else {}
    merged_report = dict(primary_report)
    for list_key in ("red_flags", "official_sources", "public_evidence"):
        combined: list[Any] = []
        seen: set[str] = set()
        for source_report in (primary_report, secondary_report):
            values = source_report.get(list_key) if isinstance(source_report.get(list_key), list) else []
            for value_item in values:
                key = json.dumps(value_item, ensure_ascii=False, sort_keys=True, default=json_default)
                if key in seen:
                    continue
                seen.add(key)
                combined.append(value_item)
        merged_report[list_key] = combined
    merged["report"] = merged_report
    merged["coibe_merged_case_ids"] = sorted(set([*(existing.get("coibe_merged_case_ids") or []), str(existing.get("id") or ""), str(incoming.get("id") or "")]) - {""})
    return sanitize_monitoring_item_record(merged)


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
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    return paths


def load_monitor_config() -> dict[str, Any]:
    config = {
        "use_gpu": ML_USE_GPU,
        "gpu_memory_limit_mb": GPU_MEMORY_LIMIT_MB,
        "use_shared_memory": USE_SHARED_MEMORY,
        "shared_memory_limit_mb": SHARED_MEMORY_LIMIT_MB,
        "scan_profile": MONITOR_SCAN_PROFILE,
        "max_concurrent_requests": MONITOR_MAX_CONCURRENT_REQUESTS,
        "public_api_concurrency": MONITOR_PUBLIC_API_CONCURRENCY,
        "public_api_source_mode": MONITOR_PUBLIC_API_SOURCE_MODE,
        "internet_sweep_concurrency": MONITOR_INTERNET_SWEEP_CONCURRENCY,
        "gpu_acceleration_level": MONITOR_GPU_ACCELERATION_LEVEL,
        "training_sample_limit": MONITOR_TRAINING_SAMPLE_LIMIT,
        "analysis_batch_size": MONITOR_ANALYSIS_BATCH_SIZE,
        "learning_batch_size": MONITOR_LEARNING_BATCH_SIZE,
        "research_timeout_seconds": RESEARCH_TIMEOUT_SECONDS,
        "research_rounds": None,
        "feed_page_size": None,
        "political_party_scan_limit": POLITICAL_PARTY_SCAN_LIMIT,
        "political_people_scan_limit": POLITICAL_PEOPLE_SCAN_LIMIT,
        "learned_terms_per_cycle": MAX_LEARNED_TERMS_PER_CYCLE,
        "search_terms_per_cycle": MONITOR_SEARCH_TERMS_PER_CYCLE,
        "search_delay_seconds": MONITOR_SEARCH_DELAY_SECONDS,
        "priority_feed_queries_per_cycle": MONITOR_PRIORITY_FEED_QUERIES_PER_CYCLE,
        "internet_sweep_enabled": MONITOR_INTERNET_SWEEP_ENABLED,
        "web_fallback_enabled": MONITOR_WEB_FALLBACK_ENABLED,
        "internet_sweep_pages_per_cycle": MONITOR_INTERNET_SWEEP_PAGES_PER_CYCLE,
        "internet_sweep_timeout_seconds": MONITOR_INTERNET_SWEEP_TIMEOUT_SECONDS,
        "selected_model_id": MONITOR_SELECTED_MODEL_ID,
        "deep_learning_enabled": MONITOR_DEEP_LEARNING_ENABLED,
        "quantization_mode": MONITOR_QUANTIZATION_MODE,
        "model_format": "joblib+onnx-manifest+quant-manifest",
    }
    if MONITOR_MODEL_CONFIG_PATH.exists():
        try:
            loaded = json.loads(MONITOR_MODEL_CONFIG_PATH.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                config.update({key: value for key, value in loaded.items() if key in config and value is not None})
        except Exception:
            pass
    return config


def effective_monitor_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    effective = dict(config or load_monitor_config())
    profile = str(effective.get("scan_profile") or "balanced").strip().lower()
    if profile not in {"conservative", "balanced", "heavy", "no-delay-heavy"}:
        profile = "balanced"
    effective["scan_profile"] = profile

    def int_value(name: str, fallback: int, minimum: int, maximum: int) -> int:
        try:
            value = int(effective.get(name) if effective.get(name) is not None else fallback)
        except Exception:
            value = fallback
        return max(minimum, min(maximum, value))

    def float_value(name: str, fallback: float, minimum: float, maximum: float) -> float:
        try:
            value = float(effective.get(name) if effective.get(name) is not None else fallback)
        except Exception:
            value = fallback
        return max(minimum, min(maximum, value))

    effective["max_concurrent_requests"] = int_value("max_concurrent_requests", MONITOR_MAX_CONCURRENT_REQUESTS, 1, 24)
    effective["public_api_concurrency"] = int_value("public_api_concurrency", MONITOR_PUBLIC_API_CONCURRENCY, 1, 8)
    effective["internet_sweep_concurrency"] = int_value("internet_sweep_concurrency", MONITOR_INTERNET_SWEEP_CONCURRENCY, 1, 24)
    effective["training_sample_limit"] = int_value("training_sample_limit", MONITOR_TRAINING_SAMPLE_LIMIT, 100, 10000)
    effective["analysis_batch_size"] = int_value("analysis_batch_size", MONITOR_ANALYSIS_BATCH_SIZE, 16, 5000)
    effective["learning_batch_size"] = int_value("learning_batch_size", MONITOR_LEARNING_BATCH_SIZE, 16, 10000)
    effective["search_delay_seconds"] = float_value("search_delay_seconds", MONITOR_SEARCH_DELAY_SECONDS, 0.0, 60.0)
    effective["priority_feed_delay_seconds"] = 0.5

    if profile == "conservative":
        effective["max_concurrent_requests"] = min(effective["max_concurrent_requests"], 2)
        effective["public_api_concurrency"] = min(effective["public_api_concurrency"], 1)
        effective["internet_sweep_concurrency"] = min(effective["internet_sweep_concurrency"], 2)
        effective["search_delay_seconds"] = max(effective["search_delay_seconds"], 3.0)
        effective["priority_feed_delay_seconds"] = 0.75
    elif profile == "heavy":
        effective["max_concurrent_requests"] = max(effective["max_concurrent_requests"], 8)
        effective["public_api_concurrency"] = min(max(effective["public_api_concurrency"], 3), 6)
        effective["internet_sweep_concurrency"] = max(effective["internet_sweep_concurrency"], 8)
        effective["search_delay_seconds"] = min(effective["search_delay_seconds"], 0.25)
        effective["priority_feed_delay_seconds"] = 0.1
        effective["training_sample_limit"] = max(effective["training_sample_limit"], 2000)
        effective["analysis_batch_size"] = max(effective["analysis_batch_size"], 512)
        effective["learning_batch_size"] = max(effective["learning_batch_size"], 1024)
    elif profile == "no-delay-heavy":
        effective["max_concurrent_requests"] = max(effective["max_concurrent_requests"], 12)
        effective["public_api_concurrency"] = min(max(effective["public_api_concurrency"], 4), 6)
        effective["internet_sweep_concurrency"] = max(effective["internet_sweep_concurrency"], 12)
        effective["search_delay_seconds"] = 0.0
        effective["priority_feed_delay_seconds"] = 0.0
        effective["training_sample_limit"] = max(effective["training_sample_limit"], 3000)
        effective["analysis_batch_size"] = max(effective["analysis_batch_size"], 1024)
        effective["learning_batch_size"] = max(effective["learning_batch_size"], 2048)

    gpu_level = str(effective.get("gpu_acceleration_level") or "balanced").strip().lower()
    if gpu_level not in {"off", "balanced", "aggressive"}:
        gpu_level = "balanced"
    effective["gpu_acceleration_level"] = gpu_level
    source_mode = str(effective.get("public_api_source_mode") or MONITOR_PUBLIC_API_SOURCE_MODE or "hybrid").strip().lower()
    if source_mode not in {"live", "hybrid", "cache-first"}:
        source_mode = "hybrid"
    effective["public_api_source_mode"] = source_mode
    return effective


def gpu_runtime_status(config: dict[str, Any] | None = None) -> dict[str, Any]:
    config = config or load_monitor_config()
    prepare_cuda_dll_paths()
    status = {
        "enabled_by_env": ML_USE_GPU,
        "enabled_by_config": bool(config.get("use_gpu")),
        "enabled": bool(config.get("use_gpu")),
        "memory_limit_mb": int(config.get("gpu_memory_limit_mb") or GPU_MEMORY_LIMIT_MB),
        "shared_memory_enabled": bool(config.get("use_shared_memory")),
        "shared_memory_limit_mb": int(config.get("shared_memory_limit_mb") or SHARED_MEMORY_LIMIT_MB),
        "available": False,
        "name": None,
        "memory_total_mb": None,
        "gpu_library": None,
        "gpu_runtime_ready": False,
    }
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader,nounits"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
        if result.returncode == 0 and result.stdout.strip():
            name, _, memory = result.stdout.strip().splitlines()[0].partition(",")
            status.update(
                {
                    "available": True,
                    "name": name.strip(),
                    "memory_total_mb": int(memory.strip()) if memory.strip().isdigit() else None,
                }
            )
    except Exception:
        pass
    try:
        import cupy

        status["gpu_library"] = f"cupy {cupy.__version__}"
        probe = cupy.asarray([1, 2, 3], dtype=cupy.float32)
        status["gpu_runtime_ready"] = bool(float(cupy.sum(probe).get()) == 6.0)
    except Exception:
        pass
    return status


def configure_gpu_runtime(config: dict[str, Any] | None = None) -> dict[str, Any]:
    config = config or load_monitor_config()
    prepare_cuda_dll_paths()
    os.environ.setdefault("TF_FORCE_GPU_ALLOW_GROWTH", "true")
    gpu_limit = int(config.get("gpu_memory_limit_mb") or GPU_MEMORY_LIMIT_MB)
    shared_limit = int(config.get("shared_memory_limit_mb") or SHARED_MEMORY_LIMIT_MB)
    if bool(config.get("use_gpu")) and gpu_limit > 0:
        os.environ["COIBE_GPU_MEMORY_LIMIT_MB"] = str(gpu_limit)
        os.environ["RAPIDS_MEMORY_LIMIT"] = str(gpu_limit * 1024 * 1024)
        os.environ["PYTORCH_CUDA_ALLOC_CONF"] = f"max_split_size_mb:{min(gpu_limit, 1024)}"
        try:
            import cupy

            cupy.get_default_memory_pool().set_limit(size=gpu_limit * 1024 * 1024)
        except Exception:
            pass
    if bool(config.get("use_shared_memory")) and shared_limit > 0:
        os.environ["COIBE_SHARED_MEMORY_LIMIT_MB"] = str(shared_limit)
        os.environ.setdefault("JOBLIB_TEMP_FOLDER", str(MODELS_DIR / "shared_memory_cache"))
        (MODELS_DIR / "shared_memory_cache").mkdir(parents=True, exist_ok=True)
    return gpu_runtime_status(config)


def load_monitor_model_state() -> dict[str, Any]:
    empty_state = {"version": "coibe-monitor-v1", "cycles": 0, "learned_terms": [], "learned_checks": [], "updated_at": None}
    if not MONITOR_MODEL_STATE_PATH.exists():
        state = empty_state
    else:
        try:
            loaded = json.loads(MONITOR_MODEL_STATE_PATH.read_text(encoding="utf-8-sig"))
        except Exception:
            loaded = empty_state
        state = loaded if isinstance(loaded, dict) else empty_state

    learned_terms = state.get("learned_terms") if isinstance(state.get("learned_terms"), list) else []
    if learned_terms:
        return state

    latest_path = Path("data/processed/latest_analysis.json")
    if latest_path.exists():
        try:
            latest = json.loads(latest_path.read_text(encoding="utf-8-sig"))
            recovered = latest.get("model") if isinstance(latest, dict) and isinstance(latest.get("model"), dict) else {}
            recovered_terms = recovered.get("learned_terms") if isinstance(recovered.get("learned_terms"), list) else []
            if recovered_terms:
                recovered_state = {**empty_state, **recovered}
                MODELS_DIR.mkdir(parents=True, exist_ok=True)
                MONITOR_MODEL_STATE_PATH.write_text(json.dumps(recovered_state, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")
                return recovered_state
        except Exception:
            pass
    return state


def merged_search_terms(default_terms: list[str], model_state: dict[str, Any]) -> list[str]:
    learned = model_state.get("learned_terms") if isinstance(model_state.get("learned_terms"), list) else []
    learned_checks = model_state.get("learned_checks") if isinstance(model_state.get("learned_checks"), list) else []
    learned_targets = model_state.get("learned_targets") if isinstance(model_state.get("learned_targets"), list) else []
    learned_terms = [
        str(term.get("term") or "").strip()
        for term in sorted(learned, key=lambda item: (float(item.get("score") or 0), int(item.get("hits") or 0)), reverse=True)
        if isinstance(term, dict)
        and str(term.get("term") or "").strip()
        and len(normalize_text(term.get("term")).split()) >= 2
        and candidate_terms_from_text(term.get("term"))
    ]
    learned_check_terms = []
    for check in sorted(learned_checks, key=lambda item: (float(item.get("score") or 0), int(item.get("hits") or 0)), reverse=True):
        if not isinstance(check, dict):
            continue
        for hint in check.get("query_hints", [])[:2] if isinstance(check.get("query_hints"), list) else []:
            clean_hint = str(hint or "").strip()
            if clean_hint:
                learned_check_terms.append(clean_hint)
    learned_target_terms = [
        str(target.get("query") or target.get("term") or "").strip()
        for target in sorted(learned_targets, key=lambda item: (float(item.get("score") or 0), int(item.get("hits") or 0)), reverse=True)
        if isinstance(target, dict) and str(target.get("query") or target.get("term") or "").strip()
    ]
    output: list[str] = []
    seen: set[str] = set()
    for term in [*default_terms, *learned_terms[:20], *learned_target_terms[:16], *learned_check_terms[:12]]:
        normalized = normalize_text(term)
        if normalized in LOW_PRIORITY_REPEATED_TERMS:
            continue
        if normalized and normalized not in seen:
            seen.add(normalized)
            output.append(term)
    return output[:60]


def priority_feed_queries(search_terms: list[str], limit: int) -> list[str]:
    if limit <= 0:
        return []
    risk_words = {
        "SUPERFATURAMENTO", "SOBREPRECO", "SOBRE PRECO", "DISPENSA", "EMERGENCIAL",
        "FORNECEDOR", "RECORRENTE", "VALOR UNITARIO", "MERENDA", "COMBUSTIVEL",
        "PAVIMENTACAO", "ASFALTO", "MEDICAMENTO",
    }
    candidates = [*HIGH_RISK_FEED_QUERIES]
    for term in search_terms:
        normalized = normalize_text(term)
        if any(word in normalized for word in risk_words):
            candidates.append(term)
    output: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        clean = str(candidate or "").strip()
        normalized = normalize_text(clean)
        if len(normalized) < 3 or normalized in seen:
            continue
        seen.add(normalized)
        output.append(clean)
        if len(output) >= limit:
            break
    return output


def candidate_terms_from_text(value: Any) -> list[str]:
    text = normalize_text(value)
    stopwords = {
        "CONTRATO", "CONTRATOS", "CONTRATACAO", "CONTRATACOES", "AQUISICAO", "AQUISICOES",
        "SERVICO", "SERVICOS", "PUBLICO", "PUBLICA", "PUBLICOS", "PUBLICAS", "PARA", "COM",
        "DOS", "DAS", "DE", "DA", "DO", "EM", "E", "LTDA", "EIRELI", "S/A", "SA", "ME",
        "MUNICIPIO", "PREFEITURA", "GOVERNO", "FEDERAL", "ESTADUAL", "RISCOS", "RISCO",
        "POSSIVEL", "POTENCIAL", "SUPERFATURAMENTO", "COMPARACAO", "VALORES", "ALTO",
        "MEDIO", "BAIXO", "ATENCAO", "ANALISE", "REGISTRO", "REGISTROS",
        "COMERCIO", "BRASIL", "OBJETO", "PRESENTE", "INSTRUMENTO", "EMPRESA", "ESPECIALIZADA",
        "DESPESA", "DESPESAS", "NACIONAL", "REGIONAL", "HOSPITAL", "UNIVERSIDADE", "INST",
        "FORNECIMENTO", "MATERIAL", "MATERIAIS", "ADMINISTRATIVO", "ADMINISTRATIVA",
        "VIAGEM", "VIAGENS", "DESLOCAMENTO", "DESLOCAMENTOS", "PARTIDO", "POLITICO",
        "POLITICA", "DEPUTADO", "DEPUTADOS", "PARLAMENTAR", "PARLAMENTARES", "ANALISADO",
        "ANALISADOS", "LIGADO", "LIGADOS", "RECORTE", "PAGAMENTO", "PAGAMENTOS",
        "ATUAL", "MANDATO", "FORA", "EXERCICIO", "NESTE", "ANTERIOR", "CADASTRO",
        "NOME", "INCLUIDO", "CRUZAMENTO", "PREVENTIVO", "FONTES", "LEGAIS", "ELEITORAIS",
        "CONTROLE", "REPUBLICA",
        "SEREM", "EXECUTADO", "EXECUTADOS", "REGIME", "DEDICACAO",
    }
    tokens = [token for token in text.split() if len(token) >= 4 and token not in stopwords]
    phrases = []
    for size in (3, 2):
        for index in range(0, max(0, len(tokens) - size + 1)):
            phrase = " ".join(tokens[index : index + size])
            if len(phrase) >= 8:
                phrases.append(phrase.title())
    phrases.extend(token.title() for token in tokens[:8])
    return phrases[:12]


def add_model_candidates(candidates: dict[str, dict[str, Any]], source_value: Any, weight: float, source: str) -> None:
    for term in candidate_terms_from_text(source_value):
        normalized = normalize_text(term)
        if not normalized:
            continue
        if len(normalized.split()) < 2:
            continue
        bucket = candidates.setdefault(normalized, {"term": term, "score": 0, "sources": set()})
        bucket["score"] += weight
        bucket["sources"].add(source)


def seed_model_candidate(candidates: dict[str, dict[str, Any]], term: str, weight: float, source: str) -> None:
    clean = " ".join(str(term or "").split())
    normalized = normalize_text(clean)
    if len(normalized.split()) < 2:
        return
    bucket = candidates.setdefault(normalized, {"term": clean, "score": 0, "sources": set()})
    bucket["score"] += weight
    bucket["sources"].add(source)


VERIFICATION_CHECK_LIBRARY: dict[str, dict[str, Any]] = {
    "reference_price_superpricing": {
        "title": "Comparar preco unitario com referencia de mercado",
        "description": "Prioriza contratos com termos de itens precificaveis e valores altos para comparar com referencias internas e pares.",
        "query_hints": ["superfaturamento preco referencia", "valor unitario contrato"],
        "signals": ["superfaturamento", "valor acima da referencia", "comparacao de preco"],
    },
    "supplier_concentration": {
        "title": "Checar concentracao por fornecedor",
        "description": "Agrupa fornecedor, CNPJ, orgao e periodo para encontrar repeticao de pagamentos ou dependencia concentrada.",
        "query_hints": ["fornecedor concentrado pagamentos", "cnpj contratos repetidos"],
        "signals": ["fornecedor recorrente", "pagamento concentrado", "cnpj repetido"],
    },
    "missing_document_trace": {
        "title": "Conferir documento fiscal ausente ou incompleto",
        "description": "Aumenta prioridade de itens sem documento, sem link oficial ou com campos fiscais incompletos.",
        "query_hints": ["documento fiscal ausente", "url documento despesa"],
        "signals": ["sem documento", "documento ausente", "fonte incompleta"],
    },
    "travel_pattern_review": {
        "title": "Revisar padrao de viagem e deslocamento",
        "description": "Cruza viagens com periodo, fornecedor, valor e sequencia de datas para achar concentracoes fora do padrao.",
        "query_hints": ["passagens hospedagem locomoção", "viagens deslocamento parlamentar"],
        "signals": ["viagem", "deslocamento", "hospedagem", "passagem"],
    },
    "political_contract_crosscheck": {
        "title": "Cruzar politico, partido e contratos locais",
        "description": "Busca nomes, partidos, fornecedores e orgaos relacionados na base acumulada antes de chamar APIs externas.",
        "query_hints": ["politico contratos compras publicas", "partido fornecedor contrato"],
        "signals": ["contrato relacionado", "partido", "politico", "fornecedor relacionado"],
    },
    "electoral_donation_crosscheck": {
        "title": "Verificar doacoes e contas eleitorais",
        "description": "Cruza pessoa, partido, fornecedor e nomes proximos com registros eleitorais ja carregados e links oficiais do TSE.",
        "query_hints": ["doacao eleitoral fornecedor", "contas eleitorais partido candidato"],
        "signals": ["doacao", "contas eleitorais", "campanha", "tse"],
    },
    "proximity_money_flow": {
        "title": "Mapear alto valor em pessoas ou fornecedores proximos",
        "description": "Procura concentracao de valores em nomes relacionados ao recorte, sem presumir parentesco ou irregularidade.",
        "query_hints": ["pessoa proxima alto valor", "fornecedor relacionado alto valor"],
        "signals": ["vinculos", "pessoa proxima", "alto valor", "movimentacao"],
    },
    "official_process_control_check": {
        "title": "Conferir processos e controle externo em fontes oficiais",
        "description": "Prioriza leitura em STF, TCU, TSE e fontes oficiais quando um nome aparece em varios recortes.",
        "query_hints": ["processo controle externo", "stf tcu tse pessoa publica"],
        "signals": ["processo", "controle externo", "stf", "tcu", "tse"],
    },
    "ocds_red_flags_methodology": {
        "title": "Aplicar metodologia OCDS de red flags por etapa da contratacao",
        "description": "Organiza sinais por planejamento, licitacao, adjudicacao, contrato e execucao, inspirado em guias Open Contracting.",
        "query_hints": ["open contracting red flags procurement", "OCDS red flags public procurement"],
        "signals": ["ocds", "red flags", "planning", "tender", "award", "contract", "implementation"],
    },
    "single_bidder_competition_check": {
        "title": "Verificar baixa competicao ou fornecedor unico",
        "description": "Procura contratacoes com poucos participantes, dispensa recorrente, inexigibilidade ou sinal de competicao limitada.",
        "query_hints": ["single bidder contract", "fornecedor unico licitacao dispensa"],
        "signals": ["fornecedor unico", "single bidder", "dispensa", "inexigibilidade", "baixa competicao"],
    },
    "contract_overrun_amendment_check": {
        "title": "Medir aditivos, atraso e aumento injustificado de custo",
        "description": "Compara valor inicial, aditivos, prazo e execucao para achar sobrecusto ou extensao fora do padrao.",
        "query_hints": ["contract amendment cost overrun", "aditivo contrato aumento prazo custo"],
        "signals": ["aditivo", "cost overrun", "delivery delay", "aumento de custo", "atraso"],
    },
    "sanctions_blacklist_supplier_check": {
        "title": "Cruzar fornecedor com sancoes, CEIS, CNEP e listas restritivas",
        "description": "Prioriza CNPJs que aparecem em bases de sancoes, empresas punidas, inidoneas ou cadastros restritivos.",
        "query_hints": ["CEIS CNEP fornecedor sancionado", "blacklisted supplier sanctions procurement"],
        "signals": ["ceis", "cnep", "sancionado", "inidoneo", "punido", "blacklist"],
    },
    "threshold_splitting_benford_check": {
        "title": "Detectar fracionamento e padroes numericos atipicos",
        "description": "Procura varios contratos abaixo de limite, valores repetidos, arredondados ou distribuicoes suspeitas por Benford.",
        "query_hints": ["threshold splitting procurement", "Benford law procurement values"],
        "signals": ["fracionamento", "benford", "limite", "valor arredondado", "contratos repetidos"],
    },
    "network_graph_relationship_check": {
        "title": "Usar grafo de relacoes entre orgao, fornecedor, socios e politicos",
        "description": "Aprende a priorizar componentes de rede com fornecedores recorrentes, pessoas relacionadas e orgaos concentrados.",
        "query_hints": ["graph fraud procurement contracts", "network anomaly supplier public official"],
        "signals": ["grafo", "rede", "socios", "relacionamento", "anomalia de rede"],
    },
    "positive_unlabeled_supplier_learning": {
        "title": "Aprender com fornecedores sancionados sem presumir culpa",
        "description": "Usa fornecedores ja sancionados como exemplos positivos e compara padroes com fornecedores nao rotulados.",
        "query_hints": ["positive unlabeled learning sanctioned suppliers", "machine learning sanctioned government suppliers"],
        "signals": ["positive unlabeled", "sancionados", "fornecedores punidos", "aprendizado semi-supervisionado"],
    },
    "open_public_source_expansion": {
        "title": "Expandir fontes abertas oficiais para controle social",
        "description": "Acrescenta PNCP, TCU, CGU, Portal da Transparencia, CEIS, CNEP, STF e TSE como trilhas de verificacao.",
        "query_hints": ["TCU dados abertos controle externo", "CGU dados abertos integridade publica", "PNCP dados abertos contratos"],
        "signals": ["pncp", "tcu", "cgu", "portal da transparencia", "ceis", "cnep", "tse", "stf"],
    },
    "backend_tunnel_cache_health": {
        "title": "Verificar estabilidade do backend, tunel e cache de API",
        "description": "Prioriza checagens quando o recorte depende de muitas consultas GET, cache local, status do backend ou tunnel publico.",
        "query_hints": ["backend health cache api", "cloudflare tunnel api latency"],
        "signals": ["backend", "api", "cache", "latencia", "tunnel", "tunel", "timeout"],
    },
    "postgis_map_spatial_cache": {
        "title": "Validar cache geografico e consulta espacial PostGIS",
        "description": "Checa se municipio, UF, coordenadas, bbox/zoom e agregacoes geograficas batem com os contratos cacheados.",
        "query_hints": ["postgis spatial cache mapa contratos", "geographic risk query bbox"],
        "signals": ["postgis", "mapa", "municipio", "uf", "coordenada", "bbox", "zoom", "geografico"],
    },
}


def add_verification_check(
    candidates: dict[str, dict[str, Any]],
    check_id: str,
    weight: float,
    source: str,
    evidence: dict[str, Any] | None = None,
) -> None:
    template = VERIFICATION_CHECK_LIBRARY.get(check_id)
    if not template:
        return
    bucket = candidates.setdefault(
        check_id,
        {
            "id": check_id,
            "title": template["title"],
            "description": template["description"],
            "query_hints": list(template["query_hints"]),
            "signals": set(template["signals"]),
            "score": 0,
            "sources": set(),
            "examples": [],
        },
    )
    bucket["score"] += weight
    bucket["sources"].add(source)
    if evidence and len(bucket["examples"]) < 8:
        compact_evidence = {
            key: str(value)
            for key, value in evidence.items()
            if value is not None and key in {"title", "entity", "supplier", "person", "party", "value", "risk_level", "type"}
        }
        if compact_evidence:
            bucket["examples"].append(compact_evidence)


def infer_verification_checks_from_text(
    candidates: dict[str, dict[str, Any]],
    value: Any,
    weight: float,
    source: str,
    evidence: dict[str, Any] | None = None,
) -> None:
    text = normalize_text(value)
    if any(term in text for term in ("SUPERFATURAMENTO", "PRECO", "REFERENCIA", "COMPARACAO", "VALOR UNITARIO")):
        add_verification_check(candidates, "reference_price_superpricing", weight, source, evidence)
    if any(term in text for term in ("FORNECEDOR", "CNPJ", "CONCENTR", "RECORRENTE", "REPETID")):
        add_verification_check(candidates, "supplier_concentration", weight, source, evidence)
    if any(term in text for term in ("DOCUMENTO", "FISCAL", "AUSENTE", "SEM DOCUMENTO", "URLDOCUMENTO")):
        add_verification_check(candidates, "missing_document_trace", weight, source, evidence)
    if any(term in text for term in ("VIAGEM", "DESLOCAMENTO", "PASSAGEM", "HOSPEDAGEM", "LOCOMOCAO", "COMBUSTIVEL")):
        add_verification_check(candidates, "travel_pattern_review", weight, source, evidence)
    if any(term in text for term in ("CONTRATO", "COMPRAS", "PARTIDO", "POLITICO", "PARLAMENTAR")):
        add_verification_check(candidates, "political_contract_crosscheck", weight, source, evidence)
    if any(term in text for term in ("DOACAO", "DOACOES", "ELEITORAL", "CAMPANHA", "CONTAS")):
        add_verification_check(candidates, "electoral_donation_crosscheck", weight, source, evidence)
    if any(term in text for term in ("VINCULO", "VINCULOS", "PROXIM", "FAMILIAR", "ALTO VALOR", "MOVIMENT")):
        add_verification_check(candidates, "proximity_money_flow", weight, source, evidence)
    if any(term in text for term in ("PROCESSO", "CONTROLE", "STF", "TCU", "TSE", "JURISPRUDENCIA")):
        add_verification_check(candidates, "official_process_control_check", weight, source, evidence)
    if any(term in text for term in ("BACKEND", "API", "CACHE", "LATENCIA", "TIMEOUT", "TUNEL", "TUNNEL")):
        add_verification_check(candidates, "backend_tunnel_cache_health", weight, source, evidence)
    if any(term in text for term in ("POSTGIS", "MAPA", "MUNICIPIO", "COORDENADA", "BBOX", "ZOOM", "GEOGRAF")):
        add_verification_check(candidates, "postgis_map_spatial_cache", weight, source, evidence)


def add_investigation_target(
    targets: dict[str, dict[str, Any]],
    target_type: str,
    query: Any,
    weight: float,
    source: str,
    evidence: dict[str, Any] | None = None,
) -> None:
    clean_query = " ".join(str(query or "").split())
    normalized = normalize_text(clean_query)
    if len(normalized) < 3:
        return
    key = f"{target_type}:{normalized}"
    bucket = targets.setdefault(
        key,
        {
            "id": key,
            "type": target_type,
            "query": clean_query,
            "term": clean_query,
            "hits": 0,
            "score": 0,
            "sources": set(),
            "examples": [],
        },
    )
    bucket["score"] += weight
    bucket["sources"].add(source)
    if evidence:
        compact_evidence = {
            key: str(value)
            for key, value in evidence.items()
            if value not in (None, "") and key in {"title", "entity", "supplier", "person", "party", "value", "risk_level", "type", "date"}
        }
        if compact_evidence and len(bucket["examples"]) < 12:
            bucket["examples"].append(compact_evidence)


def collect_date_window(*blocks: Any) -> dict[str, Any]:
    dates: list[str] = []

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for key, nested in value.items():
                if key in {"date", "dataDocumento", "contract_date", "collected_at", "generated_at", "analyzed_at"}:
                    text = str(nested or "")[:10]
                    if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
                        dates.append(text)
                elif isinstance(nested, (dict, list)):
                    walk(nested)
        elif isinstance(value, list):
            for nested in value[:500]:
                walk(nested)

    for block in blocks:
        walk(block)
    if not dates:
        return {"start": None, "end": None, "dates_found": 0}
    return {"start": min(dates), "end": max(dates), "dates_found": len(dates)}


def model_training_text(item: dict[str, Any]) -> str:
    report = item.get("report") if isinstance(item.get("report"), dict) else {}
    flags = report.get("red_flags") if isinstance(report.get("red_flags"), list) else []
    flag_text = " ".join(
        " ".join(str(flag.get(key) or "") for key in ("title", "message", "risk_level"))
        for flag in flags
        if isinstance(flag, dict)
    )
    return " ".join(
        str(value or "")
        for value in [
            item.get("title"),
            item.get("object"),
            item.get("entity"),
            item.get("supplier_name"),
            item.get("uf"),
            item.get("city"),
            item.get("risk_level"),
            item.get("value"),
            flag_text,
        ]
    )


def model_label(item: dict[str, Any]) -> int:
    risk = normalize_text(item.get("risk_level")).lower()
    score = int(item.get("risk_score") or 0)
    if risk == "alto" or score >= 70:
        return 2
    if risk in {"medio", "médio"} or score >= 35:
        return 1
    return 0


def training_dataset_profile(texts: list[str], labels: list[int]) -> dict[str, Any]:
    label_counts: dict[str, int] = {}
    token_counts = []
    non_empty = 0
    for text, label in zip(texts, labels):
        label_key = str(label)
        label_counts[label_key] = label_counts.get(label_key, 0) + 1
        tokens = [token for token in normalize_text(text).lower().split() if len(token) >= 3]
        token_counts.append(len(tokens))
        if tokens:
            non_empty += 1
    sample_size = len(texts)
    minority_count = min(label_counts.values()) if label_counts else 0
    majority_count = max(label_counts.values()) if label_counts else 0
    return {
        "sample_size": sample_size,
        "non_empty_texts": non_empty,
        "label_distribution": label_counts,
        "class_count": len(label_counts),
        "minority_class_count": minority_count,
        "majority_class_count": majority_count,
        "imbalance_ratio": round(majority_count / minority_count, 4) if minority_count else None,
        "avg_tokens": round(sum(token_counts) / sample_size, 2) if sample_size else 0,
        "max_tokens": max(token_counts) if token_counts else 0,
        "ready_for_deep_learning": sample_size >= 12 and len(label_counts) >= 2 and non_empty >= 12,
    }


def simple_classification_metrics(expected: list[int], predicted: list[int]) -> dict[str, Any]:
    if not expected or not predicted or len(expected) != len(predicted):
        return {"accuracy": None, "samples": 0}
    labels = sorted(set(expected) | set(predicted))
    correct = sum(1 for left, right in zip(expected, predicted) if int(left) == int(right))
    per_class: dict[str, dict[str, float | int]] = {}
    f1_values = []
    for label in labels:
        tp = sum(1 for left, right in zip(expected, predicted) if left == label and right == label)
        fp = sum(1 for left, right in zip(expected, predicted) if left != label and right == label)
        fn = sum(1 for left, right in zip(expected, predicted) if left == label and right != label)
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if precision + recall else 0.0
        f1_values.append(f1)
        per_class[str(label)] = {
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1": round(f1, 4),
            "support": sum(1 for value in expected if value == label),
        }
    return {
        "accuracy": round(correct / len(expected), 4),
        "macro_f1": round(sum(f1_values) / len(f1_values), 4) if f1_values else 0.0,
        "samples": len(expected),
        "per_class": per_class,
    }


def write_model_registry(selected_model_id: str, artifacts: dict[str, Any]) -> dict[str, Any]:
    registry = {
        "default_model_id": selected_model_id or "coibe-adaptive-default",
        "updated_at": brasilia_now().isoformat(),
        "models": [
            {
                "id": "coibe-adaptive-default",
                "name": "COIBE Adaptativo Padrao",
                "kind": "rules+statistics+learning",
                "path": str(MONITOR_MODEL_STATE_PATH),
                "memory_path": str(MONITOR_MODEL_MEMORY_PATH),
                "selected": (selected_model_id or "coibe-adaptive-default") == "coibe-adaptive-default",
                "available": True,
                "quantization_compatible": True,
            },
            {
                "id": "coibe-deep-mlp",
                "name": "COIBE Deep Learning MLP",
                "kind": "tfidf+mlp",
                "path": str(artifacts.get("deep_model_path") or MONITOR_DEEP_MODEL_PATH),
                "selected": selected_model_id == "coibe-deep-mlp",
                "available": bool(artifacts.get("deep_model_trained")),
                "quantization_compatible": True,
                "onnx_manifest_path": str(MONITOR_ONNX_MANIFEST_PATH),
                "quantization_manifest_path": str(MONITOR_QUANT_MANIFEST_PATH),
            },
        ],
    }
    MONITOR_MODEL_REGISTRY_PATH.write_text(json.dumps(registry, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")
    return registry


def append_model_evolution_memory(
    model_state: dict[str, Any],
    selected_terms: list[tuple[str, dict[str, Any]]],
    check_candidates: dict[str, dict[str, Any]],
    target_candidates: dict[str, dict[str, Any]],
    learned_limit: int,
) -> dict[str, Any]:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    cycle = int(model_state.get("cycles") or 0)
    memory_entry = {
        "schema": "coibe-model-memory-v1",
        "cycle": cycle,
        "updated_at": model_state.get("updated_at"),
        "selected_model_id": (model_state.get("selected_model") or {}).get("id"),
        "learned_counts": {
            "terms": len(model_state.get("learned_terms") or []),
            "checks": len(model_state.get("learned_checks") or []),
            "targets": len(model_state.get("learned_targets") or []),
        },
        "new_terms": [
            {
                "normalized": normalized,
                "term": candidate.get("term"),
                "score": round(float(candidate.get("score") or 0), 4),
                "sources": candidate.get("sources") or [],
            }
            for normalized, candidate in selected_terms[:learned_limit]
        ],
        "top_checks": [
            {
                "id": check_id,
                "title": candidate.get("title"),
                "score": round(float(candidate.get("score") or 0), 4),
                "signals": candidate.get("signals") or [],
            }
            for check_id, candidate in sorted(check_candidates.items(), key=lambda row: row[1]["score"], reverse=True)[: max(learned_limit, 8)]
        ],
        "top_targets": [
            {
                "id": target_id,
                "type": candidate.get("type"),
                "term": candidate.get("term"),
                "score": round(float(candidate.get("score") or 0), 4),
                "sources": candidate.get("sources") or [],
            }
            for target_id, candidate in sorted(target_candidates.items(), key=lambda row: row[1]["score"], reverse=True)[: max(learned_limit * 2, 16)]
        ],
        "artifacts": {
            "state_path": str(MONITOR_MODEL_STATE_PATH),
            "deep_model_path": (model_state.get("selected_model") or {}).get("deep_model_path"),
            "registry_path": (model_state.get("selected_model") or {}).get("registry_path"),
        },
    }
    with MONITOR_MODEL_MEMORY_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(memory_entry, ensure_ascii=False, default=json_default) + "\n")
    return {
        "path": str(MONITOR_MODEL_MEMORY_PATH),
        "schema": memory_entry["schema"],
        "latest_cycle": cycle,
        "latest_entry_terms": len(memory_entry["new_terms"]),
        "latest_entry_checks": len(memory_entry["top_checks"]),
        "latest_entry_targets": len(memory_entry["top_targets"]),
    }


def train_pure_python_deep_model(texts: list[str], labels: list[int]) -> dict[str, Any]:
    token_counts: dict[str, int] = {}
    tokenized: list[list[str]] = []
    for text in texts:
        tokens = [token for token in normalize_text(text).lower().split() if len(token) >= 3][:180]
        tokenized.append(tokens)
        for token in set(tokens):
            token_counts[token] = token_counts.get(token, 0) + 1
    vocab = [token for token, _ in sorted(token_counts.items(), key=lambda row: row[1], reverse=True)[:768]]
    vocab_index = {token: index for index, token in enumerate(vocab)}
    hidden_size = 24

    def seed_weight(hidden_index: int, token_index: int) -> float:
        raw = hashlib.sha256(f"{hidden_index}:{token_index}:coibe".encode("utf-8")).digest()[0]
        return (raw / 255.0 - 0.5) * 0.22

    w1 = [[seed_weight(hidden, token_index) for token_index in range(len(vocab))] for hidden in range(hidden_size)]
    w2 = [[0.0 for _ in range(hidden_size)] for _ in range(3)]

    def features(tokens: list[str]) -> list[float]:
        vector = [0.0 for _ in vocab]
        for token in tokens:
            index = vocab_index.get(token)
            if index is not None:
                vector[index] += 1.0
        total = sum(vector) or 1.0
        return [value / total for value in vector]

    def hidden(vector: list[float]) -> list[float]:
        values = []
        for row in w1:
            score = sum(weight * value for weight, value in zip(row, vector))
            values.append(max(0.0, score))
        return values

    lr = 0.45
    for _ in range(8):
        for tokens, label in zip(tokenized, labels):
            h = hidden(features(tokens))
            logits = [sum(weight * value for weight, value in zip(row, h)) for row in w2]
            prediction = max(range(3), key=lambda index: logits[index])
            if prediction == label:
                continue
            for index, value in enumerate(h):
                w2[label][index] += lr * value
                w2[prediction][index] -= lr * value
    predictions: list[int] = []
    for tokens in tokenized:
        h = hidden(features(tokens))
        logits = [sum(weight * value for weight, value in zip(row, h)) for row in w2]
        predictions.append(int(max(range(3), key=lambda index: logits[index])))

    return {
        "model_id": "coibe-deep-mlp",
        "architecture": "pure-python-hashed-relu-perceptron",
        "trained_at": brasilia_now().isoformat(),
        "labels": {"0": "baixo", "1": "medio", "2": "alto"},
        "vocab": vocab,
        "hidden_size": hidden_size,
        "w1_seed": "sha256(hidden:token:coibe)",
        "w1_scale": 0.22,
        "w2": [[round(value, 8) for value in row] for row in w2],
        "epochs": 8,
        "samples": len(texts),
        "training_metrics": simple_classification_metrics(labels, predictions),
        "quantization_compatible": True,
    }


def train_cupy_deep_model(texts: list[str], labels: list[int], config: dict[str, Any]) -> dict[str, Any]:
    import cupy

    gpu_limit = int(config.get("gpu_memory_limit_mb") or GPU_MEMORY_LIMIT_MB)
    if gpu_limit > 0:
        cupy.get_default_memory_pool().set_limit(size=gpu_limit * 1024 * 1024)

    token_counts: dict[str, int] = {}
    tokenized: list[list[str]] = []
    for text in texts:
        tokens = [token for token in normalize_text(text).lower().split() if len(token) >= 3][:240]
        tokenized.append(tokens)
        for token in set(tokens):
            token_counts[token] = token_counts.get(token, 0) + 1

    gpu_level = str(config.get("gpu_acceleration_level") or "balanced").lower()
    vocab_limit = 4096 if gpu_level == "aggressive" else 2048
    hidden_size = 128 if gpu_level == "aggressive" else 64
    vocab = [token for token, _ in sorted(token_counts.items(), key=lambda row: row[1], reverse=True)[:vocab_limit]]
    vocab_index = {token: index for index, token in enumerate(vocab)}
    if not vocab:
        raise ValueError("empty_vocabulary")

    feature_rows: list[list[float]] = []
    for tokens in tokenized:
        vector = [0.0 for _ in vocab]
        for token in tokens:
            index = vocab_index.get(token)
            if index is not None:
                vector[index] += 1.0
        total = sum(vector) or 1.0
        feature_rows.append([value / total for value in vector])

    def seed_weight(hidden_index: int, token_index: int) -> float:
        raw = hashlib.sha256(f"{hidden_index}:{token_index}:coibe".encode("utf-8")).digest()[0]
        return (raw / 255.0 - 0.5) * 0.22

    x = cupy.asarray(feature_rows, dtype=cupy.float32)
    w1 = cupy.asarray(
        [[seed_weight(hidden, token_index) for hidden in range(hidden_size)] for token_index in range(len(vocab))],
        dtype=cupy.float32,
    )
    hidden = cupy.maximum(x @ w1, 0)
    hidden = cupy.concatenate([hidden, cupy.ones((hidden.shape[0], 1), dtype=cupy.float32)], axis=1)
    y = cupy.zeros((len(labels), 3), dtype=cupy.float32)
    y[cupy.arange(len(labels)), cupy.asarray(labels, dtype=cupy.int32)] = 1.0
    regularization = cupy.eye(hidden.shape[1], dtype=cupy.float32) * 0.08
    w2 = cupy.linalg.solve(hidden.T @ hidden + regularization, hidden.T @ y).T
    predictions = cupy.asnumpy(cupy.argmax(hidden @ w2.T, axis=1)).tolist()
    w2_cpu = cupy.asnumpy(w2[:, :-1])
    bias_cpu = cupy.asnumpy(w2[:, -1])

    return {
        "model_id": "coibe-deep-mlp",
        "architecture": "cupy-random-features-ridge-classifier",
        "trained_at": brasilia_now().isoformat(),
        "labels": {"0": "baixo", "1": "medio", "2": "alto"},
        "vocab": vocab,
        "hidden_size": hidden_size,
        "w1_seed": "sha256(hidden:token:coibe)",
        "w1_scale": 0.22,
        "w2": [[round(float(value), 8) for value in row] for row in w2_cpu],
        "bias": [round(float(value), 8) for value in bias_cpu],
        "samples": len(texts),
        "training_metrics": simple_classification_metrics(labels, [int(prediction) for prediction in predictions]),
        "gpu_accelerated": True,
        "gpu_library": f"cupy {cupy.__version__}",
        "gpu_acceleration_level": gpu_level,
        "quantization_compatible": True,
    }


def predict_json_deep_model_gpu(model: dict[str, Any], texts: list[str], config: dict[str, Any]) -> list[tuple[int, float]] | None:
    if not bool(config.get("use_gpu")):
        return None
    if str(config.get("gpu_acceleration_level") or "balanced").lower() == "off":
        return None
    try:
        import cupy

        gpu_limit = int(config.get("gpu_memory_limit_mb") or GPU_MEMORY_LIMIT_MB)
        if gpu_limit > 0:
            cupy.get_default_memory_pool().set_limit(size=gpu_limit * 1024 * 1024)
        vocab = model.get("vocab") if isinstance(model.get("vocab"), list) else []
        vocab_index = {str(token): index for index, token in enumerate(vocab)}
        hidden_size = int(model.get("hidden_size") or 0)
        w2 = model.get("w2") if isinstance(model.get("w2"), list) else []
        if not vocab or hidden_size <= 0 or not w2:
            return None

        rows: list[list[float]] = []
        for text in texts:
            vector = [0.0 for _ in vocab]
            for token in [token for token in normalize_text(text).lower().split() if token in vocab_index][:240]:
                vector[vocab_index[token]] += 1.0
            total = sum(vector) or 1.0
            rows.append([value / total for value in vector])

        def seed_weight(hidden_index: int, token_index: int) -> float:
            raw = hashlib.sha256(f"{hidden_index}:{token_index}:coibe".encode("utf-8")).digest()[0]
            return (raw / 255.0 - 0.5) * float(model.get("w1_scale") or 0.22)

        x = cupy.asarray(rows, dtype=cupy.float32)
        w1 = cupy.asarray(
            [[seed_weight(hidden, token_index) for hidden in range(hidden_size)] for token_index in range(len(vocab))],
            dtype=cupy.float32,
        )
        hidden = cupy.maximum(x @ w1, 0)
        logits = hidden @ cupy.asarray(w2, dtype=cupy.float32).T
        bias = model.get("bias") if isinstance(model.get("bias"), list) else None
        if bias and len(bias) == logits.shape[1]:
            logits = logits + cupy.asarray(bias, dtype=cupy.float32)
        sorted_logits = cupy.sort(logits, axis=1)
        predictions = cupy.asnumpy(cupy.argmax(logits, axis=1)).tolist()
        spreads = cupy.asnumpy(sorted_logits[:, -1] - sorted_logits[:, -2]).tolist() if logits.shape[1] > 1 else [0.0] * len(texts)
        return [(int(prediction), float(spread)) for prediction, spread in zip(predictions, spreads)]
    except Exception:
        return None


def build_ai_model_artifacts(training_items: list[dict[str, Any]], learned_terms: list[dict[str, Any]], config: dict[str, Any]) -> dict[str, Any]:
    selected_model_id = str(config.get("selected_model_id") or "coibe-adaptive-default")
    quantization_mode = str(config.get("quantization_mode") or "dynamic-int8")
    artifacts: dict[str, Any] = {
        "selected_model_id": selected_model_id,
        "deep_learning_enabled": bool(config.get("deep_learning_enabled", True)),
        "format": "joblib+onnx-manifest+quant-manifest",
        "deep_model_path": str(MONITOR_DEEP_MODEL_PATH),
        "deep_json_model_path": str(MONITOR_DEEP_JSON_MODEL_PATH),
        "onnx_manifest_path": str(MONITOR_ONNX_MANIFEST_PATH),
        "quantization_manifest_path": str(MONITOR_QUANT_MANIFEST_PATH),
        "deep_model_trained": False,
        "training_sample_size": len(training_items),
        "quantization_compatible": True,
        "quantization_mode": quantization_mode,
    }
    labels = [model_label(item) for item in training_items]
    texts = [model_training_text(item) for item in training_items]
    dataset_profile = training_dataset_profile(texts, labels)
    artifacts["dataset_profile"] = dataset_profile
    if bool(config.get("deep_learning_enabled", True)) and len(texts) >= 12 and len(set(labels)) >= 2:
        if bool(config.get("use_gpu")) and str(config.get("gpu_acceleration_level") or "balanced").lower() != "off":
            try:
                gpu_model = train_cupy_deep_model(texts, labels, config)
                MONITOR_DEEP_JSON_MODEL_PATH.write_text(json.dumps(gpu_model, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")
                artifacts["deep_model_trained"] = True
                artifacts["deep_model_path"] = str(MONITOR_DEEP_JSON_MODEL_PATH)
                artifacts["serializer"] = "json"
                artifacts["gpu_accelerated"] = True
                artifacts["gpu_library"] = gpu_model.get("gpu_library")
                artifacts["training_metrics"] = gpu_model.get("training_metrics")
            except Exception as exc:
                artifacts["gpu_training_fallback_reason"] = str(exc)[:240]
        try:
            if artifacts["deep_model_trained"]:
                raise RuntimeError("gpu_model_already_trained")
            import pickle
            from sklearn.model_selection import train_test_split
            from sklearn.feature_extraction.text import TfidfVectorizer
            from sklearn.neural_network import MLPClassifier
            from sklearn.pipeline import Pipeline

            pipeline = Pipeline(
                [
                    ("tfidf", TfidfVectorizer(max_features=4096, ngram_range=(1, 2), min_df=1)),
                    (
                        "mlp",
                        MLPClassifier(
                            hidden_layer_sizes=(96, 32),
                            activation="relu",
                            alpha=0.0008,
                            learning_rate="adaptive",
                            max_iter=180,
                            n_iter_no_change=12,
                            random_state=42,
                        ),
                    ),
                ]
            )
            validation_metrics: dict[str, Any] | None = None
            train_texts = texts
            train_labels = labels
            if len(texts) >= 40 and dataset_profile.get("minority_class_count", 0) >= 2:
                stratify = labels if dataset_profile.get("minority_class_count", 0) >= 2 else None
                train_texts, validation_texts, train_labels, validation_labels = train_test_split(
                    texts,
                    labels,
                    test_size=0.2,
                    random_state=42,
                    stratify=stratify,
                )
                pipeline.fit(train_texts, train_labels)
                validation_predictions = [int(value) for value in pipeline.predict(validation_texts)]
                validation_metrics = simple_classification_metrics(validation_labels, validation_predictions)
            else:
                pipeline.fit(train_texts, train_labels)
            train_predictions = [int(value) for value in pipeline.predict(train_texts)]
            training_metrics = simple_classification_metrics(train_labels, train_predictions)
            payload = {
                "model_id": "coibe-deep-mlp",
                "pipeline": pipeline,
                "labels": {"0": "baixo", "1": "medio", "2": "alto"},
                "selected_model_id": selected_model_id,
                "trained_at": brasilia_now().isoformat(),
                "dataset_profile": dataset_profile,
                "training_metrics": training_metrics,
                "validation_metrics": validation_metrics,
            }
            try:
                import joblib

                joblib.dump(payload, MONITOR_DEEP_MODEL_PATH)
                artifacts["serializer"] = "joblib"
            except Exception:
                with MONITOR_DEEP_MODEL_PATH.open("wb") as model_file:
                    pickle.dump(payload, model_file)
                artifacts["serializer"] = "pickle"
            artifacts["deep_model_trained"] = True
            artifacts["training_metrics"] = training_metrics
            artifacts["validation_metrics"] = validation_metrics
            artifacts["train_sample_size"] = len(train_texts)
            artifacts["validation_sample_size"] = validation_metrics.get("samples") if validation_metrics else 0
        except Exception as exc:
            if not artifacts["deep_model_trained"]:
                fallback_model = train_pure_python_deep_model(texts, labels)
                MONITOR_DEEP_JSON_MODEL_PATH.write_text(json.dumps(fallback_model, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")
                artifacts["deep_model_trained"] = True
                artifacts["deep_model_path"] = str(MONITOR_DEEP_JSON_MODEL_PATH)
                artifacts["serializer"] = "json"
                artifacts["deep_model_fallback_reason"] = str(exc)[:240]
                artifacts["training_metrics"] = fallback_model.get("training_metrics")

    score_values = [float(term.get("score") or 0) for term in learned_terms if isinstance(term, dict)]
    max_score = max(score_values) if score_values else 1.0
    quantized_terms = []
    for term in learned_terms[:120]:
        if not isinstance(term, dict):
            continue
        score = float(term.get("score") or 0)
        quantized_terms.append(
            {
                "term": term.get("term"),
                "score_int8": int(max(-128, min(127, round((score / max_score) * 127)))) if max_score else 0,
                "hits": int(term.get("hits") or 0),
            }
        )
    quant_manifest = {
        "model_id": selected_model_id,
        "quantization_compatible": True,
        "mode": quantization_mode,
        "schema": "int8_dynamic_manifest",
        "calibration_sample_size": len(training_items),
        "dataset_profile": dataset_profile,
        "training_metrics": artifacts.get("training_metrics"),
        "validation_metrics": artifacts.get("validation_metrics"),
        "quantized_terms": quantized_terms,
    }
    onnx_manifest = {
        "model_id": selected_model_id,
        "onnx_compatible": True,
        "input": {"name": "text", "dtype": "string", "shape": ["batch"]},
        "output": {"name": "risk_label", "dtype": "int64", "labels": ["baixo", "medio", "alto"]},
        "export_note": "Exportacao ONNX real sera criada automaticamente quando skl2onnx/onnx estiverem instalados; este manifesto preserva contrato de inferencia.",
        "joblib_model_path": str(MONITOR_DEEP_MODEL_PATH),
        "dataset_profile": dataset_profile,
        "training_metrics": artifacts.get("training_metrics"),
        "validation_metrics": artifacts.get("validation_metrics"),
    }
    MONITOR_QUANT_MANIFEST_PATH.write_text(json.dumps(quant_manifest, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")
    MONITOR_ONNX_MANIFEST_PATH.write_text(json.dumps(onnx_manifest, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")
    artifacts["registry"] = write_model_registry(selected_model_id, artifacts)
    return artifacts


def update_monitor_model_state(analysis: dict[str, Any], snapshot: dict[str, Any], search_terms: list[str], gpu: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    state = load_monitor_model_state()
    cycle_items = [item for item in (analysis.get("items") or []) if isinstance(item, dict)]
    cached_training_items = [item for item in (analysis.get("model_training_items") or []) if isinstance(item, dict)]
    training_items_by_key: dict[str, dict[str, Any]] = {}
    for item in [*cycle_items, *cached_training_items]:
        key = item_key(item)
        if key.strip(":") == "":
            key = record_hash(item.get("title"), item.get("entity"), item.get("value"))
        training_items_by_key[key] = item
    training_items = list(training_items_by_key.values())
    try:
        accumulated_items_seen = max(int(analysis.get("items_analyzed") or 0), len(training_items))
    except Exception:
        accumulated_items_seen = len(training_items)
    learned_by_key: dict[str, dict[str, Any]] = {}
    for item in state.get("learned_terms", []) if isinstance(state.get("learned_terms"), list) else []:
        if not isinstance(item, dict):
            continue
        term = str(item.get("term") or "").strip()
        normalized = normalize_text(term)
        if normalized and len(normalized.split()) >= 2 and candidate_terms_from_text(term):
            learned_by_key[normalized] = item
    learned_checks_by_id: dict[str, dict[str, Any]] = {}
    for check in state.get("learned_checks", []) if isinstance(state.get("learned_checks"), list) else []:
        if not isinstance(check, dict):
            continue
        check_id = str(check.get("id") or "").strip()
        if check_id:
            learned_checks_by_id[check_id] = check
    learned_targets_by_id: dict[str, dict[str, Any]] = {}
    for target in state.get("learned_targets", []) if isinstance(state.get("learned_targets"), list) else []:
        if not isinstance(target, dict):
            continue
        target_id = str(target.get("id") or "").strip()
        if target_id:
            learned_targets_by_id[target_id] = target

    candidates: dict[str, dict[str, Any]] = {}
    check_candidates: dict[str, dict[str, Any]] = {}
    target_candidates: dict[str, dict[str, Any]] = {}
    learning_batch_size = int(config.get("learning_batch_size") or MONITOR_LEARNING_BATCH_SIZE)
    for term in BOOTSTRAP_MODEL_TERMS:
        seed_model_candidate(candidates, term, 1.5, "public_method_research")
    for check_id in VERIFICATION_CHECK_LIBRARY:
        add_verification_check(
            check_candidates,
            check_id,
            1.0,
            "public_method_research",
            {"title": VERIFICATION_CHECK_LIBRARY[check_id]["title"], "type": "metodologia_publica"},
        )
    for alert_batch in batched([alert for alert in analysis.get("alerts", [])[:80] if isinstance(alert, dict)], learning_batch_size):
        for alert in alert_batch:
            weight = 3 if str(alert.get("risk_level") or "").lower() == "alto" else 1
            alert_evidence = {
                "title": alert.get("title"),
                "entity": alert.get("entity"),
                "supplier": alert.get("supplier_name"),
                "value": alert.get("value"),
                "risk_level": alert.get("risk_level"),
            }
            infer_verification_checks_from_text(check_candidates, json.dumps(alert, ensure_ascii=False), weight, "alerts", alert_evidence)
            for source_value in [alert.get("title"), alert.get("entity"), alert.get("supplier_name")]:
                add_model_candidates(candidates, source_value, weight, "alerts")
            add_investigation_target(target_candidates, "contrato", alert.get("title"), weight, "alerts", alert_evidence)
            add_investigation_target(target_candidates, "fornecedor", alert.get("supplier_name"), weight + 1, "alerts", alert_evidence)
            add_investigation_target(target_candidates, "orgao", alert.get("entity"), weight, "alerts", alert_evidence)
            for flag in alert.get("red_flags", []) or []:
                if isinstance(flag, dict):
                    add_model_candidates(candidates, flag.get("title") or flag.get("message"), weight, "risk_flags")
                    infer_verification_checks_from_text(check_candidates, flag.get("title") or flag.get("message"), weight + 1, "risk_flags", alert_evidence)

    public_record_sample = [record for record in analysis.get("public_records", [])[:120] if isinstance(record, dict)]
    for record_batch in batched(public_record_sample, learning_batch_size):
        for record in record_batch:
            add_model_candidates(candidates, record.get("title") or record.get("query"), 1, "public_records")
            payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
            add_model_candidates(candidates, payload.get("nomeFornecedor") or payload.get("objeto") or payload.get("nome"), 1, "public_records")
            record_type = str(record.get("record_type") or "registro")
            add_investigation_target(target_candidates, record_type, record.get("title") or record.get("query"), 1, "public_records", {"title": record.get("title"), "type": record_type})
            add_investigation_target(target_candidates, "fornecedor", payload.get("nomeFornecedor") or payload.get("supplier_name"), 2, "public_records", {"title": record.get("title"), "type": record_type})
            infer_verification_checks_from_text(
                check_candidates,
                json.dumps(record, ensure_ascii=False),
                1,
                "public_records",
                {"title": record.get("title"), "type": record.get("record_type")},
            )

    training_sample_limit = int(config.get("training_sample_limit") or MONITOR_TRAINING_SAMPLE_LIMIT)
    for item_batch in batched(training_items[:training_sample_limit], learning_batch_size):
        for item in item_batch:
            weight = 4 if str(item.get("risk_level") or "").lower() == "alto" else 2
            item_evidence = {
                "title": item.get("title"),
                "entity": item.get("entity"),
                "supplier": item.get("supplier_name"),
                "value": item.get("value"),
                "risk_level": item.get("risk_level"),
            }
            for source_value in [item.get("title"), item.get("entity"), item.get("supplier_name"), item.get("object")]:
                add_model_candidates(candidates, source_value, weight, "monitoring_items")
            add_investigation_target(target_candidates, "contrato", item.get("title") or item.get("object"), weight, "monitoring_items", item_evidence)
            add_investigation_target(target_candidates, "fornecedor", item.get("supplier_name"), weight + 1, "monitoring_items", item_evidence)
            infer_verification_checks_from_text(check_candidates, json.dumps(item, ensure_ascii=False), weight, "monitoring_items", item_evidence)

    for block_name in ("political_parties", "political_people"):
        block = snapshot.get(block_name) if isinstance(snapshot.get(block_name), dict) else {}
        for item in block.get("items", [])[:80]:
            if not isinstance(item, dict):
                continue
            weight = 8 if int(item.get("priority_score") or 0) >= 80 else 3
            political_evidence = {
                "title": item.get("name"),
                "person": item.get("name"),
                "party": item.get("party"),
                "value": item.get("total_public_money"),
                "risk_level": item.get("attention_level"),
            }
            for source_value in [item.get("party"), item.get("role"), item.get("summary")]:
                add_model_candidates(candidates, source_value, weight, block_name)
            add_investigation_target(target_candidates, "politico", item.get("name"), weight, block_name, political_evidence)
            add_investigation_target(target_candidates, "partido", item.get("party"), weight, block_name, political_evidence)
            add_model_candidates(candidates, item.get("summary"), 1, block_name)
            for person in item.get("people", [])[:8] if isinstance(item.get("people"), list) else []:
                add_model_candidates(candidates, person, 1, block_name)
                add_investigation_target(target_candidates, "pessoa_ou_fornecedor", person, 2, block_name, political_evidence)
            infer_verification_checks_from_text(check_candidates, json.dumps(item, ensure_ascii=False), weight, block_name, political_evidence)

    now = brasilia_now().isoformat()
    learned_limit = int(config.get("learned_terms_per_cycle") or MAX_LEARNED_TERMS_PER_CYCLE)
    sorted_candidates = sorted(candidates.items(), key=lambda row: row[1]["score"], reverse=True)
    selected_candidates: list[tuple[str, dict[str, Any]]] = []
    seen_candidate_keys: set[str] = set()
    for normalized, candidate in sorted_candidates[:learned_limit]:
        selected_candidates.append((normalized, candidate))
        seen_candidate_keys.add(normalized)
    for normalized, candidate in sorted_candidates:
        if normalized in learned_by_key or normalized in seen_candidate_keys:
            continue
        selected_candidates.append((normalized, candidate))
        seen_candidate_keys.add(normalized)
        if len(selected_candidates) >= learned_limit * 2:
            break

    for candidate_batch in batched(selected_candidates, learning_batch_size):
        for normalized, candidate in candidate_batch:
            existing = learned_by_key.get(normalized, {"term": candidate["term"], "hits": 0, "score": 0, "first_seen_at": now})
            existing["term"] = existing.get("term") or candidate["term"]
            existing["hits"] = int(existing.get("hits") or 0) + 1
            existing["score"] = float(existing.get("score") or 0) + float(candidate["score"])
            existing["latest_seen_at"] = now
            existing["sources"] = sorted(set(existing.get("sources", [])) | set(candidate["sources"]))
            learned_by_key[normalized] = existing

    for check_id, candidate in sorted(check_candidates.items(), key=lambda row: row[1]["score"], reverse=True)[: max(learned_limit, 8)]:
        existing = learned_checks_by_id.get(
            check_id,
            {
                "id": check_id,
                "title": candidate["title"],
                "description": candidate["description"],
                "query_hints": candidate["query_hints"],
                "signals": [],
                "hits": 0,
                "score": 0,
                "first_seen_at": now,
                "examples": [],
            },
        )
        existing["hits"] = int(existing.get("hits") or 0) + 1
        existing["score"] = float(existing.get("score") or 0) + float(candidate["score"])
        existing["latest_seen_at"] = now
        existing["sources"] = sorted(set(existing.get("sources", [])) | set(candidate["sources"]))
        existing["signals"] = sorted(set(existing.get("signals", [])) | set(candidate["signals"]))
        existing["query_hints"] = list(dict.fromkeys([*(existing.get("query_hints") or []), *candidate["query_hints"]]))[:8]
        existing["examples"] = [*(existing.get("examples") or []), *candidate["examples"]][-12:]
        learned_checks_by_id[check_id] = existing

    for target_id, candidate in sorted(target_candidates.items(), key=lambda row: row[1]["score"], reverse=True)[: max(learned_limit * 2, 16)]:
        existing = learned_targets_by_id.get(
            target_id,
            {
                "id": target_id,
                "type": candidate["type"],
                "query": candidate["query"],
                "term": candidate["term"],
                "hits": 0,
                "score": 0,
                "first_seen_at": now,
                "examples": [],
            },
        )
        existing["hits"] = int(existing.get("hits") or 0) + 1
        existing["score"] = float(existing.get("score") or 0) + float(candidate["score"])
        existing["latest_seen_at"] = now
        existing["sources"] = sorted(set(existing.get("sources", [])) | set(candidate["sources"]))
        existing["examples"] = [*(existing.get("examples") or []), *candidate["examples"]][-12:]
        learned_targets_by_id[target_id] = existing

    learned_terms_memory_limit = max(200, learned_limit * 10)
    learned_checks_memory_limit = max(80, learned_limit * 4)
    learned_targets_memory_limit = max(120, learned_limit * 8)
    learned_terms = sorted(learned_by_key.values(), key=lambda item: (float(item.get("score") or 0), int(item.get("hits") or 0)), reverse=True)[:learned_terms_memory_limit]
    learned_checks = sorted(learned_checks_by_id.values(), key=lambda item: (float(item.get("score") or 0), int(item.get("hits") or 0)), reverse=True)[:learned_checks_memory_limit]
    learned_targets = sorted(learned_targets_by_id.values(), key=lambda item: (float(item.get("score") or 0), int(item.get("hits") or 0)), reverse=True)[:learned_targets_memory_limit]
    model_artifacts = build_ai_model_artifacts(training_items[:training_sample_limit], learned_terms, config)
    date_window = collect_date_window(analysis.get("items"), analysis.get("alerts"), analysis.get("public_records"), snapshot.get("political_parties"), snapshot.get("political_people"))
    record_types: dict[str, int] = {}
    for record in analysis.get("public_records", []) if isinstance(analysis.get("public_records"), list) else []:
        record_type = str(record.get("record_type") or "desconhecido")
        record_types[record_type] = record_types.get(record_type, 0) + 1
    model_state = {
        "version": "coibe-monitor-v1",
        "updated_at": now,
        "cycles": int(state.get("cycles") or 0) + 1,
        "learned_terms": learned_terms,
        "learned_checks": learned_checks,
        "learned_targets": learned_targets,
        "selected_model": {
            "id": model_artifacts.get("selected_model_id") or "coibe-adaptive-default",
            "registry_path": str(MONITOR_MODEL_REGISTRY_PATH),
            "deep_model_path": model_artifacts.get("deep_model_path"),
            "quantization_mode": model_artifacts.get("quantization_mode"),
        },
        "model_artifacts": model_artifacts,
        "cache_profile": {
            "date_window": date_window,
            "public_record_types": record_types,
            "public_records_seen": len(analysis.get("public_records", []) or []),
            "alerts_seen": len(analysis.get("alerts", []) or []),
            "items_seen": accumulated_items_seen,
            "cycle_items_seen": len(cycle_items),
            "cached_training_items_seen": len(cached_training_items),
            "model_training_sample_seen": min(len(training_items), training_sample_limit),
            "internet_pages_seen": len((snapshot.get("internet_sweep") or {}).get("items", []) if isinstance(snapshot.get("internet_sweep"), dict) else []),
            "method_research_sources": [
                "Open Contracting Partnership - Red Flags in Public Procurement / OCDS",
                "World Bank - procurement fraud and corruption warning signs",
                "Portal Nacional de Contratacoes Publicas - dados abertos",
                "Tribunal de Contas da Uniao - dados abertos",
                "Controladoria-Geral da Uniao - dados abertos e integridade publica",
            ],
        },
        "last_training": {
            "generated_at": analysis.get("generated_at"),
            "items_analyzed": accumulated_items_seen,
            "cycle_items_analyzed": len(cycle_items),
            "model_training_items": len(cached_training_items),
            "model_training_sample": min(len(training_items), training_sample_limit),
            "learning_batch_size": learning_batch_size,
            "alerts_count": analysis.get("alerts_count"),
            "learned_terms_added": len(selected_candidates),
            "learned_checks_added": min(len(check_candidates), max(learned_limit, 8)),
            "learned_targets_added": min(len(target_candidates), max(learned_limit * 2, 16)),
            "active_memory_limits": {
                "terms": learned_terms_memory_limit,
                "checks": learned_checks_memory_limit,
                "targets": learned_targets_memory_limit,
            },
            "method_research_applied": True,
            "method_research_sources": [
                "Open Contracting Partnership",
                "World Bank",
                "PNCP",
                "TCU",
                "CGU",
            ],
            "internet_sweep": snapshot.get("internet_sweep") if isinstance(snapshot.get("internet_sweep"), dict) else {},
            "model_artifacts": model_artifacts,
            "search_terms_used": search_terms,
            "optimized_search_terms_next_cycle": merged_search_terms(search_terms, {"learned_terms": learned_terms, "learned_checks": learned_checks, "learned_targets": learned_targets})[:60],
            "date_window": date_window,
            "public_record_types": record_types,
            "gpu": gpu,
            "config": config,
            "model_storage": str(MODELS_DIR),
            "legal_safety": "Aprendizado usado para priorizar busca, estrategias de verificacao e triagem; nao conclui crime, culpa ou vinculo familiar.",
        },
    }
    memory_status = append_model_evolution_memory(
        model_state,
        selected_candidates,
        check_candidates,
        target_candidates,
        learned_limit,
    )
    model_state["evolution_memory"] = memory_status
    model_state["last_training"]["evolution_memory"] = memory_status
    MONITOR_MODEL_STATE_PATH.write_text(json.dumps(model_state, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")
    with MONITOR_MODEL_TRAINING_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(model_state["last_training"], ensure_ascii=False, default=json_default) + "\n")
    return model_state


async def get_json(client: httpx.AsyncClient, path: str) -> Any:
    response = await client.get(path)
    response.raise_for_status()
    return response.json()


def rate_limit_retry_after(error: Exception) -> float | None:
    if not isinstance(error, httpx.HTTPStatusError):
        return None
    if error.response.status_code != 429:
        return None
    retry_after = error.response.headers.get("retry-after")
    try:
        return max(float(retry_after or 0), 1.0)
    except ValueError:
        return 30.0


async def collect_connector(
    client: httpx.AsyncClient,
    snapshot: dict[str, Any],
    name: str,
    path: str,
    kind: str,
) -> Any:
    started_at = brasilia_now()
    start_perf = asyncio.get_running_loop().time()
    monitor_print(f"Analisando {name} | tipo={kind} | endpoint={path}")
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
        elapsed = asyncio.get_running_loop().time() - start_perf
        monitor_print(
            f"OK {name} | registros={record_count} | tempo={elapsed:.2f}s | velocidade={records_per_second(record_count, elapsed)} reg/s"
        )
        return payload
    except Exception as exc:
        elapsed = asyncio.get_running_loop().time() - start_perf
        retry_after = rate_limit_retry_after(exc)
        is_timeout = isinstance(exc, (httpx.TimeoutException, TimeoutError, asyncio.TimeoutError))
        detail = str(exc) or exc.__class__.__name__
        snapshot["connectors"].append(
            {
                "name": name,
                "kind": kind,
                "path": path,
                "status": "rate_limited" if retry_after else "timeout" if is_timeout else "error",
                "record_count": 0,
                "error": detail,
                "error_type": exc.__class__.__name__,
                "retry_after_seconds": retry_after,
                "started_at": started_at.isoformat(),
                "finished_at": brasilia_now().isoformat(),
            }
        )
        snapshot["errors"].append({"connector": name, "error": detail, "error_type": exc.__class__.__name__})
        if retry_after:
            monitor_print(f"LIMITE {name} | tempo={elapsed:.2f}s | aguardando {retry_after:.0f}s antes de novas buscas | detalhe={detail}")
        elif is_timeout:
            monitor_print(f"TIMEOUT {name} | tempo={elapsed:.2f}s | detalhe={detail}")
        else:
            monitor_print(f"ERRO {name} | tempo={elapsed:.2f}s | detalhe={detail}")
        return None


async def collect_connector_batch(
    client: httpx.AsyncClient,
    snapshot: dict[str, Any],
    requests: list[dict[str, str]],
    max_concurrent: int,
) -> list[tuple[dict[str, str], Any]]:
    semaphore = asyncio.Semaphore(max(1, int(max_concurrent or 1)))

    async def run(request: dict[str, str]) -> tuple[dict[str, str], Any]:
        async with semaphore:
            payload = await collect_connector(
                client,
                snapshot,
                request["name"],
                request["path"],
                request["kind"],
            )
            return request, payload

    return await asyncio.gather(*(run(request) for request in requests))


def blocked_public_ip_address(value: str) -> bool:
    try:
        ip = ipaddress.ip_address(value)
    except ValueError:
        return True
    return bool(
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


async def safe_public_url(raw_url: str) -> str | None:
    parsed = urlparse(str(raw_url or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return None
    host = parsed.hostname.strip().lower()
    if host in {"localhost", "localhost.localdomain"} or host.endswith(".local"):
        return None
    try:
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        address_info = await asyncio.to_thread(socket.getaddrinfo, host, port, type=socket.SOCK_STREAM)
        resolved_ips = {info[4][0] for info in address_info}
    except Exception:
        return None
    if not resolved_ips or any(blocked_public_ip_address(ip) for ip in resolved_ips):
        return None
    return parsed.geturl()


def text_from_html(response_text: str) -> tuple[str | None, str]:
    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(response_text, "html.parser")
        title = soup.title.string.strip() if soup.title and soup.title.string else None
        for tag in soup(["script", "style", "noscript", "svg"]):
            tag.decompose()
        return title, " ".join(soup.get_text(" ").split())
    except Exception:
        return None, " ".join(response_text.split())


def internet_url_candidates(snapshot: dict[str, Any]) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []

    def add_url(url: Any, source: str, query: str = "") -> None:
        text = str(url or "").strip()
        if text:
            candidates.append({"url": text, "source": source, "query": query})

    for url in PUBLIC_WEB_SEED_URLS:
        add_url(url, "public_method_seed")
    for term, response in (snapshot.get("searches") or {}).items():
        if not isinstance(response, dict):
            continue
        for result in response.get("results", []) or []:
            if isinstance(result, dict):
                add_url(result.get("url"), str(result.get("source") or "universal_search"), str(term))
    for block_name in ("political_parties", "political_people"):
        block = snapshot.get(block_name) if isinstance(snapshot.get(block_name), dict) else {}
        for item in block.get("items", []) or []:
            if not isinstance(item, dict):
                continue
            for source in item.get("sources", []) or []:
                if isinstance(source, dict):
                    add_url(source.get("url"), block_name, item.get("name") or "")

    unique: dict[str, dict[str, str]] = {}
    for candidate in candidates:
        unique.setdefault(candidate["url"], candidate)
    return list(unique.values())


async def collect_internet_sweep(snapshot: dict[str, Any], search_terms: list[str], config: dict[str, Any]) -> dict[str, Any]:
    if not bool(config.get("internet_sweep_enabled", True)):
        return {"enabled": False, "items": [], "errors": []}

    limit = max(0, int(config.get("internet_sweep_pages_per_cycle") or MONITOR_INTERNET_SWEEP_PAGES_PER_CYCLE))
    timeout_seconds = max(3, int(config.get("internet_sweep_timeout_seconds") or MONITOR_INTERNET_SWEEP_TIMEOUT_SECONDS))
    concurrency = max(1, min(24, int(config.get("internet_sweep_concurrency") or MONITOR_INTERNET_SWEEP_CONCURRENCY)))
    keywords = list(dict.fromkeys([*search_terms[:12], *BOOTSTRAP_MODEL_TERMS[:12]]))
    selected_candidates = internet_url_candidates(snapshot)[:limit]
    records: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    timeout = httpx.Timeout(float(timeout_seconds), connect=min(5.0, float(timeout_seconds)))
    headers = {"User-Agent": "COIBE.IA internet-sweep/0.3 (+public-risk-research)"}

    async with httpx.AsyncClient(timeout=timeout, headers=headers, follow_redirects=False) as client:
        semaphore = asyncio.Semaphore(concurrency)

        async def fetch_candidate(candidate: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, str] | None]:
            safe_url = await safe_public_url(candidate["url"])
            if not safe_url:
                return None, None
            try:
                async with semaphore:
                    current_url = safe_url
                    response_text = ""
                    for _ in range(4):
                        checked = await safe_public_url(current_url)
                        if not checked:
                            raise ValueError("blocked_redirect")
                        response = await client.get(checked)
                        if 300 <= response.status_code < 400 and response.headers.get("location"):
                            current_url = urljoin(checked, response.headers["location"])
                            continue
                        response.raise_for_status()
                        content_type = response.headers.get("content-type", "").lower()
                        if content_type and not any(kind in content_type for kind in ("text/", "html", "json", "xml")):
                            raise ValueError("non_text_content")
                        response_text = response.text[:300000]
                        current_url = str(response.url)
                        break
                if not response_text:
                    return None, None
                title, text = text_from_html(response_text)
                lowered = normalize_text(text).lower()
                hits = {
                    keyword: lowered.count(normalize_text(keyword).lower())
                    for keyword in keywords
                    if keyword and lowered.count(normalize_text(keyword).lower()) > 0
                }
                parsed = urlparse(current_url)
                host = parsed.hostname or "internet"
                return (
                    {
                        "record_key": f"internet:{hashlib.sha256(current_url.encode('utf-8')).hexdigest()[:24]}",
                        "record_type": "internet_public_page",
                        "collected_at": snapshot.get("collected_at") or brasilia_now().isoformat(),
                        "source": host,
                        "title": title or host,
                        "subtitle": f"Varredura HTML publica | {sum(hits.values())} termo(s) encontrados",
                        "url": current_url,
                        "query": candidate.get("query") or "",
                        "payload": {
                            "url": current_url,
                            "source": candidate.get("source"),
                            "title": title,
                            "text_excerpt": text[:3000],
                            "keyword_hits": hits,
                            "selected_model_id": config.get("selected_model_id"),
                            "deep_learning_enabled": bool(config.get("deep_learning_enabled")),
                        },
                    },
                    None,
                )
            except Exception as exc:
                return None, {"url": safe_url, "error": str(exc)[:160]}

        for record, error in await asyncio.gather(*(fetch_candidate(candidate) for candidate in selected_candidates)):
            if record is not None:
                records.append(record)
            if error is not None:
                errors.append(error)

    return {
        "enabled": True,
        "generated_at": brasilia_now().isoformat(),
        "candidates": len(selected_candidates),
        "concurrency": concurrency,
        "items_count": len(records),
        "items": records,
        "errors": errors[:10],
    }


async def collect_snapshot(
    api_base: str,
    search_terms: list[str],
    pages: int,
    page_size: int,
    start_page: int = 1,
    timeout_seconds: int = 90,
    political_party_limit: int = POLITICAL_PARTY_SCAN_LIMIT,
    political_people_limit: int = POLITICAL_PEOPLE_SCAN_LIMIT,
    political_people_offset: int = 0,
    search_terms_per_cycle: int = MONITOR_SEARCH_TERMS_PER_CYCLE,
    search_term_offset: int = 0,
    search_delay_seconds: float = MONITOR_SEARCH_DELAY_SECONDS,
    priority_feed_queries_per_cycle: int = MONITOR_PRIORITY_FEED_QUERIES_PER_CYCLE,
    priority_feed_delay_seconds: float = 0.5,
    max_concurrent_requests: int = MONITOR_MAX_CONCURRENT_REQUESTS,
    public_api_concurrency: int = MONITOR_PUBLIC_API_CONCURRENCY,
    public_api_source_mode: str = MONITOR_PUBLIC_API_SOURCE_MODE,
) -> dict[str, Any]:
    timeout_value = max(15, int(timeout_seconds))
    timeout = httpx.Timeout(float(timeout_value), connect=min(10.0, float(timeout_value)))
    source_mode = str(public_api_source_mode or "hybrid").strip().lower()
    if source_mode not in {"live", "hybrid", "cache-first"}:
        source_mode = "hybrid"
    feed_source = "live" if source_mode == "live" else "auto"
    derived_source = "live" if source_mode == "live" else "auto"
    public_api_concurrency = max(1, min(8, int(public_api_concurrency or 1), int(max_concurrent_requests or 1)))
    headers = {"X-COIBE-Monitor": "local-monitor"}
    async with httpx.AsyncClient(base_url=api_base, timeout=timeout, headers=headers) as client:
        snapshot: dict[str, Any] = {
            "collected_at": brasilia_now().isoformat(),
            "api_base": api_base,
            "pipeline_order": ["connectors_and_scrapers", "internet_html_sweep", "raw_database_merge", "risk_rules_and_ml", "ai_artifact_training"],
            "connectors": [],
            "sources": [],
            "portal_transparencia": {},
            "feed_pages": [],
            "state_map": {},
            "political_parties": {},
            "political_people": {},
            "political_people_offset": max(int(political_people_offset or 0), 0),
            "priority_feed_queries": [],
            "search_term_offset": max(int(search_term_offset or 0), 0),
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
            f"/api/monitoring/state-map?page_size=80&source={derived_source}",
            "public_api_aggregation",
        )
        if state_map is not None:
            snapshot["state_map"] = state_map

        political_parties = await collect_connector(
            client,
            snapshot,
            "political_parties_scan",
            f"/api/political/parties?limit={political_party_limit}&source={derived_source}",
            "public_political_risk_scan",
        )
        if political_parties is not None:
            snapshot["political_parties"] = political_parties

        political_people = await collect_connector(
            client,
            snapshot,
            "political_people_scan",
            f"/api/political/politicians?limit={political_people_limit}&source={derived_source}&offset={max(int(political_people_offset or 0), 0)}",
            "public_political_risk_scan",
        )
        if political_people is not None:
            snapshot["political_people"] = political_people

        latest_feed_page = await collect_connector(
            client,
            snapshot,
            "public_contracts_feed_latest_page",
            f"/api/monitoring/feed?page=1&page_size={page_size}&source={derived_source}",
            "public_api_feed_latest",
        )
        if latest_feed_page is not None:
            snapshot["feed_pages"].append(latest_feed_page)

        priority_queries = priority_feed_queries(search_terms, priority_feed_queries_per_cycle)
        snapshot["priority_feed_queries"] = priority_queries
        for index, query in enumerate(priority_queries):
            if priority_feed_delay_seconds and index > 0:
                await asyncio.sleep(priority_feed_delay_seconds)
            encoded = httpx.QueryParams(
                {
                    "page": 1,
                    "page_size": min(page_size, 50),
                    "source": derived_source,
                    "q": query,
                    "risk_level": "alto",
                    "size_order": "desc",
                }
            )
            priority_page = await collect_connector(
                client,
                snapshot,
                f"priority_high_risk_feed:{query}",
                f"/api/monitoring/feed?{encoded}",
                "priority_high_risk_feed",
            )
            if priority_page is not None:
                snapshot["feed_pages"].append(priority_page)
            latest_connector = snapshot["connectors"][-1] if snapshot.get("connectors") else {}
            if latest_connector.get("status") == "rate_limited":
                break

        start_page = max(start_page, 1)
        end_page = start_page + pages
        feed_requests = [
            {
                "name": f"public_contracts_feed_page_{page}",
                "path": f"/api/monitoring/feed?page={page}&page_size={page_size}&source={feed_source}",
                "kind": "public_api_feed",
                "page": str(page),
            }
            for page in range(start_page, end_page)
        ]
        feed_results = await collect_connector_batch(client, snapshot, feed_requests, public_api_concurrency)
        for request, feed_page in sorted(feed_results, key=lambda row: int(row[0].get("page") or 0)):
            if feed_page is not None:
                snapshot["feed_pages"].append(feed_page)

        search_limit = max(0, int(search_terms_per_cycle))
        search_delay = max(0.0, float(search_delay_seconds))
        rotated_search_terms = rotate_list(search_terms, search_term_offset)
        limited_search_terms = rotated_search_terms[:search_limit] if search_limit else []
        if len(search_terms) > len(limited_search_terms):
            monitor_print(
                f"Busca universal limitada neste ciclo | termos_usados={len(limited_search_terms)} "
                f"de {len(search_terms)} | offset={search_term_offset} | ajuste COIBE_MONITOR_SEARCH_TERMS_PER_CYCLE se precisar"
            )
        if search_delay == 0 and max_concurrent_requests > 1:
            search_requests = [
                {
                    "name": f"universal_search:{term}",
                    "path": f"/api/search?{httpx.QueryParams({'q': term})}",
                    "kind": "public_api_search",
                    "term": term,
                }
                for term in limited_search_terms
            ]
            search_results = await collect_connector_batch(client, snapshot, search_requests, public_api_concurrency)
            for request, search_result in search_results:
                term = request["term"]
                snapshot["searches"][term] = search_result if search_result is not None else {"error": "connector failed", "results": []}
        else:
            for index, term in enumerate(limited_search_terms):
                if search_delay and index > 0:
                    await asyncio.sleep(search_delay)
                encoded = httpx.QueryParams({"q": term})
                search_result = await collect_connector(
                    client,
                    snapshot,
                    f"universal_search:{term}",
                    f"/api/search?{encoded}",
                    "public_api_search",
                )
                snapshot["searches"][term] = search_result if search_result is not None else {"error": "connector failed", "results": []}
                latest_connector = snapshot["connectors"][-1] if snapshot.get("connectors") else {}
                if latest_connector.get("status") == "rate_limited":
                    retry_after = float(latest_connector.get("retry_after_seconds") or 30)
                    monitor_print(
                        f"Busca universal pausada por limite HTTP 429 | retomara no proximo ciclo | "
                        f"retry_after={retry_after:.0f}s"
                    )
                    break

    return snapshot


def count_failed_feed_pages(snapshot: dict[str, Any]) -> int:
    return sum(
        1
        for connector in snapshot.get("connectors", [])
        if isinstance(connector, dict)
        and "_feed_page_" in str(connector.get("name") or "")
        and connector.get("status") != "ok"
    )


def count_api_feed_items(snapshot: dict[str, Any]) -> int:
    return sum(
        len(page.get("items") or [])
        for page in snapshot.get("feed_pages", [])
        if isinstance(page, dict) and page.get("source_kind") != "web_fallback"
    )


def web_fallback_item_from_record(record: dict[str, Any], index: int, feed_pages_failed: int) -> dict[str, Any]:
    payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
    url = str(record.get("url") or payload.get("url") or "")
    title = str(record.get("title") or payload.get("title") or record.get("source") or "Pagina publica")
    excerpt = str(payload.get("text_excerpt") or "")[:3000]
    keyword_hits = payload.get("keyword_hits") if isinstance(payload.get("keyword_hits"), dict) else {}
    total_hits = sum(int(value or 0) for value in keyword_hits.values())
    collected_at = str(record.get("collected_at") or brasilia_now().isoformat())
    item_date = collected_at[:10] if len(collected_at) >= 10 else brasilia_now().date().isoformat()
    source = sanitize_source_text(record.get("source") or urlparse(url).hostname or "web_publica", {"url": url}, "internet_public_page")
    stable_basis = "|".join(
        [
            str(record.get("record_key") or ""),
            url,
            title,
            item_date,
        ]
    )
    stable_id = hashlib.sha256(stable_basis.encode("utf-8", errors="ignore")).hexdigest()[:24]
    risk_score = min(75, 12 + (10 if feed_pages_failed else 0) + min(total_hits, 8) * 6)
    risk_level = "médio" if risk_score >= 30 else "baixo"
    evidence = {
        "record_type": "internet_public_page",
        "source": source,
        "title": title,
        "url": url,
        "query": record.get("query") or payload.get("query") or "",
        "matches_count": total_hits,
        "keyword_hits": keyword_hits,
        "text_excerpt": excerpt[:1000],
    }
    return sanitize_monitoring_item_record({
        "id": f"web-fallback-{stable_id}",
        "date": item_date,
        "title": title,
        "object": excerpt or title,
        "entity": source,
        "supplier_name": "",
        "supplier_cnpj": "",
        "value": 0,
        "risk_score": risk_score,
        "risk_level": risk_level,
        "source": source,
        "source_kind": "web_fallback",
        "url": url,
        "monitor_status": "web_fallback_pending_review",
        "report": {
            "summary": "Item criado por varredura web automatica porque o feed/API publica falhou ou retornou vazio.",
            "red_flags": [
                {
                    "code": "WEB-FALLBACK-PUBLICATION",
                    "title": "Varredura web por falha de API publica",
                    "has_risk": risk_score >= 30,
                    "risk_level": risk_level,
                    "message": "A plataforma manteve a coleta ativa usando pagina publica rastreada na web; exige revisao/cruzamento antes de conclusao.",
                    "evidence": {
                        "url": url,
                        "source": source,
                        "keyword_hits": keyword_hits,
                        "feed_pages_failed": feed_pages_failed,
                    },
                    "criteria": {"rule": "feed/API sem itens confiaveis no ciclo e pagina publica disponivel na varredura"},
                }
            ],
            "official_sources": [],
            "public_evidence": [evidence],
        },
        "public_evidence": [evidence],
        "web_fallback": {
            "enabled": True,
            "reason": "public_api_failed_or_empty",
            "feed_pages_failed": feed_pages_failed,
            "source_record_key": record.get("record_key"),
            "rank": index + 1,
        },
    })


def build_web_fallback_feed_items(
    snapshot: dict[str, Any],
    config: dict[str, Any],
    api_feed_items_collected: int,
    feed_pages_failed: int,
) -> list[dict[str, Any]]:
    if not bool(config.get("web_fallback_enabled", True)):
        return []
    if api_feed_items_collected > 0 and feed_pages_failed == 0:
        return []
    internet_sweep = snapshot.get("internet_sweep") if isinstance(snapshot.get("internet_sweep"), dict) else {}
    records = [record for record in internet_sweep.get("items", []) or [] if isinstance(record, dict)]
    if not records:
        return []
    seen: set[str] = set()
    items: list[dict[str, Any]] = []
    for record in records:
        key = str(record.get("record_key") or record.get("url") or "")
        if key in seen:
            continue
        seen.add(key)
        items.append(web_fallback_item_from_record(record, len(items), feed_pages_failed))
    return items


def attach_web_fallback_feed(snapshot: dict[str, Any], items: list[dict[str, Any]], enabled: bool = True) -> None:
    if not items:
        snapshot["web_fallback"] = {
            "enabled": enabled,
            "active": False,
            "items_count": 0,
            "reason": None,
        }
        return
    snapshot.setdefault("feed_pages", []).append(
        {
            "page": "web_fallback",
            "source_kind": "web_fallback",
            "items": items,
            "generated_at": brasilia_now().isoformat(),
        }
    )
    snapshot["web_fallback"] = {
        "enabled": enabled,
        "active": True,
        "items_count": len(items),
        "reason": "feed/API publica falhou ou veio vazia; usando varredura web",
    }


def flatten_feed(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for page in snapshot.get("feed_pages", []):
        for item in page.get("items", []):
            key = f"{item.get('id')}:{item.get('date')}"
            if key in seen:
                continue
            seen.add(key)
            items.append(sanitize_monitoring_item_record(item) if isinstance(item, dict) else item)
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
                "source": "Monitoramento interno da plataforma",
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

    for bucket_name, source_name in [
        ("political_parties", "Câmara dos Deputados / TSE / TCU"),
        ("political_people", "Câmara dos Deputados / Senado / TSE / STF / TCU"),
    ]:
        response = snapshot.get(bucket_name, {})
        if not isinstance(response, dict):
            continue
        for item in response.get("items", []) or []:
            if not isinstance(item, dict):
                continue
            records.append(
                {
                    "record_key": f"{bucket_name}:{item.get('type')}:{item.get('id') or normalize_text(item.get('name'))}",
                    "record_type": bucket_name,
                    "collected_at": collected_at,
                    "source": source_name,
                    "title": item.get("name"),
                    "subtitle": item.get("summary"),
                    "risk_level": item.get("attention_level"),
                    "payload": item,
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
                sanitize_public_record_row({
                    "record_key": f"evidence:{item_id}:{record_type}:{index}",
                    "record_type": record_type,
                    "collected_at": collected_at,
                    "source": evidence.get("source"),
                    "title": evidence.get("title") or item.get("title"),
                    "subtitle": f"{evidence.get('matches_count', 0)} registro(s) vinculados ao item",
                    "url": evidence.get("url"),
                    "payload": {"item": item, "evidence": evidence},
                })
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

    internet_sweep = snapshot.get("internet_sweep") if isinstance(snapshot.get("internet_sweep"), dict) else {}
    for record in internet_sweep.get("items", []) or []:
        if isinstance(record, dict):
            records.append(record)

    return [sanitize_public_record_row(record) for record in records if isinstance(record, dict)]


def ml_value_anomalies(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    values = [float(item.get("value") or 0) for item in items]
    if len(values) < 6:
        return {}

    try:
        monitor_config = load_monitor_config()
        if bool(monitor_config.get("use_gpu")):
            prepare_cuda_dll_paths()
            try:
                import cudf
                from cuml.ensemble import IsolationForest as GpuIsolationForest

                frame = cudf.DataFrame({"value": values})
                model = GpuIsolationForest(n_estimators=150, contamination=0.15, random_state=42)
                predictions = model.fit_predict(frame[["value"]]).to_numpy()
                scores = model.decision_function(frame[["value"]]).to_numpy()
                model_name = "cuML IsolationForest (GPU)"
            except Exception:
                try:
                    import cupy

                    gpu_limit = int(monitor_config.get("gpu_memory_limit_mb") or GPU_MEMORY_LIMIT_MB)
                    if gpu_limit > 0:
                        cupy.get_default_memory_pool().set_limit(size=gpu_limit * 1024 * 1024)
                    gpu_values = cupy.asarray(values, dtype=cupy.float32)
                    baseline_gpu = cupy.mean(gpu_values)
                    deviation_gpu = cupy.std(gpu_values)
                    deviation_gpu = cupy.where(deviation_gpu == 0, cupy.asarray(1.0, dtype=cupy.float32), deviation_gpu)
                    z_scores = cupy.abs((gpu_values - baseline_gpu) / deviation_gpu)
                    scores = cupy.asnumpy(z_scores)
                    predictions = cupy.asnumpy(cupy.where(z_scores > 1.7, -1, 1))
                    model_name = "CuPy z-score (GPU)"
                except Exception:
                    import pandas as pd
                    from sklearn.ensemble import IsolationForest

                    frame = pd.DataFrame({"value": values})
                    model = IsolationForest(n_estimators=150, contamination=0.15, random_state=42)
                    predictions = model.fit_predict(frame[["value"]])
                    scores = model.decision_function(frame[["value"]])
                    model_name = "IsolationForest (CPU fallback; GPU indisponivel)"
        else:
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


def deep_verification_checks_for_item(item: dict[str, Any]) -> list[dict[str, Any]]:
    candidates: dict[str, dict[str, Any]] = {}
    evidence = {
        "title": item.get("title"),
        "entity": item.get("entity"),
        "supplier": item.get("supplier_name"),
        "value": item.get("value"),
        "risk_level": item.get("risk_level"),
    }
    infer_verification_checks_from_text(
        candidates,
        json.dumps(item, ensure_ascii=False, default=json_default),
        1.0,
        "deep_learning_context",
        evidence,
    )
    selected = sorted(candidates.values(), key=lambda row: row.get("score", 0), reverse=True)[:5]
    return [
        {
            "id": check.get("id"),
            "title": check.get("title"),
            "description": check.get("description"),
            "query_hints": check.get("query_hints", [])[:3],
        }
        for check in selected
    ]


def deep_model_attention_flags(items: list[dict[str, Any]], config: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {item_key(item): [] for item in items}
    if str(config.get("selected_model_id") or "coibe-adaptive-default") != "coibe-deep-mlp":
        return output
    if not bool(config.get("deep_learning_enabled", True)):
        return output

    texts = [model_training_text(item) for item in items]
    if not texts:
        return output

    try:
        prefer_json_model = bool(config.get("use_gpu")) and MONITOR_DEEP_JSON_MODEL_PATH.exists()
        if MONITOR_DEEP_MODEL_PATH.exists() and not prefer_json_model:
            import joblib

            payload = joblib.load(MONITOR_DEEP_MODEL_PATH)
            pipeline = payload.get("pipeline") if isinstance(payload, dict) else payload
            labels = getattr(pipeline, "classes_", None)
            predictions = list(pipeline.predict(texts))
            probabilities = pipeline.predict_proba(texts) if hasattr(pipeline, "predict_proba") else None
            for item, prediction_index, probability_row in zip(items, predictions, probabilities if probabilities is not None else [None] * len(items)):
                label = int(prediction_index)
                confidence = float(max(probability_row)) if probability_row is not None else 0.0
                if labels is not None and probability_row is not None:
                    label = int(labels[int(max(range(len(probability_row)), key=lambda index: probability_row[index]))])
                if label >= 2 and confidence >= 0.55:
                    level = "alto"
                elif label == 1 and confidence >= 0.62:
                    level = "médio"
                else:
                    continue
                output[item_key(item)].append(
                    {
                        "code": "DL-LOCAL-RISK-CLASSIFIER",
                        "title": "Classificador Deep Learning Local",
                        "has_risk": True,
                        "risk_level": level,
                        "message": "Modelo local selecionado identificou padrao textual e contextual semelhante a registros priorizados no treino.",
                        "evidence": {
                            "model_id": "coibe-deep-mlp",
                            "confidence": round(confidence, 4),
                            "predicted_label": label,
                            "selected_model_id": config.get("selected_model_id"),
                            "serializer": "joblib",
                            "verification_checks": deep_verification_checks_for_item(item),
                        },
                        "criteria": {"rule": "modelo selecionado coibe-deep-mlp + confianca minima por classe"},
                    }
                )
            return output

        if MONITOR_DEEP_JSON_MODEL_PATH.exists():
            model = json.loads(MONITOR_DEEP_JSON_MODEL_PATH.read_text(encoding="utf-8-sig"))
            vocab = model.get("vocab") if isinstance(model.get("vocab"), list) else []
            vocab_index = {str(token): index for index, token in enumerate(vocab)}
            hidden_size = int(model.get("hidden_size") or 0)
            w2 = model.get("w2") if isinstance(model.get("w2"), list) else []
            gpu_predictions = predict_json_deep_model_gpu(model, texts, config)

            def seed_weight(hidden_index: int, token_index: int) -> float:
                raw = hashlib.sha256(f"{hidden_index}:{token_index}:coibe".encode("utf-8")).digest()[0]
                return (raw / 255.0 - 0.5) * float(model.get("w1_scale") or 0.22)

            for index, (item, text) in enumerate(zip(items, texts)):
                if gpu_predictions is not None:
                    prediction, spread = gpu_predictions[index]
                else:
                    tokens = [token for token in normalize_text(text).lower().split() if token in vocab_index][:180]
                    if not tokens:
                        continue
                    vector = [0.0 for _ in vocab]
                    for token in tokens:
                        vector[vocab_index[token]] += 1.0
                    total = sum(vector) or 1.0
                    vector = [value / total for value in vector]
                    hidden = []
                    for hidden_index in range(hidden_size):
                        score = sum(seed_weight(hidden_index, token_index) * value for token_index, value in enumerate(vector))
                        hidden.append(max(0.0, score))
                    logits = [sum(float(weight) * value for weight, value in zip(row, hidden)) for row in w2 if isinstance(row, list)]
                    bias = model.get("bias") if isinstance(model.get("bias"), list) else None
                    if bias and len(bias) == len(logits):
                        logits = [value + float(bias[bias_index]) for bias_index, value in enumerate(logits)]
                    if not logits:
                        continue
                    prediction = int(max(range(len(logits)), key=lambda index: logits[index]))
                    spread = max(logits) - (sorted(logits)[-2] if len(logits) > 1 else 0)
                if prediction >= 2 and spread >= 0.02:
                    level = "alto"
                elif prediction == 1 and spread >= 0.03:
                    level = "médio"
                else:
                    continue
                output[item_key(item)].append(
                    {
                        "code": "DL-LOCAL-RISK-CLASSIFIER",
                        "title": "Classificador Deep Learning Local",
                        "has_risk": True,
                        "risk_level": level,
                        "message": "Modelo local selecionado identificou padrao textual e contextual semelhante a registros priorizados no treino.",
                        "evidence": {
                            "model_id": "coibe-deep-mlp",
                            "confidence_margin": round(spread, 4),
                            "predicted_label": prediction,
                            "selected_model_id": config.get("selected_model_id"),
                            "serializer": "json",
                            "gpu_accelerated": gpu_predictions is not None,
                            "verification_checks": deep_verification_checks_for_item(item),
                        },
                        "criteria": {"rule": "modelo selecionado coibe-deep-mlp + margem minima por classe"},
                    }
                )
    except Exception:
        return output
    return output


def record_search_text(record: dict[str, Any]) -> str:
    payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
    return normalize_text(
        " ".join(
            str(value or "")
            for value in [
                record.get("title"),
                record.get("subtitle"),
                record.get("source"),
                record.get("query"),
                record.get("record_type"),
                payload.get("nome") if isinstance(payload, dict) else "",
                payload.get("nomeFornecedor") if isinstance(payload, dict) else "",
                payload.get("objeto") if isinstance(payload, dict) else "",
            ]
        )
    )


def item_supplier_key(item: dict[str, Any]) -> str:
    digits = "".join(char for char in str(item.get("supplier_cnpj") or "") if char.isdigit())
    return digits or normalize_text(item.get("supplier_name"))


def public_source_queries_for_item(item: dict[str, Any]) -> list[str]:
    supplier = str(item.get("supplier_name") or "").strip()
    supplier_cnpj = "".join(char for char in str(item.get("supplier_cnpj") or "") if char.isdigit())
    entity = str(item.get("entity") or "").strip()
    title = str(item.get("title") or item.get("object") or "").strip()
    terms = [
        supplier_cnpj,
        supplier,
        entity,
        title[:120],
        f"{supplier} contrato",
        f"{supplier} CEIS CNEP",
        f"{entity} {supplier}",
    ]
    return [term for term in dict.fromkeys(term.strip() for term in terms) if term]


def run_autonomous_agents(
    snapshot: dict[str, Any],
    items: list[dict[str, Any]],
    connector_records: list[dict[str, Any]],
) -> dict[str, Any]:
    item_flags: dict[str, list[dict[str, Any]]] = {item_key(item): [] for item in items}
    item_insights: dict[str, list[dict[str, Any]]] = {item_key(item): [] for item in items}
    insights: list[dict[str, Any]] = []

    connector_errors = [
        connector
        for connector in snapshot.get("connectors", [])
        if isinstance(connector, dict) and connector.get("status") != "ok"
    ]
    rate_limited = [connector for connector in connector_errors if connector.get("status") == "rate_limited"]
    insights.append(
        {
            "agent": "data_quality_agent",
            "title": "Saude das fontes publicas",
            "risk_level": "médio" if connector_errors else "baixo",
            "summary": f"{len(connector_errors)} fonte(s) com erro; {len(rate_limited)} por limite/rate limit.",
            "evidence": {
                "connectors_total": len(snapshot.get("connectors", []) or []),
                "connectors_failed": len(connector_errors),
                "rate_limited": len(rate_limited),
                "failed_sources": [connector.get("name") for connector in connector_errors[:8]],
            },
            "recommended_action": "Repetir ciclo, reduzir concorrencia se houver muitos 429, ou priorizar cache local.",
        }
    )

    supplier_groups: dict[str, list[dict[str, Any]]] = {}
    entity_supplier_groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for item in items:
        supplier_key = item_supplier_key(item)
        if not supplier_key:
            continue
        entity_key = normalize_text(item.get("entity"))
        supplier_groups.setdefault(supplier_key, []).append(item)
        entity_supplier_groups.setdefault((entity_key, supplier_key), []).append(item)

    for supplier_key, related in supplier_groups.items():
        entities = {normalize_text(item.get("entity")) for item in related if normalize_text(item.get("entity"))}
        total_value = sum(float(item.get("value") or 0) for item in related)
        if len(related) >= 4 and (len(entities) >= 2 or total_value >= 1_000_000):
            supplier_name = next((item.get("supplier_name") for item in related if item.get("supplier_name")), supplier_key)
            insight = {
                "agent": "relationship_graph_agent",
                "title": "Padrao relacional indireto por fornecedor",
                "risk_level": "médio",
                "summary": f"Fornecedor aparece em {len(related)} contratos e {len(entities)} orgao(s)/entidade(s).",
                "evidence": {
                    "supplier": supplier_name,
                    "supplier_key": supplier_key,
                    "contracts": len(related),
                    "entities": len(entities),
                    "total_value": round(total_value, 2),
                },
                "recommended_action": "Cruzar fornecedor com CEIS/CNEP, quadro societario, contratos por orgao e notas fiscais publicas.",
            }
            insights.append(insight)
            for item in related[:20]:
                key = item_key(item)
                item_flags[key].append(
                    {
                        "code": "AA-GRAPH-SUPPLIER-PATTERN",
                        "title": "Agente de Grafo: padrão indireto por fornecedor",
                        "has_risk": True,
                        "risk_level": "médio",
                        "message": "Agente autônomo encontrou concentração ou espalhamento relevante do fornecedor na base pública.",
                        "evidence": insight["evidence"],
                        "criteria": {"rule": ">=4 contratos do fornecedor e múltiplos órgãos ou valor total elevado"},
                    }
                )
                item_insights[key].append(insight)

    for (entity_key, supplier_key), related in entity_supplier_groups.items():
        total_value = sum(float(item.get("value") or 0) for item in related)
        if len(related) >= 3 and total_value >= 250_000:
            insights.append(
                {
                    "agent": "relationship_graph_agent",
                    "title": "Recorrência órgão-fornecedor",
                    "risk_level": "médio",
                    "summary": f"Mesmo órgão e fornecedor aparecem em {len(related)} registros somando R$ {total_value:,.2f}.",
                    "evidence": {
                        "entity": entity_key,
                        "supplier_key": supplier_key,
                        "contracts": len(related),
                        "total_value": round(total_value, 2),
                    },
                    "recommended_action": "Verificar objeto, datas, modalidade, aditivos e possível fracionamento.",
                }
            )

    records_by_text = [(record, record_search_text(record)) for record in connector_records[:5000]]
    for item in items:
        key = item_key(item)
        report = item.get("report") if isinstance(item.get("report"), dict) else {}
        public_evidence = report.get("public_evidence") if isinstance(report.get("public_evidence"), list) else []
        official_sources = report.get("official_sources") if isinstance(report.get("official_sources"), list) else []
        value = float(item.get("value") or 0)
        risk_score = int(item.get("risk_score") or 0)
        if (risk_score >= 35 or value >= 500_000) and (not public_evidence or len(official_sources) < 1):
            queries = public_source_queries_for_item(item)[:8]
            flag = {
                "code": "AA-EVIDENCE-GAP",
                "title": "Agente de Evidência: lacuna de comprovação pública",
                "has_risk": True,
                "risk_level": "médio",
                "message": "Item priorizado ainda tem poucas evidências cruzadas anexadas; exige busca documental adicional.",
                "evidence": {
                    "risk_score": risk_score,
                    "value": round(value, 2),
                    "public_evidence_count": len(public_evidence),
                    "official_sources_count": len(official_sources),
                    "suggested_queries": queries,
                },
                "criteria": {"rule": "risco/valor alto com poucas evidencias publicas anexadas"},
            }
            item_flags[key].append(flag)
            item_insights[key].append(
                {
                    "agent": "evidence_gap_agent",
                    "title": "Buscar fontes adicionais",
                    "risk_level": "médio",
                    "summary": "Lacuna de evidência para item priorizado.",
                    "evidence": flag["evidence"],
                    "recommended_action": "Executar busca por CNPJ, fornecedor, orgao e objeto em PNCP, Compras.gov.br, CEIS/CNEP e Portal da Transparencia.",
                }
            )

        terms = [normalize_text(item.get("supplier_name")), normalize_text(item.get("entity"))]
        terms = [term for term in terms if term and len(term) >= 5]
        matching_records = []
        for record, search_text in records_by_text:
            if any(term in search_text for term in terms):
                matching_records.append(record)
            if len(matching_records) >= 8:
                break
        if matching_records:
            item_insights[key].append(
                {
                    "agent": "source_discovery_agent",
                    "title": "Fontes relacionadas encontradas",
                    "risk_level": "baixo",
                    "summary": f"{len(matching_records)} registro(s) publico(s) relacionados por termo forte.",
                    "evidence": {
                        "matched_records": len(matching_records),
                        "record_types": sorted({str(record.get("record_type") or "") for record in matching_records})[:8],
                        "sources": sorted({str(record.get("source") or "") for record in matching_records if record.get("source")})[:8],
                    },
                    "recommended_action": "Usar os registros relacionados para ampliar o contexto antes da conclusao humana.",
                }
            )

    suggested_queries = []
    for item in [
        item
        for item in items
        if int(item.get("risk_score") or 0) >= 35 or float(item.get("value") or 0) >= 500_000
    ][:30]:
        suggested_queries.extend(public_source_queries_for_item(item)[:4])
    insights.append(
        {
            "agent": "source_discovery_agent",
            "title": "Fila autônoma de próximas buscas",
            "risk_level": "baixo",
            "summary": f"{len(dict.fromkeys(suggested_queries))} consulta(s) sugerida(s) para aprofundar fontes públicas.",
            "evidence": {"suggested_queries": list(dict.fromkeys(suggested_queries))[:40]},
            "recommended_action": "Usar estas consultas para ampliar varredura pública no próximo ciclo.",
        }
    )

    return {
        "enabled": True,
        "generated_at": brasilia_now().isoformat(),
        "agents": [
            "data_quality_agent",
            "relationship_graph_agent",
            "evidence_gap_agent",
            "source_discovery_agent",
        ],
        "summary": {
            "items_reviewed": len(items),
            "public_records_reviewed": len(connector_records),
            "insights_count": len(insights),
            "item_flags_count": sum(len(flags) for flags in item_flags.values()),
            "items_with_agent_insights": sum(1 for values in item_insights.values() if values),
            "safety": "Agentes usam apenas dados publicos/localmente coletados e geram hipoteses para verificacao humana.",
        },
        "insights": insights[:80],
        "item_flags": item_flags,
        "item_insights": item_insights,
    }


def parse_date_safe(value: Any):
    try:
        return datetime.fromisoformat(str(value)).date()
    except Exception:
        return None


def analyze_items(snapshot: dict[str, Any], items: list[dict[str, Any]], connector_records: list[dict[str, Any]]) -> dict[str, Any]:
    anomalies = ml_value_anomalies(items)
    monitor_config = effective_monitor_config(load_monitor_config())
    analysis_batch_size = int(monitor_config.get("analysis_batch_size") or MONITOR_ANALYSIS_BATCH_SIZE)
    adaptive_flags = adaptive_ml_attention_flags(items)
    deep_flags = deep_model_attention_flags(items, monitor_config)
    agent_report = run_autonomous_agents(snapshot, items, connector_records)
    alerts = []

    for item_batch in batched(items, analysis_batch_size):
        for item in item_batch:
            filter_legacy_generic_flags(item)
            red_flags = item.get("report", {}).get("red_flags", [])
            for flag in adaptive_flags.get(item_key(item), []):
                append_attention_flag(item, flag)
            for flag in deep_flags.get(item_key(item), []):
                append_attention_flag(item, flag)
            for flag in agent_report.get("item_flags", {}).get(item_key(item), []):
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
                        "agent_insights": agent_report.get("item_insights", {}).get(item_key(item), []),
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
        "analysis_batch": {
            "batch_size": analysis_batch_size,
            "batches": len(batched(items, analysis_batch_size)),
            "mode": "batched_vectorized",
        },
        "alerts": alerts,
        "items": items,
        "public_records": connector_records,
        "agents": {
            key: value
            for key, value in agent_report.items()
            if key not in {"item_flags", "item_insights"}
        },
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
        item = sanitize_monitoring_item_record(item)
        item["normalized_title"] = normalize_text(item.get("title"))
        item["normalized_entity"] = normalize_text(item.get("entity"))
        item["normalized_supplier"] = normalize_text(item.get("supplier_name") or item.get("supplier_cnpj"))
        item["coibe_dedup_hash"] = record_hash(item.get("supplier_cnpj"), item.get("date"), item.get("value"))
        item["coibe_case_dedup_key"] = monitoring_case_dedup_key(item)
        key = item["coibe_case_dedup_key"]
        if key.strip(":") == "":
            continue
        if key in by_key:
            by_key[key] = merge_monitoring_case_records(by_key[key], item)
        else:
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


def public_record_key(record: dict[str, Any], fallback_index: int = 0) -> str:
    key = str(record.get("record_key") or "").strip()
    if key:
        return key
    raw = "|".join(
        str(record.get(field) or "")
        for field in ("record_type", "source", "query", "title", "subtitle", "url")
    )
    return f"public:{fallback_index}:{hashlib.md5(raw.encode('utf-8')).hexdigest()}"


def normalize_public_record(record: dict[str, Any], fallback_index: int = 0) -> dict[str, Any]:
    normalized = sanitize_public_record_row(record)
    normalized["record_key"] = public_record_key(normalized, fallback_index)
    normalized["normalized_title"] = normalize_text(normalized.get("title"))
    normalized["normalized_source"] = normalize_text(normalized.get("source"))
    return normalized


def merge_public_record_rows(records: list[dict[str, Any]], limit: int | None = None) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        normalized = normalize_public_record(record, index)
        by_key[normalized["record_key"]] = normalized

    merged = list(by_key.values())
    merged.sort(key=lambda item: (str(item.get("collected_at") or ""), str(item.get("record_key") or "")), reverse=True)
    return merged[:limit] if limit else merged


def load_public_records_database(paths: dict[str, Path], limit: int = 2000) -> list[dict[str, Any]]:
    database_path = paths["processed"] / "public_api_records.json"
    if not database_path.exists():
        return []
    try:
        loaded = json.loads(database_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(loaded, list):
        return []
    return merge_public_record_rows([item for item in loaded if isinstance(item, dict)], limit=limit)


def merge_public_records_database(paths: dict[str, Path], records: list[dict[str, Any]]) -> int:
    database_path = paths["processed"] / "public_api_records.json"
    existing = load_public_records_database(paths, limit=5000)
    merged = merge_public_record_rows([*existing, *records], limit=5000)
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
        item = sanitize_monitoring_item_record(item)
        official_sources = item.get("report", {}).get("official_sources") if isinstance(item.get("report"), dict) else []
        first_source = official_sources[0] if isinstance(official_sources, list) and official_sources else {}
        records.append(
            {
                "library_key": f"monitoring:{item.get('id')}:{item.get('date')}",
                "library_type": "monitoring_item",
                "collected_at": collected_at,
                "source": first_source.get("label") or "Compras.gov.br Dados Abertos",
                "title": item.get("title"),
                "url": first_source.get("url"),
                "public_codes": public_codes_from_item(item),
                "payload": item,
            }
        )

    for record in public_records:
        record = sanitize_public_record_row(record)
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
    cycle_started = asyncio.get_running_loop().time()
    stamp = now_slug()
    monitor_config = effective_monitor_config(load_monitor_config())
    gpu = configure_gpu_runtime(monitor_config)
    collection_state = load_collection_state(paths)
    model_state = load_monitor_model_state()
    search_terms = merged_search_terms(args.search_terms, model_state)
    pages = int(monitor_config.get("research_rounds") or args.pages)
    page_size = int(monitor_config.get("feed_page_size") or args.page_size)
    timeout_seconds = int(monitor_config.get("research_timeout_seconds") or RESEARCH_TIMEOUT_SECONDS)
    political_party_limit = int(monitor_config.get("political_party_scan_limit") or POLITICAL_PARTY_SCAN_LIMIT)
    political_people_limit = int(monitor_config.get("political_people_scan_limit") or POLITICAL_PEOPLE_SCAN_LIMIT)
    search_terms_per_cycle = int(monitor_config.get("search_terms_per_cycle") or MONITOR_SEARCH_TERMS_PER_CYCLE)
    search_delay_seconds = float(
        monitor_config.get("search_delay_seconds")
        if monitor_config.get("search_delay_seconds") is not None
        else MONITOR_SEARCH_DELAY_SECONDS
    )
    priority_feed_delay_seconds = float(
        monitor_config.get("priority_feed_delay_seconds")
        if monitor_config.get("priority_feed_delay_seconds") is not None
        else 0
    )
    max_concurrent_requests = int(monitor_config.get("max_concurrent_requests") or MONITOR_MAX_CONCURRENT_REQUESTS)
    public_api_concurrency = int(monitor_config.get("public_api_concurrency") or MONITOR_PUBLIC_API_CONCURRENCY)
    public_api_source_mode = str(monitor_config.get("public_api_source_mode") or MONITOR_PUBLIC_API_SOURCE_MODE)
    training_sample_limit = int(monitor_config.get("training_sample_limit") or MONITOR_TRAINING_SAMPLE_LIMIT)
    priority_feed_queries_per_cycle = int(monitor_config.get("priority_feed_queries_per_cycle") or MONITOR_PRIORITY_FEED_QUERIES_PER_CYCLE)
    start_page = max(int(collection_state.get("next_feed_page") or 1), 1)
    political_people_offset = max(int(collection_state.get("political_people_offset") or 0), 0)
    search_term_offset = max(int(collection_state.get("search_term_offset") or 0), 0)
    monitor_print(
        "Iniciando ciclo de monitoramento | "
        f"paginas={pages} | itens_por_pagina={page_size} | pagina_inicial={start_page} | "
        f"partidos={political_party_limit} | politicos={political_people_limit} | offset_politicos={political_people_offset} | "
        f"perfil={monitor_config.get('scan_profile')} | concorrencia={max_concurrent_requests} | delay_busca={search_delay_seconds}s | "
        f"api_publica={public_api_source_mode}/{public_api_concurrency} | "
        f"modelo={monitor_config.get('selected_model_id')} | internet={'ativa' if monitor_config.get('internet_sweep_enabled') else 'desativada'} | "
        f"gpu={'ativa' if gpu.get('enabled') else 'desativada'} | "
        f"memoria_compartilhada={'ativa' if gpu.get('shared_memory_enabled') else 'desativada'}"
    )
    snapshot = await collect_snapshot(
        args.api_base,
        search_terms,
        pages,
        page_size,
        start_page=start_page,
        timeout_seconds=timeout_seconds,
        political_party_limit=political_party_limit,
        political_people_limit=political_people_limit,
        political_people_offset=political_people_offset,
        search_terms_per_cycle=search_terms_per_cycle,
        search_term_offset=search_term_offset,
        search_delay_seconds=search_delay_seconds,
        priority_feed_queries_per_cycle=priority_feed_queries_per_cycle,
        priority_feed_delay_seconds=priority_feed_delay_seconds,
        max_concurrent_requests=max_concurrent_requests,
        public_api_concurrency=public_api_concurrency,
        public_api_source_mode=public_api_source_mode,
    )
    snapshot["internet_sweep"] = await collect_internet_sweep(snapshot, search_terms, monitor_config)
    api_feed_items_collected = count_api_feed_items(snapshot)
    feed_pages_failed = count_failed_feed_pages(snapshot)
    web_fallback_items = build_web_fallback_feed_items(
        snapshot,
        monitor_config,
        api_feed_items_collected,
        feed_pages_failed,
    )
    attach_web_fallback_feed(snapshot, web_fallback_items, bool(monitor_config.get("web_fallback_enabled", True)))
    raw_path = paths["raw"] / f"snapshot-{stamp}.json"
    write_json(raw_path, snapshot)

    existing_items = load_monitoring_database(paths)
    existing_keys = {item_key(item) for item in existing_items}
    feed_items = flatten_feed(snapshot)
    new_feed_items = [item for item in feed_items if item_key(item) not in existing_keys]
    analysis_feed_items: list[dict[str, Any]] = []
    analysis_seen_keys: set[str] = set()
    for item in [*feed_items, *existing_items[:500]]:
        key = item_key(item)
        if key in analysis_seen_keys or key.strip(":") == "":
            continue
        analysis_seen_keys.add(key)
        analysis_feed_items.append(item)
    snapshot_public_records = flatten_public_records(snapshot)
    cached_public_records = load_public_records_database(paths, limit=2000)
    connector_records = merge_public_record_rows([*cached_public_records, *snapshot_public_records], limit=5000)

    analysis = analyze_items(snapshot, analysis_feed_items, connector_records)
    for record in analysis["public_records"]:
        if record.get("monitor_status") == "pending_analysis" or record.get("cached_from"):
            record["monitor_status"] = "analyzed"
            record["analyzed_at"] = analysis["generated_at"]
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
    analysis["feed_items_analyzed"] = len(analysis_feed_items)
    analysis["cached_feed_items_reanalyzed"] = max(0, len(analysis_feed_items) - len(feed_items))
    analysis["alerts"] = cached_alerts
    analysis["alerts_count"] = len(cached_alerts)
    library_records = build_library_records(snapshot, analysis_feed_items, analysis["public_records"])
    library_status = append_platform_library(paths, library_records)
    feed_items_collected = sum(
        len(page.get("items") or [])
        for page in snapshot.get("feed_pages", [])
        if isinstance(page, dict)
    )
    api_feed_items_collected = count_api_feed_items(snapshot)
    web_fallback_items_collected = len(web_fallback_items)
    priority_feed_items_collected = sum(
        int(connector.get("record_count") or 0)
        for connector in snapshot.get("connectors", [])
        if connector.get("kind") == "priority_high_risk_feed"
    )
    internet_items_collected = len((snapshot.get("internet_sweep") or {}).get("items", []) if isinstance(snapshot.get("internet_sweep"), dict) else [])
    next_feed_page = start_page + pages
    reset_reason = None
    if api_feed_items_collected == 0:
        if feed_pages_failed > 0:
            next_feed_page = start_page
            reset_reason = "feed publico falhou neste ciclo; usando fallback web e mantendo pagina para nova tentativa"
        else:
            next_feed_page = start_page + pages
            reset_reason = "feed publico sem itens neste ciclo; fallback web ativo se houver evidencias e avancando janela"
    next_political_people_offset = (political_people_offset + max(political_people_limit, 1)) % 240
    next_search_term_offset = (search_term_offset + max(search_terms_per_cycle, 1)) % max(len(search_terms), 1)
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
        "feed_page_end": start_page + pages - 1,
        "next_feed_page": next_feed_page,
        "feed_items_collected": feed_items_collected,
        "api_feed_items_collected": api_feed_items_collected,
        "web_fallback_items_collected": web_fallback_items_collected,
        "web_fallback_active": web_fallback_items_collected > 0,
        "priority_feed_items_collected": priority_feed_items_collected,
        "internet_items_collected": internet_items_collected,
        "priority_feed_queries": snapshot.get("priority_feed_queries", []),
        "new_items_analyzed": len(new_feed_items),
        "feed_items_analyzed": len(analysis_feed_items),
        "items_cached": len(existing_items),
        "feed_pages_failed": feed_pages_failed,
        "reset_reason": reset_reason,
        "political_people_offset": political_people_offset,
        "next_political_people_offset": next_political_people_offset,
        "search_term_offset": search_term_offset,
        "next_search_term_offset": next_search_term_offset,
    }
    model_training_analysis = {**analysis, "model_training_items": accumulated_items[:training_sample_limit]}
    analysis["model"] = update_monitor_model_state(model_training_analysis, snapshot, search_terms, gpu, monitor_config)
    processed_path = paths["processed"] / f"analysis-{stamp}.json"
    write_json(processed_path, analysis)
    write_json(latest_path, analysis)
    save_collection_state(paths, {
        "next_feed_page": next_feed_page,
        "political_people_offset": next_political_people_offset,
        "search_term_offset": next_search_term_offset,
        "updated_at": analysis["generated_at"],
    })

    for alert in new_alerts:
        alert_path = paths["alerts"] / f"{stamp}-{safe_filename_part(alert.get('id'))}.json"
        write_json(alert_path, alert)

    log_line = (
        f"{brasilia_now().isoformat()} "
        f"snapshot={raw_path.name} connectors={len(analysis['connectors'])} items={analysis['items_analyzed']} "
        f"db_items={database_count} public_records={public_records_count} "
        f"feed_items={feed_items_collected} api_feed={api_feed_items_collected} web_fallback={web_fallback_items_collected} "
        f"internet_items={internet_items_collected} analyzed_feed={len(analysis_feed_items)} new_items={len(new_feed_items)} cached={len(existing_items)} "
        f"feed_errors={feed_pages_failed} next_page={next_feed_page} "
        f"library={library_status['library_records_count']} added={library_status['library_records_added']} "
        f"alerts={analysis['alerts_count']}\n"
    )
    with (paths["logs"] / "monitor.log").open("a", encoding="utf-8") as log:
        log.write(log_line)

    elapsed = asyncio.get_running_loop().time() - cycle_started
    scanned_total = feed_items_collected + int(public_records_count or 0)
    monitor_print(
        "Ciclo concluido | "
        f"tempo={elapsed:.2f}s | velocidade={records_per_second(scanned_total, elapsed)} reg/s | "
        f"itens_feed={feed_items_collected} | api_feed={api_feed_items_collected} | web_fallback={web_fallback_items_collected} | novos={len(new_feed_items)} | "
        f"internet={internet_items_collected} | "
        f"analisados_feed={len(analysis_feed_items)} | "
        f"base_total={database_count} | registros_publicos={public_records_count} | alertas={analysis['alerts_count']}"
    )
    monitor_print(log_line.strip())


async def main() -> None:
    parser = argparse.ArgumentParser(description="COIBE.IA collector and risk monitor")
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--interval-minutes", type=float, default=15)
    parser.add_argument("--pages", type=int, default=10)
    parser.add_argument("--page-size", type=int, default=DEFAULT_FEED_PAGE_SIZE)
    parser.add_argument("--startup-delay-seconds", type=float, default=0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--search-terms", nargs="*", default=DEFAULT_SEARCH_TERMS)
    args = parser.parse_args()

    paths = ensure_dirs(Path(args.data_dir))
    if args.startup_delay_seconds > 0:
        await asyncio.sleep(args.startup_delay_seconds)

    monitor_print(
        "Treinamento/monitoramento iniciado | "
        f"api={args.api_base} | intervalo_min={args.interval_minutes} | "
        f"rodadas={args.pages} | page_size={args.page_size}"
    )
    while True:
        try:
            await run_once(args, paths)
        except Exception as exc:
            error_line = f"{brasilia_now().isoformat()} ERROR {exc}\n"
            with (paths["logs"] / "monitor.log").open("a", encoding="utf-8") as log:
                log.write(error_line)
            monitor_print(error_line.strip())

        if args.once:
            break
        if args.interval_minutes > 0:
            monitor_print(f"Aguardando proximo ciclo por {args.interval_minutes} minuto(s).")
            await asyncio.sleep(args.interval_minutes * 60)


if __name__ == "__main__":
    asyncio.run(main())
