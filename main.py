import os
import asyncio
import hashlib
import ipaddress
import math
import re
import signal
import socket
import subprocess
import sys
import time
import unicodedata
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
import json
from pathlib import Path
from statistics import mean, pstdev
from typing import Any
from urllib.parse import urlencode, urljoin, urlparse
from zoneinfo import ZoneInfo

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, HttpUrl

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


BRASIL_API_CNPJ_URL = "https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
PNCP_API_BASE_URL = "https://pncp.gov.br/api/consulta"
PNCP_CONTRATOS_URL = f"{PNCP_API_BASE_URL}/v1/contratos"
PNCP_CONTRATACOES_PUBLICACAO_URL = f"{PNCP_API_BASE_URL}/v1/contratacoes/publicacao"
PORTAL_TRANSPARENCIA_BASE_URL = os.getenv(
    "PORTAL_TRANSPARENCIA_BASE_URL",
    "https://api.portaldatransparencia.gov.br/api-de-dados",
).rstrip("/")
PORTAL_TRANSPARENCIA_API_KEY = os.getenv("PORTAL_TRANSPARENCIA_API_KEY", "")
PORTAL_TRANSPARENCIA_EMAIL = os.getenv("PORTAL_TRANSPARENCIA_EMAIL", "")
IBGE_CITY_SEARCH_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
COMPRAS_CONTRATOS_URL = "https://dadosabertos.compras.gov.br/modulo-contratos/1_consultarContratos"
COMPRAS_UASG_URL = "https://dadosabertos.compras.gov.br/modulo-uasg/1_consultarUasg"
QUERIDO_DIARIO_GAZETTES_URL = "https://api.queridodiario.ok.org.br/gazettes"
CAMARA_DEPUTADOS_URL = "https://dadosabertos.camara.leg.br/api/v2/deputados"
CAMARA_PARTIDOS_URL = "https://dadosabertos.camara.leg.br/api/v2/partidos"
SENADO_SENADORES_URL = "https://legis.senado.leg.br/dadosabertos/senador/lista/atual.json"
STF_PROCESSOS_URL = "https://portal.stf.jus.br/processos/listarProcessos.asp"
STF_JURISPRUDENCIA_URL = "https://jurisprudencia.stf.jus.br/pages/search"
IBGE_STATES_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
IBGE_STATES_GEOJSON_URL = "https://servicodados.ibge.gov.br/api/v3/malhas/paises/BR"
FALLBACK_CONTRACTS_START = "2025-09-01"
FALLBACK_CONTRACTS_END = "2025-09-30"
AUTO_CONTRACT_WINDOWS_DAYS = (45, 120, 240)
RED_FLAG_01_MAX_AGE_DAYS = 180
RED_FLAG_01_MIN_CONTRACT_VALUE = Decimal("500000")
UASG_LOCATION_CACHE: dict[str, dict[str, Any] | None] = {}
CNPJ_DETAILS_CACHE: dict[str, dict[str, Any] | None] = {}
LOCAL_MONITOR_LATEST_PATH = Path(os.getenv("COIBE_MONITOR_LATEST_PATH", "data/processed/latest_analysis.json"))
LOCAL_MONITOR_DB_PATH = Path(os.getenv("COIBE_MONITOR_DB_PATH", "data/processed/monitoring_items.json"))
LOCAL_PUBLIC_RECORDS_PATH = Path(os.getenv("COIBE_PUBLIC_RECORDS_PATH", "data/processed/public_api_records.json"))
PLATFORM_LIBRARY_PATH = Path(os.getenv("COIBE_LIBRARY_PATH", "data/library/library_records.jsonl"))
PLATFORM_LIBRARY_INDEX_PATH = Path(os.getenv("COIBE_LIBRARY_INDEX_PATH", "data/library/library_index.json"))
PLATFORM_PUBLIC_CODES_PATH = Path(os.getenv("COIBE_PUBLIC_CODES_PATH", "data/library/public_codes.json"))
SEARCH_CACHE_DIR = Path(os.getenv("COIBE_SEARCH_CACHE_DIR", "data/cache/search"))
SEARCH_CACHE_TTL_HOURS = float(os.getenv("COIBE_SEARCH_CACHE_TTL_HOURS", "24"))
AUTO_MONITOR_ENABLED = os.getenv("COIBE_AUTO_MONITOR", "true").lower() not in {"0", "false", "no"}
AUTO_MONITOR_INTERVAL_MINUTES = float(os.getenv("COIBE_AUTO_MONITOR_INTERVAL_MINUTES", "0"))
AUTO_MONITOR_PAGES = int(os.getenv("COIBE_AUTO_MONITOR_PAGES", "10"))
AUTO_MONITOR_PAGE_SIZE = int(os.getenv("COIBE_AUTO_MONITOR_PAGE_SIZE", "50"))
AUTO_MONITOR_API_BASE = os.getenv("COIBE_AUTO_MONITOR_API_BASE", "http://127.0.0.1:8000")
AUTO_MONITOR_PID_PATH = Path(os.getenv("COIBE_AUTO_MONITOR_PID_PATH", "data/state/monitor.pid"))
PUBLIC_CONTRACT_SOURCES = {
    source.strip().lower()
    for source in os.getenv("COIBE_PUBLIC_CONTRACT_SOURCES", "compras,pncp").split(",")
    if source.strip()
}
PUBLIC_DATA_ENRICHMENT_LIMIT = int(os.getenv("COIBE_PUBLIC_DATA_ENRICHMENT_LIMIT", "25"))
PORTAL_TRANSPARENCIA_ENRICHMENT_ENABLED = os.getenv(
    "COIBE_PORTAL_TRANSPARENCIA_ENRICHMENT",
    "true",
).lower() not in {"0", "false", "no"}
COIBE_ENV = os.getenv("COIBE_ENV", os.getenv("ENV", "production")).strip().lower()
IS_PRODUCTION = COIBE_ENV in {"prod", "production", "public"}
ADMIN_TOKEN = os.getenv("COIBE_ADMIN_TOKEN", "").strip()
REQUIRE_ADMIN_TOKEN = os.getenv("COIBE_REQUIRE_ADMIN_TOKEN", "true").lower() not in {"0", "false", "no"}
ENABLE_DOCS = os.getenv("COIBE_ENABLE_DOCS", "false" if IS_PRODUCTION else "true").lower() in {"1", "true", "yes"}
ENABLE_STORAGE_STATUS = os.getenv("COIBE_ENABLE_STORAGE_STATUS", "false").lower() in {"1", "true", "yes"}
RATE_LIMIT_ENABLED = os.getenv("COIBE_RATE_LIMIT_ENABLED", "true").lower() not in {"0", "false", "no"}
RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("COIBE_RATE_LIMIT_WINDOW_SECONDS", "60"))
RATE_LIMIT_PUBLIC_REQUESTS = int(os.getenv("COIBE_RATE_LIMIT_PUBLIC_REQUESTS", "120"))
RATE_LIMIT_HEAVY_REQUESTS = int(os.getenv("COIBE_RATE_LIMIT_HEAVY_REQUESTS", "12"))
SCRAPE_MAX_BYTES = int(os.getenv("COIBE_SCRAPE_MAX_BYTES", str(1024 * 1024)))
SCRAPE_MAX_REDIRECTS = int(os.getenv("COIBE_SCRAPE_MAX_REDIRECTS", "3"))
CORS_ORIGINS = [
    origin.strip()
    for origin in os.getenv("COIBE_CORS_ORIGINS", "https://coibe.com.br,https://www.coibe.com.br").split(",")
    if origin.strip()
]
if IS_PRODUCTION and "*" in CORS_ORIGINS:
    CORS_ORIGINS = ["https://coibe.com.br", "https://www.coibe.com.br"]
DATA_S3_SYNC_ENABLED = os.getenv("COIBE_DATA_S3_SYNC", "false").lower() in {"1", "true", "yes"}
DATA_S3_WRITE_THROUGH_ENABLED = os.getenv(
    "COIBE_DATA_S3_WRITE_THROUGH",
    os.getenv("COIBE_DATA_S3_SYNC", "false"),
).lower() in {"1", "true", "yes"}
DATA_LOCAL_CACHE_ENABLED = os.getenv(
    "COIBE_DATA_LOCAL_CACHE",
    "false" if DATA_S3_SYNC_ENABLED else "true",
).lower() in {"1", "true", "yes"}
DATA_S3_BUCKET = os.getenv("COIBE_DATA_S3_BUCKET", "")
DATA_S3_PREFIX = os.getenv("COIBE_DATA_S3_PREFIX", "data").strip("/")
DATA_S3_ENDPOINT_URL = os.getenv("COIBE_DATA_S3_ENDPOINT_URL", "")
DATA_S3_REGION = os.getenv("COIBE_DATA_S3_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
S3_CLIENT: Any | None = None
BRASILIA_TZ = ZoneInfo("America/Sao_Paulo")


def brasilia_now() -> datetime:
    return datetime.now(BRASILIA_TZ)


def brasilia_today() -> date:
    return brasilia_now().date()

S3_DATA_FILES = [
    ("processed/latest_analysis.json", LOCAL_MONITOR_LATEST_PATH),
    ("processed/monitoring_items.json", LOCAL_MONITOR_DB_PATH),
    ("processed/public_api_records.json", LOCAL_PUBLIC_RECORDS_PATH),
    ("library/library_records.jsonl", PLATFORM_LIBRARY_PATH),
    ("library/library_index.json", PLATFORM_LIBRARY_INDEX_PATH),
    ("library/public_codes.json", PLATFORM_PUBLIC_CODES_PATH),
]

POLITICAL_RELATED_PEOPLE = [
    {
        "title": "Luiz Inacio Lula da Silva",
        "aliases": ["lula", "luiz inacio", "luiz inacio lula", "lula da silva"],
        "subtitle": "Pessoa publica/politica relacionada a busca",
        "related_queries": ["Lula", "Luiz Inacio Lula da Silva", "PT", "Presidencia", "Governo Federal"],
        "url": "https://www.gov.br/planalto/",
    },
    {
        "title": "Jair Messias Bolsonaro",
        "aliases": ["jair bolsonaro", "bolsonaro", "jair messias bolsonaro"],
        "subtitle": "Pessoa publica/politica relacionada a busca",
        "related_queries": ["Jair Bolsonaro", "Bolsonaro", "PL", "Presidencia", "Governo Federal"],
        "url": "https://www.tse.jus.br/",
    },
]


REFERENCE_PRICES = {
    "computadores": Decimal("3600"),
    "notebooks": Decimal("4200"),
    "merenda escolar": Decimal("18"),
    "combustivel": Decimal("6.30"),
    "asfalto": Decimal("620"),
}

CITY_COORDS = {
    "BRASÍLIA": (-15.7939, -47.8828),
    "SÃO PAULO": (-23.5505, -46.6333),
    "RIO DE JANEIRO": (-22.9068, -43.1729),
    "RECIFE": (-8.0476, -34.8770),
    "CURITIBA": (-25.4284, -49.2733),
    "MANAUS": (-3.1190, -60.0217),
    "BELO HORIZONTE": (-19.9167, -43.9345),
    "PORTO ALEGRE": (-30.0346, -51.2177),
    "SALVADOR": (-12.9777, -38.5016),
    "FORTALEZA": (-3.7319, -38.5267),
    "GOIÂNIA": (-16.6869, -49.2648),
    "BELÉM": (-1.4558, -48.4902),
    "CARUARU": (-8.2846, -35.9702),
}

UF_CAPITAL_COORDS = {
    "AC": (-9.9747, -67.8243),
    "AL": (-9.6658, -35.7353),
    "AM": (-3.1190, -60.0217),
    "AP": (0.0349, -51.0694),
    "BA": (-12.9777, -38.5016),
    "CE": (-3.7319, -38.5267),
    "DF": (-15.7939, -47.8828),
    "ES": (-20.3155, -40.3128),
    "GO": (-16.6869, -49.2648),
    "MA": (-2.5391, -44.2829),
    "MG": (-19.9167, -43.9345),
    "MS": (-20.4697, -54.6201),
    "MT": (-15.6010, -56.0974),
    "PA": (-1.4558, -48.4902),
    "PB": (-7.1195, -34.8450),
    "PE": (-8.0476, -34.8770),
    "PI": (-5.0892, -42.8019),
    "PR": (-25.4284, -49.2733),
    "RJ": (-22.9068, -43.1729),
    "RN": (-5.7793, -35.2009),
    "RO": (-8.7608, -63.8999),
    "RR": (2.8235, -60.6758),
    "RS": (-30.0346, -51.2177),
    "SC": (-27.5949, -48.5482),
    "SE": (-10.9472, -37.0731),
    "SP": (-23.5505, -46.6333),
    "TO": (-10.1840, -48.3336),
}


class RedFlagResult(BaseModel):
    code: str
    title: str
    has_risk: bool
    risk_level: str
    message: str
    evidence: dict[str, Any] = Field(default_factory=dict)
    criteria: dict[str, Any] = Field(default_factory=dict)


class CnpjAnalysisResponse(BaseModel):
    cnpj: str
    razao_social: str | None = None
    nome_fantasia: str | None = None
    municipio: str | None = None
    uf: str | None = None
    capital_social: Decimal | None = None
    abertura: date | None = None
    qsa: list[dict[str, Any]] = Field(default_factory=list)
    red_flags: list[RedFlagResult]
    source: str = "Brasil API"


class PurchaseItem(BaseModel):
    id: int
    cidade: str
    preco_unitario: Decimal = Field(ge=0)
    descricao: str = "Computadores"


class PurchaseAnalysisRequest(BaseModel):
    compras: list[PurchaseItem] | None = None
    contamination: float = Field(0.18, ge=0.01, le=0.5)


class PurchaseAnalysisItem(PurchaseItem):
    anomaly_score: float
    is_anomaly: bool
    risk_level: str
    risk_reason: str


class PurchaseAnalysisResponse(BaseModel):
    model: str
    baseline_average: Decimal
    items: list[PurchaseAnalysisItem]


class ContractAnalysisRequest(BaseModel):
    cnpj: str
    valor_contrato: Decimal = Field(ge=0)
    data_assinatura: date = Field(default_factory=brasilia_today)
    objeto: str = "Computadores"
    cidade: str | None = None
    preco_unitario: Decimal | None = Field(default=None, ge=0)
    quantidade: int | None = Field(default=None, ge=1)


class ContractAnalysisResponse(BaseModel):
    contract: ContractAnalysisRequest
    company: CnpjAnalysisResponse
    red_flags: list[RedFlagResult]
    risk_score: int
    risk_level: str
    summary: str
    checked_sources: list[str]


class ScrapeRequest(BaseModel):
    url: HttpUrl
    keywords: list[str] = Field(default_factory=list)


class ScrapeResponse(BaseModel):
    url: str
    title: str | None = None
    text_excerpt: str
    keyword_hits: dict[str, int]
    source: str = "HTTP scrape"


class MonitoringSource(BaseModel):
    label: str
    url: str
    kind: str


class MonitoringReport(BaseModel):
    id: str
    summary: str
    risk_score: int
    risk_level: str
    red_flags: list[RedFlagResult]
    official_sources: list[MonitoringSource]
    public_evidence: list[dict[str, Any]] = Field(default_factory=list)
    generated_at: datetime
    ml_model: str


class MonitoringItem(BaseModel):
    id: str
    date: date
    title: str
    location: str
    city: str | None = None
    uf: str | None = None
    entity: str
    supplier_name: str | None = None
    supplier_cnpj: str | None = None
    value: Decimal
    formatted_value: str
    estimated_variation: Decimal
    formatted_variation: str
    risk_score: int
    risk_level: str
    object: str
    report: MonitoringReport


class MonitoringFeedResponse(BaseModel):
    page: int
    page_size: int
    has_more: bool
    total_returned: int
    sources: list[str]
    items: list[MonitoringItem]


class MonitoringMapPoint(BaseModel):
    city: str
    uf: str
    lat: float
    lng: float
    risk_score: int
    total_value: Decimal
    alerts_count: int


class MonitoringMapResponse(BaseModel):
    sources: list[str]
    points: list[MonitoringMapPoint]


class StateRiskPoint(BaseModel):
    uf: str
    state_name: str | None = None
    risk_score: int
    total_value: Decimal
    alerts_count: int


class StateMapResponse(BaseModel):
    sources: list[str]
    states: list[StateRiskPoint]


class PublicDataSourceStatus(BaseModel):
    name: str
    kind: str
    url: str
    status: str
    auto_update: str
    coverage: str


class UniversalSearchResult(BaseModel):
    type: str
    title: str
    subtitle: str | None = None
    source: str
    url: str | None = None
    risk_level: str = "baixo"
    payload: dict[str, Any] = Field(default_factory=dict)


class UniversalSearchResponse(BaseModel):
    query: str
    generated_at: datetime
    sources: list[str]
    results: list[UniversalSearchResult]


class LocalMonitorStatus(BaseModel):
    running: bool
    latest_file_exists: bool
    latest_analysis_path: str
    database_path: str = str(LOCAL_MONITOR_DB_PATH)
    public_records_path: str = str(LOCAL_PUBLIC_RECORDS_PATH)
    generated_at: str | None = None
    items_analyzed: int = 0
    alerts_count: int = 0
    database_items_count: int = 0
    public_records_count: int = 0
    library_records_count: int = 0
    library_records_added: int = 0
    public_codes_count: int = 0
    total_value: Decimal = Decimal("0")
    estimated_variation_total: Decimal = Decimal("0")
    high_alerts_count: int = 0
    monitored_entities_count: int = 0
    collector_state: dict[str, Any] = Field(default_factory=dict)
    acquisition_first: bool = True
    message: str


class LibraryStatusResponse(BaseModel):
    records_count: int
    records_added_last_cycle: int = 0
    public_codes_count: int
    library_path: str
    index_path: str
    public_codes_path: str
    updated_at: str | None = None


class PipelineReadinessResponse(BaseModel):
    storage_mode: str
    zero_file_storage: bool
    pipeline_order: list[str]
    implemented_now: list[str]
    production_targets: list[str]


class SpatialRiskRequest(BaseModel):
    company_lat: float = Field(ge=-90, le=90)
    company_lng: float = Field(ge=-180, le=180)
    agency_lat: float = Field(ge=-90, le=90)
    agency_lng: float = Field(ge=-180, le=180)
    contract_value: Decimal = Field(ge=0)
    activity: str = ""
    threshold_km: float = Field(800, ge=1)


class SpatialRiskResponse(BaseModel):
    distance_km: float
    requires_physical_presence: bool
    alert_logistico: bool
    risk_level: str
    message: str


class LocalSuperpricingItem(BaseModel):
    id: str
    date: date
    title: str
    entity: str
    supplier_name: str | None = None
    uf: str | None = None
    value: Decimal
    baseline: Decimal
    z_score: float
    is_anomaly: bool
    risk_level: str
    source_url: str | None = None


class LocalSuperpricingResponse(BaseModel):
    query: str | None = None
    uf: str | None = None
    items_compared: int
    baseline_average: Decimal
    standard_deviation: Decimal
    model: str
    items: list[LocalSuperpricingItem]


class LocalSearchResponse(BaseModel):
    query: str
    generated_at: datetime
    results: list[UniversalSearchResult]


class PriorityScanResponse(BaseModel):
    generated_at: datetime
    priority: str
    items_found: int
    items_added: int
    library_records_added: int
    library_records_count: int
    items: list[MonitoringItem]


RATE_LIMIT_BUCKETS: dict[tuple[str, str], list[float]] = {}
ADMIN_PROTECTED_PATHS = {
    "/api/storage/status",
    "/api/public-data/portal-transparencia/proxy",
    "/api/monitoring/priority-scan",
    "/api/analyze-contract",
    "/api/analyze-superpricing",
    "/api/analyze-spatial-risk",
    "/api/scrape/public-page",
}
HEAVY_RATE_LIMIT_PATHS = ADMIN_PROTECTED_PATHS | {
    "/api/search",
    "/api/monitoring/feed",
    "/api/monitoring/map",
    "/api/monitoring/state-map",
}


def client_ip_from_request(request: Request) -> str:
    forwarded = request.headers.get("cf-connecting-ip") or request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",", 1)[0].strip()
    return request.client.host if request.client else "unknown"


def check_rate_limit(request: Request) -> None:
    if not RATE_LIMIT_ENABLED:
        return

    now = time.monotonic()
    window = max(RATE_LIMIT_WINDOW_SECONDS, 1)
    limit = RATE_LIMIT_HEAVY_REQUESTS if request.url.path in HEAVY_RATE_LIMIT_PATHS else RATE_LIMIT_PUBLIC_REQUESTS
    key = (client_ip_from_request(request), request.url.path)
    timestamps = [stamp for stamp in RATE_LIMIT_BUCKETS.get(key, []) if now - stamp < window]
    if len(timestamps) >= limit:
        RATE_LIMIT_BUCKETS[key] = timestamps
        raise HTTPException(status_code=429, detail="Muitas requisicoes. Tente novamente em instantes.")
    timestamps.append(now)
    RATE_LIMIT_BUCKETS[key] = timestamps


def require_admin_token(x_coibe_admin_token: str | None = Header(default=None)) -> None:
    if not REQUIRE_ADMIN_TOKEN:
        return
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=503, detail="Endpoint administrativo indisponivel sem COIBE_ADMIN_TOKEN.")
    if x_coibe_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Token administrativo invalido ou ausente.")


def blocked_ip_address(value: str) -> bool:
    try:
        ip = ipaddress.ip_address(value)
    except ValueError:
        return True
    return any(
        [
            ip.is_private,
            ip.is_loopback,
            ip.is_link_local,
            ip.is_multicast,
            ip.is_reserved,
            ip.is_unspecified,
        ]
    )


async def ensure_public_http_url(raw_url: str) -> str:
    parsed = urlparse(raw_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise HTTPException(status_code=400, detail="URL publica invalida.")

    host = parsed.hostname.strip().lower()
    if host in {"localhost", "localhost.localdomain"} or host.endswith(".local"):
        raise HTTPException(status_code=400, detail="URL bloqueada por seguranca.")

    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        address_info = await asyncio.to_thread(socket.getaddrinfo, host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise HTTPException(status_code=400, detail="Nao foi possivel resolver o dominio informado.") from exc

    resolved_ips = {info[4][0] for info in address_info}
    if not resolved_ips or any(blocked_ip_address(ip) for ip in resolved_ips):
        raise HTTPException(status_code=400, detail="URL bloqueada por seguranca.")

    return raw_url


async def fetch_public_page_text(raw_url: str) -> tuple[str, str]:
    timeout = httpx.Timeout(15.0, connect=5.0)
    headers = {"User-Agent": "COIBE.IA public-page-scraper/0.2"}
    current_url = raw_url

    async with httpx.AsyncClient(timeout=timeout, headers=headers, follow_redirects=False) as client:
        for _ in range(SCRAPE_MAX_REDIRECTS + 1):
            await ensure_public_http_url(current_url)
            try:
                async with client.stream("GET", current_url) as response:
                    if 300 <= response.status_code < 400 and response.headers.get("location"):
                        current_url = urljoin(current_url, response.headers["location"])
                        continue
                    response.raise_for_status()
                    content_type = response.headers.get("content-type", "").lower()
                    if content_type and not any(kind in content_type for kind in ("text/", "html", "json", "xml")):
                        raise HTTPException(status_code=415, detail="Conteudo nao textual bloqueado.")

                    total = 0
                    chunks: list[bytes] = []
                    async for chunk in response.aiter_bytes():
                        total += len(chunk)
                        if total > SCRAPE_MAX_BYTES:
                            raise HTTPException(status_code=413, detail="Pagina maior que o limite permitido.")
                        chunks.append(chunk)
                    encoding = response.encoding or "utf-8"
                    return current_url, b"".join(chunks).decode(encoding, errors="replace")
            except HTTPException:
                raise
            except httpx.HTTPError as exc:
                raise HTTPException(status_code=502, detail="Nao foi possivel baixar a pagina publica.") from exc

    raise HTTPException(status_code=400, detail="Redirecionamentos demais para a URL informada.")


app = FastAPI(
    title="COIBE.IA API",
    description=(
        "Backend para consulta de dados públicos, scraping controlado, "
        "regras de risco e detecção de possíveis superfaturamentos."
    ),
    version="0.2.0",
    docs_url="/docs" if ENABLE_DOCS else None,
    redoc_url="/redoc" if ENABLE_DOCS else None,
    openapi_url="/openapi.json" if ENABLE_DOCS else None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS or ["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    try:
        check_rate_limit(request)
    except HTTPException as exc:
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
    return await call_next(request)


app.mount("/static", StaticFiles(directory="static"), name="static")


def process_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def terminate_process(pid: int) -> None:
    if pid <= 0 or pid == os.getpid() or not process_is_running(pid):
        return
    try:
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        else:
            os.kill(pid, signal.SIGTERM)
    except Exception:
        pass


def s3_key_for_data_file(relative_key: str) -> str:
    if not DATA_S3_PREFIX:
        return relative_key
    return f"{DATA_S3_PREFIX}/{relative_key}"


def data_relative_key_for_path(path: Path) -> str | None:
    try:
        return path.resolve().relative_to(Path("data").resolve()).as_posix()
    except ValueError:
        return None


def is_bucket_data_path(path: Path) -> bool:
    return bool(DATA_S3_BUCKET and data_relative_key_for_path(path))


def s3_client() -> Any | None:
    global S3_CLIENT
    if S3_CLIENT is not None:
        return S3_CLIENT
    if not DATA_S3_BUCKET:
        return None
    try:
        import boto3
    except Exception as exc:
        print(f"Não foi possível carregar boto3 para acessar dados S3: {exc}")
        return None

    client_kwargs: dict[str, Any] = {"region_name": DATA_S3_REGION}
    if DATA_S3_ENDPOINT_URL:
        client_kwargs["endpoint_url"] = DATA_S3_ENDPOINT_URL

    S3_CLIENT = boto3.client("s3", **client_kwargs)
    return S3_CLIENT


def data_path_s3_key(path: Path) -> str | None:
    relative_key = data_relative_key_for_path(path)
    if not relative_key:
        return None
    return s3_key_for_data_file(relative_key)


def read_data_text(path: Path) -> str:
    if DATA_S3_SYNC_ENABLED and is_bucket_data_path(path):
        s3 = s3_client()
        key = data_path_s3_key(path)
        if s3 is not None and key:
            response = s3.get_object(Bucket=DATA_S3_BUCKET, Key=key)
            return response["Body"].read().decode("utf-8")
    return path.read_text(encoding="utf-8")


def data_path_exists(path: Path) -> bool:
    if DATA_S3_SYNC_ENABLED and is_bucket_data_path(path):
        s3 = s3_client()
        key = data_path_s3_key(path)
        if s3 is None or not key:
            return False
        try:
            s3.head_object(Bucket=DATA_S3_BUCKET, Key=key)
            return True
        except Exception:
            return False
    return path.exists()


def write_data_text(path: Path, content: str) -> None:
    if DATA_S3_WRITE_THROUGH_ENABLED and is_bucket_data_path(path):
        s3 = s3_client()
        key = data_path_s3_key(path)
        if s3 is not None and key:
            s3.put_object(
                Bucket=DATA_S3_BUCKET,
                Key=key,
                Body=content.encode("utf-8"),
                ContentType="application/json; charset=utf-8" if path.suffix == ".json" else "text/plain; charset=utf-8",
            )
        if not DATA_LOCAL_CACHE_ENABLED:
            return

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def append_data_text(path: Path, content: str) -> None:
    if DATA_S3_WRITE_THROUGH_ENABLED and is_bucket_data_path(path):
        current = ""
        if data_path_exists(path):
            try:
                current = read_data_text(path)
            except Exception:
                current = ""
        write_data_text(path, current + content)
        if not DATA_LOCAL_CACHE_ENABLED:
            return

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file:
        file.write(content)


def read_data_json(path: Path, default: Any = None) -> Any:
    if not data_path_exists(path):
        return default
    try:
        return json.loads(read_data_text(path))
    except Exception:
        return default


def sync_data_from_s3() -> None:
    if not DATA_S3_SYNC_ENABLED or not DATA_LOCAL_CACHE_ENABLED:
        return
    if not DATA_S3_BUCKET:
        print("COIBE_DATA_S3_SYNC ativo, mas COIBE_DATA_S3_BUCKET não foi configurado.")
        return

    s3 = s3_client()
    if s3 is None:
        return
    downloaded = 0

    for relative_key, local_path in S3_DATA_FILES:
        key = s3_key_for_data_file(relative_key)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            s3.download_file(DATA_S3_BUCKET, key, str(local_path))
            downloaded += 1
        except Exception as exc:
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                print(f"Arquivo de dados não encontrado no bucket: s3://{DATA_S3_BUCKET}/{key}")
                continue
            print(f"Falha ao baixar s3://{DATA_S3_BUCKET}/{key}: {exc}")

    print(f"Sincronização S3 concluída: {downloaded}/{len(S3_DATA_FILES)} arquivos baixados.")


def start_auto_monitor() -> None:
    if not AUTO_MONITOR_ENABLED:
        return

    AUTO_MONITOR_PID_PATH.parent.mkdir(parents=True, exist_ok=True)
    if AUTO_MONITOR_PID_PATH.exists():
        try:
            existing_pid = int(AUTO_MONITOR_PID_PATH.read_text(encoding="utf-8").strip())
            if process_is_running(existing_pid):
                terminate_process(existing_pid)
        except Exception:
            pass

    logs_dir = Path("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)
    stdout = (logs_dir / "monitor.out.log").open("a", encoding="utf-8")
    stderr = (logs_dir / "monitor.err.log").open("a", encoding="utf-8")
    command = [
        sys.executable,
        "local_monitor.py",
        "--api-base",
        AUTO_MONITOR_API_BASE,
        "--interval-minutes",
        str(AUTO_MONITOR_INTERVAL_MINUTES),
        "--pages",
        str(AUTO_MONITOR_PAGES),
        "--page-size",
        str(AUTO_MONITOR_PAGE_SIZE),
        "--startup-delay-seconds",
        "8",
    ]
    creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    process = subprocess.Popen(command, stdout=stdout, stderr=stderr, creationflags=creationflags)
    AUTO_MONITOR_PID_PATH.write_text(str(process.pid), encoding="utf-8")


@app.on_event("startup")
async def start_background_collection() -> None:
    sync_data_from_s3()
    start_auto_monitor()


def normalize_cnpj(cnpj: str) -> str:
    digits = "".join(char for char in cnpj if char.isdigit())
    if len(digits) != 14:
        raise HTTPException(status_code=400, detail="CNPJ deve conter 14 dígitos.")
    return digits


def parse_brazilian_date(value: str | None) -> date | None:
    if not value:
        return None

    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue

    raise HTTPException(status_code=502, detail=f"Data inválida recebida da fonte pública: {value}")


def parse_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None

    try:
        if isinstance(value, str):
            value = value.replace(".", "").replace(",", ".") if "," in value else value
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def money(value: Decimal) -> str:
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def evidence_number(evidence: dict[str, Any], key: str) -> float | None:
    raw_value = evidence.get(key)
    if raw_value in (None, ""):
        return None
    try:
        return float(str(raw_value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def percent_text(value: float | None) -> str | None:
    if value is None:
        return None
    return f"{value:,.1f}%".replace(",", "X").replace(".", ",").replace("X", ".")


def number_text(value: float | None, suffix: str = "") -> str | None:
    if value is None:
        return None
    return f"{value:,.0f}{suffix}".replace(",", ".")


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def write_json(path: Path, data: Any) -> None:
    write_data_text(path, json.dumps(data, ensure_ascii=False, indent=2, default=json_default))


def contract_date_windows(content_date_from: date | None = None, content_date_to: date | None = None) -> list[tuple[str, str]]:
    today = brasilia_today()
    if content_date_from or content_date_to:
        start = content_date_from or (content_date_to or today) - timedelta(days=240)
        end = content_date_to or today
        return [(start.isoformat(), end.isoformat())]

    windows = [
        ((today - timedelta(days=days)).isoformat(), today.isoformat())
        for days in AUTO_CONTRACT_WINDOWS_DAYS
    ]
    windows.append((FALLBACK_CONTRACTS_START, FALLBACK_CONTRACTS_END))
    return windows


async def get_json(url: str, params: dict[str, Any] | None = None) -> Any:
    timeout = httpx.Timeout(15.0, connect=5.0)
    headers = {"User-Agent": "COIBE.IA public-data-risk-engine/0.2"}

    async with httpx.AsyncClient(timeout=timeout, headers=headers, follow_redirects=True) as client:
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Fonte pública retornou HTTP {exc.response.status_code}: {url}",
            ) from exc
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail=f"Falha ao consultar fonte pública: {url}") from exc

    return response.json()


async def get_portal_transparencia_json(path: str, params: dict[str, Any] | None = None) -> Any:
    if not PORTAL_TRANSPARENCIA_API_KEY:
        raise HTTPException(status_code=503, detail="PORTAL_TRANSPARENCIA_API_KEY não configurada no .env.")

    timeout = httpx.Timeout(30.0, connect=10.0)
    headers = {
        "User-Agent": "COIBE.IA portal-transparencia-connector/0.1",
        "chave-api-dados": PORTAL_TRANSPARENCIA_API_KEY,
    }
    if PORTAL_TRANSPARENCIA_EMAIL:
        headers["From"] = PORTAL_TRANSPARENCIA_EMAIL

    url = f"{PORTAL_TRANSPARENCIA_BASE_URL}/{path.lstrip('/')}"
    async with httpx.AsyncClient(timeout=timeout, headers=headers, follow_redirects=True) as client:
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Portal da Transparência retornou HTTP {exc.response.status_code}.",
            ) from exc
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail="Falha ao consultar Portal da Transparência.") from exc

    return response.json()


def response_items(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        for key in ("data", "resultado", "dados", "items", "content"):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def pncp_date_param(value: str | date) -> str:
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    return str(value).replace("-", "")[:8]


def portal_period_params(days: int = 3650) -> dict[str, str]:
    end = brasilia_today()
    start = end - timedelta(days=days)
    return {
        "dataInicialSancao": start.strftime("%d/%m/%Y"),
        "dataFinalSancao": end.strftime("%d/%m/%Y"),
    }


async def safe_portal_transparencia_json(path: str, params: dict[str, Any] | None = None) -> Any:
    if not PORTAL_TRANSPARENCIA_API_KEY or not PORTAL_TRANSPARENCIA_ENRICHMENT_ENABLED:
        return None
    try:
        return await get_portal_transparencia_json(path, params=params)
    except HTTPException:
        return None


async def fetch_portal_supplier_evidence(cnpj: str | None) -> list[dict[str, Any]]:
    digits = "".join(ch for ch in str(cnpj or "") if ch.isdigit())
    if len(digits) != 14 or not PORTAL_TRANSPARENCIA_API_KEY or not PORTAL_TRANSPARENCIA_ENRICHMENT_ENABLED:
        return []

    period = portal_period_params()
    checks = [
        (
            "portal_transparencia_ceis",
            "Cadastro Nacional de Empresas Inidoneas e Suspensas (CEIS)",
            "ceis",
            {"codigoSancionado": digits, "pagina": 1, **period},
            "https://portaldatransparencia.gov.br/sancoes/ceis",
        ),
        (
            "portal_transparencia_cnep",
            "Cadastro Nacional de Empresas Punidas (CNEP)",
            "cnep",
            {"codigoSancionado": digits, "pagina": 1, **period},
            "https://portaldatransparencia.gov.br/sancoes/cnep",
        ),
        (
            "portal_transparencia_contratos_fornecedor",
            "Contratos do Poder Executivo Federal por CPF/CNPJ",
            "contratos/cpf-cnpj",
            {"cpfCnpj": digits, "pagina": 1},
            "https://portaldatransparencia.gov.br/contratos",
        ),
        (
            "portal_transparencia_notas_fiscais_emitente",
            "Notas Fiscais Eletronicas do Executivo Federal por emitente",
            "notas-fiscais",
            {"cnpjEmitente": digits, "pagina": 1},
            "https://portaldatransparencia.gov.br/notas-fiscais",
        ),
    ]

    evidence: list[dict[str, Any]] = []
    for record_type, title, path, params, url in checks:
        data = await safe_portal_transparencia_json(path, params=params)
        items = response_items(data)
        if not items:
            continue
        evidence.append(
            {
                "source": "Portal da Transparencia CGU",
                "record_type": record_type,
                "title": title,
                "url": url,
                "query": params,
                "matches_count": len(items),
                "sample": items[:3],
            }
        )
    return evidence


async def fetch_cnpj_from_brasil_api(cnpj: str) -> dict[str, Any]:
    url = BRASIL_API_CNPJ_URL.format(cnpj=cnpj)
    try:
        return await get_json(url)
    except HTTPException as exc:
        if exc.status_code == 502 and "HTTP 404" in str(exc.detail):
            raise HTTPException(status_code=404, detail="CNPJ não encontrado na Brasil API.") from exc
        raise


async def fetch_cnpj_details_cached(cnpj: str | None) -> dict[str, Any] | None:
    digits = "".join(ch for ch in str(cnpj or "") if ch.isdigit())
    if len(digits) != 14:
        return None
    if digits in CNPJ_DETAILS_CACHE:
        return CNPJ_DETAILS_CACHE[digits]
    try:
        CNPJ_DETAILS_CACHE[digits] = await fetch_cnpj_from_brasil_api(digits)
    except HTTPException:
        CNPJ_DETAILS_CACHE[digits] = None
    return CNPJ_DETAILS_CACHE[digits]


async def fetch_city_from_ibge(city_name: str) -> dict[str, Any] | None:
    cities = await get_json(IBGE_CITY_SEARCH_URL)
    normalized = normalize_text(city_name)
    for city in cities:
        if normalize_text(city.get("nome", "")) == normalized:
            return city
    return None


def apply_red_flag_01(opening_date: date | None, contract_value: Decimal, contract_date: date) -> RedFlagResult:
    criteria = {
        "max_company_age_days": RED_FLAG_01_MAX_AGE_DAYS,
        "min_contract_value": str(RED_FLAG_01_MIN_CONTRACT_VALUE),
        "rule": "idade_cnpj < 180 dias E valor_contrato > R$ 500.000",
    }

    if opening_date is None:
        return RedFlagResult(
            code="RF01",
            title="Empresa de Fachada (Tempo de Vida)",
            has_risk=False,
            risk_level="indeterminado",
            message="A data de abertura do CNPJ não foi encontrada na fonte pública.",
            evidence={"company_opening_date": None, "cnpj_age_days": None},
            criteria=criteria,
        )

    cnpj_age_days = (contract_date - opening_date).days
    is_recent = cnpj_age_days < RED_FLAG_01_MAX_AGE_DAYS
    is_large_contract = contract_value > RED_FLAG_01_MIN_CONTRACT_VALUE
    has_risk = is_recent and is_large_contract

    if has_risk:
        risk_level = "alto"
        message = "CNPJ recente demais para assumir contrato de alto valor."
    elif is_recent:
        risk_level = "médio"
        message = "CNPJ recente, mas o valor informado não ultrapassa o limite da regra."
    elif is_large_contract:
        risk_level = "baixo"
        message = "Contrato de alto valor, mas o CNPJ não é recente."
    else:
        risk_level = "baixo"
        message = "Sem risco pela Red Flag 01 com os dados informados."

    return RedFlagResult(
        code="RF01",
        title="Empresa de Fachada (Tempo de Vida)",
        has_risk=has_risk,
        risk_level=risk_level,
        message=message,
        evidence={
            "company_opening_date": opening_date.isoformat(),
            "cnpj_age_days": cnpj_age_days,
            "contract_date": contract_date.isoformat(),
            "contract_value": str(contract_value),
        },
        criteria=criteria,
    )


def apply_reference_price_flag(objeto: str, preco_unitario: Decimal | None) -> RedFlagResult:
    if preco_unitario is None:
        return RedFlagResult(
            code="RF05",
            title="Preço Unitário Fora da Referência",
            has_risk=False,
            risk_level="indeterminado",
            message="Preço unitário não informado para comparação de referência.",
        )

    key = objeto.strip().lower()
    reference = next((price for name, price in REFERENCE_PRICES.items() if name in key), None)
    if reference is None:
        return RedFlagResult(
            code="RF05",
            title="Preço Unitário Fora da Referência",
            has_risk=False,
            risk_level="indeterminado",
            message="Objeto ainda não possui preço de referência cadastrado.",
            evidence={"objeto": objeto, "preco_unitario": str(preco_unitario)},
        )

    variation = (preco_unitario - reference) / reference
    has_risk = variation > Decimal("0.50")
    risk_level = "alto" if variation > Decimal("1.0") else "médio" if has_risk else "baixo"

    return RedFlagResult(
        code="RF05",
        title="Preço Unitário Fora da Referência",
        has_risk=has_risk,
        risk_level=risk_level,
        message=(
            f"Preço unitário {variation:.0%} acima da referência."
            if has_risk
            else "Preço unitário dentro da faixa de referência demonstrativa."
        ),
        evidence={
            "objeto": objeto,
            "preco_unitario": str(preco_unitario),
            "preco_referencia": str(reference),
            "variacao_percentual": float(variation),
        },
        criteria={"rule": "preco_unitario > referencia + 50%"},
    )


def default_computer_purchases() -> list[PurchaseItem]:
    return [
        PurchaseItem(id=1, cidade="São Paulo", preco_unitario=Decimal("3450")),
        PurchaseItem(id=2, cidade="São Paulo", preco_unitario=Decimal("3590")),
        PurchaseItem(id=3, cidade="Brasília", preco_unitario=Decimal("3720")),
        PurchaseItem(id=4, cidade="Curitiba", preco_unitario=Decimal("3380")),
        PurchaseItem(id=5, cidade="Recife", preco_unitario=Decimal("3650")),
        PurchaseItem(id=6, cidade="Manaus", preco_unitario=Decimal("3890")),
        PurchaseItem(id=7, cidade="Belo Horizonte", preco_unitario=Decimal("3520")),
        PurchaseItem(id=8, cidade="Porto Alegre", preco_unitario=Decimal("3410")),
        PurchaseItem(id=9, cidade="Salvador", preco_unitario=Decimal("3770")),
        PurchaseItem(id=10, cidade="Fortaleza", preco_unitario=Decimal("3480")),
        PurchaseItem(id=11, cidade="Goiânia", preco_unitario=Decimal("11900")),
        PurchaseItem(id=12, cidade="Belém", preco_unitario=Decimal("9800")),
    ]


def analyze_purchase_anomalies(
    purchases: list[PurchaseItem],
    contamination: float,
) -> PurchaseAnalysisResponse:
    prices = [float(item.preco_unitario) for item in purchases]
    baseline = Decimal(str(round(mean(prices), 2)))

    try:
        import pandas as pd
        from sklearn.ensemble import IsolationForest

        frame = pd.DataFrame([item.model_dump() for item in purchases])
        model = IsolationForest(n_estimators=150, contamination=contamination, random_state=42)
        features = frame[["preco_unitario"]].astype(float)
        predictions = model.fit_predict(features)
        scores = model.decision_function(features)
        model_name = "IsolationForest (scikit-learn)"
    except Exception:
        deviation = pstdev(prices) or 1
        scores = [(price - mean(prices)) / deviation for price in prices]
        predictions = [-1 if score > 1.6 else 1 for score in scores]
        model_name = "Fallback estatístico (z-score)"

    items: list[PurchaseAnalysisItem] = []
    for purchase, prediction, score in zip(purchases, predictions, scores):
        is_anomaly = int(prediction) == -1
        risk_level = "alto" if is_anomaly else "baixo"
        reason = (
            "Preço unitário muito fora da curva em relação ao conjunto analisado."
            if is_anomaly
            else "Preço unitário dentro do padrão do conjunto analisado."
        )
        items.append(
            PurchaseAnalysisItem(
                **purchase.model_dump(),
                anomaly_score=float(score),
                is_anomaly=is_anomaly,
                risk_level=risk_level,
                risk_reason=reason,
            )
        )

    items.sort(key=lambda item: (not item.is_anomaly, -float(item.preco_unitario)))
    return PurchaseAnalysisResponse(model=model_name, baseline_average=baseline, items=items)


def score_risk(flags: list[RedFlagResult]) -> tuple[int, str]:
    points = 0
    for flag in flags:
        if flag.risk_level == "alto":
            points += 40
        elif flag.risk_level == "médio":
            points += 20
        elif flag.risk_level == "indeterminado":
            points += 5

    score = min(points, 100)
    if score >= 70:
        return score, "alto"
    if score >= 35:
        return score, "médio"
    return score, "baixo"


def flag_weight(flag: RedFlagResult) -> int:
    if flag.risk_level == "alto":
        return 3
    if flag.risk_level == "médio":
        return 2
    if flag.risk_level == "indeterminado":
        return 1
    return 0


def compact_attention_flags(flags: list[RedFlagResult], limit: int = 5) -> list[RedFlagResult]:
    actionable = [flag for flag in flags if flag.has_risk or flag.risk_level in {"alto", "médio", "indeterminado"}]
    if not actionable:
        return flags[:limit]
    actionable.sort(key=lambda flag: (flag_weight(flag), flag.has_risk), reverse=True)
    return actionable[:limit]


def normalize_risk_level(value: Any) -> str:
    normalized = normalize_text(value).lower()
    if normalized == "medio":
        return "médio"
    if normalized in {"alto", "baixo", "indeterminado"}:
        return normalized
    return str(value or "").strip().lower()


def score_risk(flags: list[RedFlagResult]) -> tuple[int, str]:
    points = 0
    for flag in flags:
        risk_level = normalize_risk_level(flag.risk_level)
        if risk_level == "alto":
            points += 40
        elif risk_level == "médio":
            points += 20
        elif risk_level == "indeterminado":
            points += 5

    score = min(points, 100)
    if score >= 70:
        return score, "alto"
    if score >= 35:
        return score, "médio"
    return score, "baixo"


def flag_weight(flag: RedFlagResult) -> int:
    risk_level = normalize_risk_level(flag.risk_level)
    if risk_level == "alto":
        return 3
    if risk_level == "médio":
        return 2
    if risk_level == "indeterminado":
        return 1
    return 0


def compact_attention_flags(flags: list[RedFlagResult], limit: int = 5) -> list[RedFlagResult]:
    actionable = [
        flag
        for flag in flags
        if flag.has_risk or normalize_risk_level(flag.risk_level) in {"alto", "médio", "indeterminado"}
    ]
    if not actionable:
        return flags[:limit]
    actionable.sort(key=lambda flag: (flag_weight(flag), flag.has_risk), reverse=True)
    return actionable[:limit]


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_iso_date(value: str | None) -> date:
    parsed = parse_iso_datetime(value)
    if parsed:
        if parsed.tzinfo is not None:
            return parsed.astimezone(BRASILIA_TZ).date()
        return parsed.date()
    return brasilia_today()


def contract_content_date(contract: dict[str, Any]) -> date:
    return parse_iso_date(
        contract.get("dataVigenciaInicial")
        or contract.get("dataVigenciaInicio")
        or contract.get("dataAssinatura")
        or contract.get("dataInicioVigencia")
        or contract.get("dataPublicacaoPncp")
        or contract.get("dataPublicacaoPNCP")
        or contract.get("dataHoraInclusao")
    )


def classify_contract_risk(contract: dict[str, Any]) -> tuple[list[RedFlagResult], int, str, Decimal]:
    value = Decimal(str(contract.get("valorGlobal") or 0))
    aditivos = int(contract.get("numeroAditivo") or contract.get("numeroAditivos") or 0)
    object_text = str(contract.get("objeto") or contract.get("objetoContrato") or contract.get("objetoCompra") or "")
    estimated_variation = Decimal("0")
    flags: list[RedFlagResult] = []

    if value >= Decimal("1000000"):
        estimated_variation = max(estimated_variation, value * Decimal("0.18"))
        flags.append(
            RedFlagResult(
                code="RFVALOR",
                title="Contrato de Alto Valor",
                has_risk=True,
                risk_level="médio",
                message="Contrato acima de R$ 1 milhão priorizado para auditoria humana.",
                evidence={"valor_global": str(value)},
                criteria={"rule": "valorGlobal >= R$ 1.000.000"},
            )
        )

    if value >= Decimal("10000000"):
        estimated_variation = max(estimated_variation, value * Decimal("0.25"))
        flags.append(
            RedFlagResult(
                code="RFALTOIMPACTO",
                title="Alto Impacto Orçamentário",
                has_risk=True,
                risk_level="alto",
                message="Valor global muito elevado para monitoramento sistêmico.",
                evidence={"valor_global": str(value)},
                criteria={"rule": "valorGlobal >= R$ 10.000.000"},
            )
        )

    suspicious_terms = ["computador", "notebook", "combust", "pavimenta", "merenda", "medicamento", "obra"]
    if any(term in object_text.lower() for term in suspicious_terms):
        estimated_variation = max(estimated_variation, value * Decimal("0.12"))
        flags.append(
            RedFlagResult(
                code="RFOBJETO",
                title="Objeto Sensível para Comparação de Preços",
                has_risk=True,
                risk_level="médio",
                message="Objeto contratual possui histórico de variação de preço e requer comparação por itens.",
                evidence={"objeto": object_text[:500]},
                criteria={"rule": "objeto contém termo monitorado pela COIBE.IA"},
            )
        )

    if aditivos >= 2:
        estimated_variation = max(estimated_variation, value * Decimal("0.20"))
        flags.append(
            RedFlagResult(
                code="RF04",
                title="Explosão de Aditivos",
                has_risk=True,
                risk_level="alto",
                message="Contrato possui múltiplos aditivos registrados.",
                evidence={"numero_aditivos": aditivos},
                criteria={"rule": "numeroAditivo >= 2"},
            )
        )

    if not flags:
        flags.append(
            RedFlagResult(
                code="RFBASE",
                title="Monitoramento Preventivo",
                has_risk=False,
                risk_level="baixo",
                message="Sem fator forte nos dados comparaveis disponiveis; item mantido em monitoramento por valor, fornecedor, orgao e localidade.",
                evidence={"valor_global": str(value)},
            )
        )

    score, risk_level = score_risk(flags)
    return flags, score, risk_level, estimated_variation


def classify_contract_risk(contract: dict[str, Any]) -> tuple[list[RedFlagResult], int, str, Decimal]:
    value = Decimal(str(contract.get("valorGlobal") or 0))
    aditivos = int(contract.get("numeroAditivo") or contract.get("numeroAditivos") or 0)
    object_text = str(contract.get("objeto") or "")
    estimated_variation = Decimal("0")
    flags: list[RedFlagResult] = []

    sensitive_terms = ["computador", "notebook", "combust", "pavimenta", "merenda", "medicamento", "obra"]
    if any(term in object_text.lower() for term in sensitive_terms):
        flags.append(
            RedFlagResult(
                code="RFOBJETO",
                title="Objeto Elegivel para Comparacao Estatistica",
                has_risk=False,
                risk_level="baixo",
                message="Objeto com termo monitorado; a atencao depende de comparacao real por categoria/regiao.",
                evidence={"objeto": object_text[:500]},
                criteria={"rule": "termo sensivel encontrado; sem irregularidade presumida sem baseline comparavel"},
            )
        )

    if aditivos >= 2:
        estimated_variation = max(estimated_variation, value * Decimal("0.08"))
        flags.append(
            RedFlagResult(
                code="RF04",
                title="Aditivos Contratuais Frequentes",
                has_risk=True,
                risk_level="médio",
                message=f"Contrato possui {aditivos} aditivos registrados na fonte oficial; exige conferencia de justificativas e limites legais.",
                evidence={"numero_aditivos": aditivos, "valor_global": str(value)},
                criteria={"rule": "numeroAditivo >= 2"},
            )
        )

    if not flags:
        flags.append(
            RedFlagResult(
                code="RFBASE",
                title="Monitoramento Preventivo",
                has_risk=False,
                risk_level="baixo",
                message="Sem fator forte nos dados comparaveis disponiveis; item mantido em monitoramento por valor, fornecedor, orgao e localidade.",
                evidence={"valor_global": str(value)},
            )
        )

    score, risk_level = score_risk(flags)
    return flags, score, risk_level, estimated_variation


def contract_value(contract: dict[str, Any]) -> Decimal:
    return Decimal(str(contract.get("valorGlobal") or 0))


def contract_category(contract: dict[str, Any]) -> str:
    text = str(contract.get("objeto") or contract.get("nomeCategoria") or "geral").lower()
    for term in ["computador", "notebook", "combust", "pavimenta", "merenda", "medicamento", "limpeza", "serviço", "material"]:
        if term in text:
            return term
    return str(contract.get("nomeCategoria") or "geral").lower()


def estimate_variations_with_ml(contracts: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    values = [float(contract_value(contract)) for contract in contracts]
    if len(values) < 3:
        return {}

    categories: dict[str, list[float]] = {}
    for contract, value in zip(contracts, values):
        categories.setdefault(contract_category(contract), []).append(value)

    global_baseline = mean(values)
    global_deviation = pstdev(values) or 1

    predictions: list[int]
    scores: list[float]
    model_name = "z-score fallback"
    try:
        import pandas as pd
        from sklearn.ensemble import IsolationForest

        frame = pd.DataFrame({"valor_global": values})
        contamination = min(0.25, max(0.05, 2 / len(values)))
        model = IsolationForest(n_estimators=150, contamination=contamination, random_state=42)
        predictions = list(model.fit_predict(frame[["valor_global"]]))
        scores = [float(score) for score in model.decision_function(frame[["valor_global"]])]
        model_name = "IsolationForest"
    except Exception:
        zscores = [(value - global_baseline) / global_deviation for value in values]
        predictions = [-1 if score > 1.4 else 1 for score in zscores]
        scores = zscores

    output: dict[str, dict[str, Any]] = {}
    for contract, value, prediction, score in zip(contracts, values, predictions, scores):
        category_values = categories.get(contract_category(contract), [])
        baseline = mean(category_values) if len(category_values) >= 3 else global_baseline
        deviation = pstdev(category_values) if len(category_values) >= 3 else global_deviation
        deviation = deviation or global_deviation or 1
        z_score = (value - baseline) / deviation
        percent_above = ((value - baseline) / baseline * 100) if baseline > 0 else 0
        excess = max(0.0, value - baseline)
        if excess == 0 and int(prediction) == -1:
            excess = max(0.0, value - global_baseline)
        uncertainty_floor = False
        is_anomaly = z_score > 3.0
        if excess == 0 and value > 0 and not is_anomaly:
            excess = Decimal("0")

        output[contract_stable_key(contract)] = {
            "estimated_variation": Decimal(str(round(excess, 2))),
            "baseline": Decimal(str(round(baseline, 2))),
            "standard_deviation": Decimal(str(round(deviation, 2))),
            "z_score": round(z_score, 4),
            "percent_above_baseline": round(percent_above, 2),
            "sample_size": len(category_values) if len(category_values) >= 3 else len(values),
            "category": contract_category(contract),
            "model": model_name,
            "score": float(score),
            "is_anomaly": is_anomaly,
            "is_uncertainty_floor": uncertainty_floor,
        }
    return output


def compras_contract_url_from_contract(contract: dict[str, Any]) -> str:
    params: dict[str, Any] = {
        "pagina": 1,
        "tamanhoPagina": 10,
    }
    id_compra = contract.get("idCompra")
    if id_compra:
        params["idCompra"] = id_compra
    numero_contrato = contract.get("numeroContrato")
    if numero_contrato:
        params["numeroContrato"] = numero_contrato
    fornecedor = "".join(ch for ch in str(contract.get("niFornecedor") or "") if ch.isdigit())
    if fornecedor:
        params["niFornecedor"] = fornecedor
    codigo_uasg = contract.get("codigoUnidadeGestora")
    if codigo_uasg:
        params["codigoUnidadeGestora"] = codigo_uasg
    contract_date = contract_content_date(contract)
    if contract_date:
        params["dataVigenciaInicialMin"] = contract_date.isoformat()
        params["dataVigenciaInicialMax"] = contract_date.isoformat()
    return f"{COMPRAS_CONTRATOS_URL}?{urlencode(params)}"


def compras_contract_url_from_item(item: MonitoringItem) -> str:
    params: dict[str, Any] = {
        "pagina": 1,
        "tamanhoPagina": 10,
    }
    if item.id:
        params["idCompra"] = item.id
    fornecedor = "".join(ch for ch in str(item.supplier_cnpj or "") if ch.isdigit())
    if fornecedor:
        params["niFornecedor"] = fornecedor
    if item.date:
        params["dataVigenciaInicialMin"] = item.date.isoformat()
        params["dataVigenciaInicialMax"] = item.date.isoformat()
    return f"{COMPRAS_CONTRATOS_URL}?{urlencode(params)}"


def ensure_precise_compras_source(item: MonitoringItem) -> MonitoringItem:
    precise_source = MonitoringSource(
        label=f"Compras.gov.br Dados Abertos - contrato {item.id}",
        url=compras_contract_url_from_item(item),
        kind="API oficial federal com filtros do item",
    )

    sources = list(item.report.official_sources or [])
    if not sources:
        item.report.official_sources = [precise_source]
        return item

    first_url = str(sources[0].url)
    if (
        "dadosabertos.compras.gov.br" in first_url
        and (
            "swagger-ui" in first_url
            or "modulo-contratos/1_consultarContratos" not in first_url
        )
    ):
        sources[0] = precise_source
    item.report.official_sources = sources
    return item


def monitoring_summary_for_contract(
    contract: dict[str, Any],
    flags: list[RedFlagResult],
    risk_level: str,
    value: Decimal,
) -> str:
    object_text = " ".join(str(contract.get("objeto") or contract.get("objetoContrato") or contract.get("objetoCompra") or "").split())
    object_hint = object_text[:80] if object_text else "contrato publico"
    supplier = str(contract.get("nomeRazaoSocialFornecedor") or "fornecedor informado na fonte oficial")
    comparisons: list[str] = []

    for flag in flags:
        evidence = flag.evidence or {}
        percent_above = evidence_number(evidence, "percent_above_baseline")
        sample_size = evidence_number(evidence, "sample_size")
        if percent_above is not None:
            sample_part = f" em {number_text(sample_size) or 'varios'} contratos comparaveis"
            comparisons.append(f"valor {percent_text(percent_above)} acima da media{sample_part}")

        cnpj_age = evidence_number(evidence, "cnpj_age_days")
        if cnpj_age is not None:
            comparisons.append(f"empresa com {number_text(cnpj_age, ' dias')} de abertura na data analisada")

        ratio = evidence_number(evidence, "ratio_contract_to_capital")
        if ratio is not None:
            comparisons.append(f"contrato equivale a {ratio:,.1f}x o capital social declarado".replace(",", "X").replace(".", ",").replace("X", "."))

        distance = evidence_number(evidence, "distance_km")
        if distance is not None:
            comparisons.append(f"sede do fornecedor a cerca de {number_text(distance, ' km')} do orgao contratante")

        related_contracts = evidence_number(evidence, "related_contracts") or evidence_number(evidence, "contracts_in_window")
        total_window = evidence_number(evidence, "total_window_value") or evidence_number(evidence, "window_total_value")
        if related_contracts is not None and total_window is not None:
            comparisons.append(f"{number_text(related_contracts)} contratos proximos somam {money(Decimal(str(total_window)))}")

        matching_records = evidence_number(evidence, "matching_records")
        if matching_records is not None:
            comparisons.append(f"{number_text(matching_records)} registro(s) publico(s) relacionado(s) a socio, pessoa ou empresa")

        supplier_contracts = evidence_number(evidence, "supplier_contracts")
        supplier_total = evidence_number(evidence, "supplier_total_value")
        if supplier_contracts is not None and supplier_total is not None:
            comparisons.append(f"historico do fornecedor tem {number_text(supplier_contracts)} contratos somando {money(Decimal(str(supplier_total)))}")

    unique_comparisons = list(dict.fromkeys(comparisons))[:3]
    risk_sentence = (
        f"Risco {risk_level}: prioridade de leitura definida por comparacoes publicas de valor, "
        "historico de fornecedor, socios, empresa, localidade e orgao."
    )
    if unique_comparisons:
        comparison_sentence = "Principais comparacoes: " + "; ".join(unique_comparisons) + "."
    else:
        comparison_sentence = "Nenhum desvio forte apareceu nos dados comparaveis disponiveis; o item segue monitorado para novas bases publicas."

    return (
        f"{risk_sentence} Item analisado: {object_hint}, fornecedor {supplier}, valor {money(value)}. "
        f"{comparison_sentence} A busca pode cruzar este contexto por produto, pessoa, CNPJ, empresa, partido, STF, cidade, estado ou orgao."
    )


def normalize_attention_flag_text(flag: RedFlagResult) -> RedFlagResult:
    if "Z-Score" in flag.title or "z-score" in flag.title.lower():
        flag.title = "Possivel Superfaturamento por Comparacao de Valores"
        percent_above = evidence_number(flag.evidence or {}, "percent_above_baseline")
        sample_size = evidence_number(flag.evidence or {}, "sample_size")
        if percent_above is not None:
            flag.message = (
                f"Valor global ficou {percent_text(percent_above)} acima da media de "
                f"{number_text(sample_size) or 'varios'} contratos comparaveis."
            )
    if "Contrato real coletado" in flag.message:
        flag.message = (
            "Sem fator forte nos dados comparaveis disponiveis; item mantido em monitoramento "
            "por valor, fornecedor, orgao e localidade."
        )
    return flag


def monitoring_summary_for_item(item: MonitoringItem) -> str:
    comparisons: list[str] = []
    for flag in item.report.red_flags or []:
        evidence = flag.evidence or {}
        percent_above = evidence_number(evidence, "percent_above_baseline")
        sample_size = evidence_number(evidence, "sample_size")
        if percent_above is not None:
            comparisons.append(
                f"valor {percent_text(percent_above)} acima da media de {number_text(sample_size) or 'varios'} contratos comparaveis"
            )

        cnpj_age = evidence_number(evidence, "cnpj_age_days")
        if cnpj_age is not None:
            comparisons.append(f"empresa com {number_text(cnpj_age, ' dias')} de abertura")

        ratio = evidence_number(evidence, "ratio_contract_to_capital")
        if ratio is not None:
            comparisons.append(f"contrato equivale a {ratio:,.1f}x o capital social declarado".replace(",", "X").replace(".", ",").replace("X", "."))

        distance = evidence_number(evidence, "distance_km")
        if distance is not None:
            comparisons.append(f"fornecedor a cerca de {number_text(distance, ' km')} do orgao contratante")

        related_contracts = evidence_number(evidence, "related_contracts") or evidence_number(evidence, "contracts_in_window")
        total_window = evidence_number(evidence, "total_window_value") or evidence_number(evidence, "window_total_value")
        if related_contracts is not None and total_window is not None:
            comparisons.append(f"{number_text(related_contracts)} contratos proximos somam {money(Decimal(str(total_window)))}")

        matching_records = evidence_number(evidence, "matching_records")
        if matching_records is not None:
            comparisons.append(f"{number_text(matching_records)} registro(s) publico(s) relacionado(s) a socio, pessoa ou empresa")

    unique_comparisons = list(dict.fromkeys(comparisons))[:3]
    comparison_sentence = (
        "Principais comparacoes: " + "; ".join(unique_comparisons) + "."
        if unique_comparisons
        else "Nenhum desvio forte apareceu nos dados comparaveis disponiveis; o item segue monitorado para novas bases publicas."
    )
    return (
        f"Risco {item.risk_level}: prioridade de leitura definida por comparacoes publicas de valor, "
        f"historico de fornecedor, socios, empresa, localidade e orgao. Item analisado: {item.title}, "
        f"fornecedor {item.supplier_name or 'nao informado'}, valor {item.formatted_value or money(item.value)}. "
        f"{comparison_sentence} A busca pode cruzar este contexto por produto, pessoa, CNPJ, empresa, partido, STF, cidade, estado ou orgao."
    )


def load_local_monitoring_items(
    q: str | None = None,
    uf: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[MonitoringItem]:
    raw_items: Any = []
    try:
        if data_path_exists(LOCAL_MONITOR_DB_PATH):
            raw_items = read_data_json(LOCAL_MONITOR_DB_PATH, [])
        elif data_path_exists(LOCAL_MONITOR_LATEST_PATH):
            latest = read_data_json(LOCAL_MONITOR_LATEST_PATH, {})
            raw_items = latest.get("items", [])
        if not raw_items:
            snapshots = sorted(Path("data/raw").glob("snapshot-*.json"), reverse=True)
            if snapshots:
                snapshot = json.loads(snapshots[0].read_text(encoding="utf-8"))
                raw_items = [
                    item
                    for page in snapshot.get("feed_pages", [])
                    for item in page.get("items", [])
                ]
    except Exception:
        return []

    if not isinstance(raw_items, list):
        return []

    query_parts = split_related_query(q)
    normalized_queries = [normalize_text(part) for part in query_parts]
    query_digits_parts = ["".join(ch for ch in part if ch.isdigit()) for part in query_parts]
    uf_filter = (uf or "").strip().upper()
    items: list[MonitoringItem] = []

    for raw_item in raw_items:
        try:
            item = MonitoringItem.model_validate(raw_item)
        except Exception:
            continue
        item = ensure_precise_compras_source(item)
        item.report.red_flags = [normalize_attention_flag_text(flag) for flag in item.report.red_flags]
        item.report.summary = monitoring_summary_for_item(item)

        if uf_filter and (item.uf or "").upper() != uf_filter:
            continue

        if date_from and item.date < date_from:
            continue

        if date_to and item.date > date_to:
            continue

        if query_parts:
            searchable = " ".join(
                str(value or "")
                for value in [
                    item.id,
                    item.title,
                    item.entity,
                    item.supplier_name,
                    item.supplier_cnpj,
                    item.object,
                    item.city,
                    item.uf,
                    item.location,
                ]
            )
            searchable_normalized = normalize_text(searchable)
            searchable_digits = "".join(ch for ch in searchable if ch.isdigit())
            matches_text = False
            for normalized_query in normalized_queries:
                query_tokens = [token for token in normalized_query.split() if len(token) >= 2]
                if normalized_query and (
                    normalized_query in searchable_normalized
                    or all(token in searchable_normalized for token in query_tokens[:6])
                ):
                    matches_text = True
                    break
            matches_digits = any(query_digits and query_digits in searchable_digits for query_digits in query_digits_parts)
            if not matches_text and not matches_digits:
                continue

        items.append(item)

    items.sort(key=lambda item: (item.date.isoformat(), item.report.generated_at.isoformat(), item.id), reverse=True)
    return items


def paginate_monitoring_items(items: list[MonitoringItem], page: int, page_size: int) -> tuple[list[MonitoringItem], bool]:
    start = (page - 1) * page_size
    end = start + page_size
    return items[start:end], end < len(items)


def filter_and_order_monitoring_items(
    items: list[MonitoringItem],
    risk_level: str | None = None,
    size_order: str | None = None,
) -> list[MonitoringItem]:
    normalized_risk = (risk_level or "").strip().lower()
    if normalized_risk == "medio":
        normalized_risk = "médio"

    filtered = list(items)
    if normalized_risk and normalized_risk != "todos":
        filtered = [
            item
            for item in filtered
            if (item.risk_level or "").strip().lower() == normalized_risk
        ]

    if size_order == "desc":
        filtered.sort(key=lambda item: (item.value, item.date, item.id), reverse=True)
    elif size_order == "asc":
        filtered.sort(key=lambda item: (item.value, item.date, item.id))
    else:
        filtered.sort(key=lambda item: (item.date.isoformat(), item.report.generated_at.isoformat(), item.id), reverse=True)

    return filtered


def count_json_array(path: Path) -> int:
    if not data_path_exists(path):
        return 0
    try:
        data = read_data_json(path, {})
    except Exception:
        return 0
    return len(data) if isinstance(data, list) else 0


def platform_monitoring_metrics(items: list[MonitoringItem]) -> dict[str, Any]:
    entities = {
        normalize_text(item.entity)
        for item in items
        if normalize_text(item.entity)
    }
    return {
        "total_value": sum((item.value for item in items), Decimal("0")),
        "estimated_variation_total": sum((item.estimated_variation for item in items), Decimal("0")),
        "high_alerts_count": sum(1 for item in items if (item.risk_level or "").strip().lower() == "alto"),
        "monitored_entities_count": len(entities),
    }


def read_library_status(latest_analysis: dict[str, Any] | None = None) -> LibraryStatusResponse:
    records_count = 0
    updated_at = None
    if data_path_exists(PLATFORM_LIBRARY_INDEX_PATH):
        try:
            index = read_data_json(PLATFORM_LIBRARY_INDEX_PATH, {})
            if isinstance(index, dict):
                records_count = int(index.get("total_records") or len(index.get("keys", {})))
                updated_at = index.get("updated_at")
        except Exception:
            records_count = 0

    public_codes_count = 0
    if data_path_exists(PLATFORM_PUBLIC_CODES_PATH):
        try:
            codes = read_data_json(PLATFORM_PUBLIC_CODES_PATH, {})
            if isinstance(codes, dict):
                public_codes_count = sum(
                    len(value) for key, value in codes.items() if isinstance(value, list)
                )
        except Exception:
            public_codes_count = 0

    last_added = 0
    if latest_analysis:
        library = latest_analysis.get("library") or {}
        if isinstance(library, dict):
            last_added = int(library.get("library_records_added") or 0)

    return LibraryStatusResponse(
        records_count=records_count,
        records_added_last_cycle=last_added,
        public_codes_count=public_codes_count,
        library_path=str(PLATFORM_LIBRARY_PATH),
        index_path=str(PLATFORM_LIBRARY_INDEX_PATH),
        public_codes_path=str(PLATFORM_PUBLIC_CODES_PATH),
        updated_at=updated_at,
    )


def normalize_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = re.sub(r"[^A-Za-z0-9]+", " ", text)
    return " ".join(text.upper().split())


def split_related_query(value: str | None) -> list[str]:
    parts = [part.strip() for part in re.split(r"[|;]+", str(value or "")) if part.strip()]
    return parts or ([str(value).strip()] if str(value or "").strip() else [])


SEARCH_TYPE_ORDER = {
    "estado": 10,
    "municipio": 20,
    "politico_relacionado": 30,
    "politico_deputado": 31,
    "politico_senador": 32,
    "cnpj": 40,
    "partido_politico": 50,
    "risco_superfaturamento": 55,
    "contrato": 60,
    "monitoring_item": 61,
    "universal_search_result": 70,
    "stf_processo": 80,
    "stf_jurisprudencia": 81,
}


def search_result_order(result: UniversalSearchResult) -> tuple[int, str]:
    return (SEARCH_TYPE_ORDER.get(result.type, 90), normalize_text(result.title))


def sort_search_results(results: list[UniversalSearchResult]) -> list[UniversalSearchResult]:
    return sorted(results, key=search_result_order)


def search_cache_key(query: str) -> str:
    normalized = normalize_text(query).lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def search_cache_path(query: str) -> Path:
    return SEARCH_CACHE_DIR / f"{search_cache_key(query)}.json"


def load_cached_universal_search(query: str) -> UniversalSearchResponse | None:
    path = search_cache_path(query)
    if not data_path_exists(path):
        return None
    try:
        payload = read_data_json(path, {})
        cached_at = datetime.fromisoformat(str(payload.get("cached_at")).replace("Z", "+00:00"))
        if SEARCH_CACHE_TTL_HOURS > 0 and datetime.now(cached_at.tzinfo) - cached_at > timedelta(hours=SEARCH_CACHE_TTL_HOURS):
            return None
        response_data = payload.get("response")
        if not isinstance(response_data, dict):
            return None
        response = UniversalSearchResponse.model_validate(response_data)
        response.sources = ["Cache local COIBE.IA", *[source for source in response.sources if source != "Cache local COIBE.IA"]]
        return response
    except Exception:
        return None


def save_cached_universal_search(query: str, response: UniversalSearchResponse) -> None:
    payload = {
        "query": query,
        "normalized_query": normalize_text(query),
        "cached_at": brasilia_now().isoformat(),
        "response": response.model_dump(mode="json"),
    }
    write_json(search_cache_path(query), payload)


def superpricing_search_results(q: str, limit: int = 8) -> list[UniversalSearchResult]:
    scored: list[tuple[float, float, UniversalSearchResult]] = []
    for item in load_local_monitoring_items():
        variation = float(item.estimated_variation or 0)
        is_superpricing_risk = variation > 0 or normalize_risk_level(item.risk_level) in {"alto", "médio"}
        if not is_superpricing_risk:
            continue
        text = " ".join(
            str(value or "")
            for value in [
                item.title,
                item.object,
                item.entity,
                item.supplier_name,
                item.supplier_cnpj,
                item.city,
                item.uf,
            ]
        )
        score = similarity(q, text)
        if score < 0.32 and normalize_text(q) not in normalize_text(text):
            continue
        scored.append(
            (
                score,
                variation,
                UniversalSearchResult(
                    type="risco_superfaturamento",
                    title=f"Risco de Superfaturamento - {item.title}",
                    subtitle=(
                        f"{item.entity} - {item.formatted_value} - "
                        f"variação estimada {item.formatted_variation}"
                    ),
                    source="COIBE.IA - análise de superfaturamento",
                    url=item_source_url(item),
                    risk_level=item.risk_level,
                    payload={
                        "id": item.id,
                        "uf": item.uf,
                        "date": item.date.isoformat(),
                        "supplier_cnpj": item.supplier_cnpj,
                        "supplier_name": item.supplier_name,
                        "value": str(item.value),
                        "estimated_variation": str(item.estimated_variation),
                    },
                ),
            )
        )
    scored.sort(key=lambda row: (row[0], row[1]), reverse=True)
    return [result for _, _, result in scored[:limit]]


PRODUCT_STOPWORDS = {
    "A",
    "AS",
    "O",
    "OS",
    "DE",
    "DA",
    "DAS",
    "DO",
    "DOS",
    "E",
    "EM",
    "NO",
    "NA",
    "NOS",
    "NAS",
    "COM",
    "SEM",
    "PARA",
    "POR",
    "TIPO",
    "COR",
    "ITEM",
    "MATERIAL",
    "SERVICO",
    "SERVICOS",
    "AQUISICAO",
    "CONTRATACAO",
}


def normalize_product_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").lower())
    text = "".join(char for char in text if not unicodedata.combining(char))
    unit_patterns = [
        (r"(\d+(?:[,.]\d+)?)\s*(litros|litro|lts|lt|l)\b", r"\1 litro"),
        (r"(\d+(?:[,.]\d+)?)\s*(quilogramas|quilograma|kilos|quilo|kgs|kg)\b", r"\1 kg"),
        (r"(\d+(?:[,.]\d+)?)\s*(mililitros|mililitro|ml)\b", r"\1 ml"),
        (r"(\d+(?:[,.]\d+)?)\s*(metros quadrados|metro quadrado|m2|m²)\b", r"\1 m2"),
        (r"(\d+(?:[,.]\d+)?)\s*(unidades|unidade|unds|und|un)\b", r"\1 un"),
    ]
    for pattern, replacement in unit_patterns:
        text = re.sub(pattern, replacement, text)

    tokens = [
        token
        for token in normalize_text(text).split()
        if token not in PRODUCT_STOPWORDS and len(token) > 1
    ]
    return " ".join(tokens)


def token_set_ratio(left: str, right: str) -> float:
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    if not left_tokens or not right_tokens:
        return 0.0
    intersection = len(left_tokens & right_tokens)
    containment = intersection / min(len(left_tokens), len(right_tokens))
    jaccard = intersection / len(left_tokens | right_tokens)
    return max(containment, jaccard)


def has_token_overlap(left: str, right: str) -> bool:
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    if not left_tokens or not right_tokens:
        return False
    return bool(left_tokens & right_tokens)


def char_ngram_cosine(left: str, right: str, n: int = 3) -> float:
    def ngrams(text: str) -> dict[str, int]:
        compact = re.sub(r"\s+", " ", text.strip())
        if len(compact) < n:
            return {compact: 1} if compact else {}
        output: dict[str, int] = {}
        for index in range(len(compact) - n + 1):
            gram = compact[index : index + n]
            output[gram] = output.get(gram, 0) + 1
        return output

    left_grams = ngrams(left)
    right_grams = ngrams(right)
    if not left_grams or not right_grams:
        return 0.0
    dot = sum(value * right_grams.get(key, 0) for key, value in left_grams.items())
    left_norm = math.sqrt(sum(value * value for value in left_grams.values()))
    right_norm = math.sqrt(sum(value * value for value in right_grams.values()))
    return dot / (left_norm * right_norm) if left_norm and right_norm else 0.0


def public_record_hash(cnpj: str | None, event_date: str | date | None, value: Any) -> str:
    normalized_value = parse_decimal(value) or Decimal("0")
    raw = "|".join(
        [
            "".join(ch for ch in str(cnpj or "") if ch.isdigit()),
            str(event_date or ""),
            str(normalized_value.quantize(Decimal("0.01"))),
        ]
    )
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def load_public_records() -> list[dict[str, Any]]:
    if not data_path_exists(LOCAL_PUBLIC_RECORDS_PATH):
        return []
    try:
        records = read_data_json(LOCAL_PUBLIC_RECORDS_PATH, [])
    except Exception:
        return []
    return [record for record in records if isinstance(record, dict)] if isinstance(records, list) else []


def similarity(left: str, right: str) -> float:
    left_norm = normalize_product_text(left)
    right_norm = normalize_product_text(right)
    if not left_norm or not right_norm:
        return 0.0
    if left_norm in right_norm or right_norm in left_norm:
        return 1.0
    if not has_token_overlap(left_norm, right_norm):
        return 0.0
    return max(
        SequenceMatcher(None, left_norm, right_norm).ratio(),
        token_set_ratio(left_norm, right_norm),
        char_ngram_cosine(left_norm, right_norm),
    )


def haversine_distance_km(origin_lat: float, origin_lng: float, target_lat: float, target_lng: float) -> float:
    radius_km = 6371.0088
    lat1 = math.radians(origin_lat)
    lat2 = math.radians(target_lat)
    delta_lat = math.radians(target_lat - origin_lat)
    delta_lng = math.radians(target_lng - origin_lng)
    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lng / 2) ** 2
    )
    return radius_km * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def requires_physical_presence(activity: str) -> bool:
    normalized = normalize_text(activity)
    physical_terms = [
        "OBRA",
        "CONSTRUCAO",
        "LIMPEZA",
        "JARDINAGEM",
        "PAVIMENTACAO",
        "ASFALTO",
        "MERENDA",
        "TRANSPORTE",
        "MANUTENCAO",
        "SERVICO",
    ]
    return any(term in normalized for term in physical_terms)


def item_source_url(item: MonitoringItem) -> str | None:
    if not item.report.official_sources:
        return None
    return item.report.official_sources[0].url


def save_monitoring_items(items: list[MonitoringItem]) -> tuple[int, int]:
    existing_raw: list[dict[str, Any]] = []
    if data_path_exists(LOCAL_MONITOR_DB_PATH):
        try:
            loaded = read_data_json(LOCAL_MONITOR_DB_PATH, [])
            if isinstance(loaded, list):
                existing_raw = [item for item in loaded if isinstance(item, dict)]
        except Exception:
            existing_raw = []

    by_key = {f"{item.get('id')}:{item.get('date')}": item for item in existing_raw if item.get("id")}
    added = 0
    for item in items:
        raw = item.model_dump(mode="json")
        raw["normalized_title"] = normalize_text(item.title)
        raw["normalized_entity"] = normalize_text(item.entity)
        raw["normalized_supplier"] = normalize_text(item.supplier_name or item.supplier_cnpj)
        raw["coibe_dedup_hash"] = public_record_hash(item.supplier_cnpj, item.date, item.value)
        key = f"{raw.get('id')}:{raw.get('date')}"
        if key not in by_key:
            added += 1
        by_key[key] = raw

    merged = list(by_key.values())
    merged.sort(key=lambda row: (str(row.get("date") or ""), str(row.get("id") or "")), reverse=True)
    write_json(LOCAL_MONITOR_DB_PATH, merged)
    return len(merged), added


def update_platform_codes_from_items(items: list[MonitoringItem]) -> int:
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
    if data_path_exists(PLATFORM_PUBLIC_CODES_PATH):
        try:
            loaded = read_data_json(PLATFORM_PUBLIC_CODES_PATH, {})
            if isinstance(loaded, dict):
                for key in codes:
                    codes[key].update(str(value) for value in loaded.get(key, []) if value)
        except Exception:
            pass

    for item in items:
        if item.supplier_cnpj:
            codes["cnpjs"].add("".join(char for char in item.supplier_cnpj if char.isdigit()))
        if item.uf:
            codes["ufs"].add(item.uf)
        if item.city:
            codes["cities"].add(item.city)
        if item.entity:
            codes["entities"].add(item.entity)
        codes["sources"].add("Compras.gov.br Dados Abertos")
        codes["record_types"].add("monitoring_item")
        source_url = item_source_url(item)
        if source_url:
            codes["urls"].add(source_url)

    output = {key: sorted(value) for key, value in codes.items()}
    output["updated_at"] = brasilia_now().isoformat()
    write_json(PLATFORM_PUBLIC_CODES_PATH, output)
    return sum(len(value) for value in codes.values())


def append_items_to_library(items: list[MonitoringItem], priority: str) -> tuple[int, int]:
    index: dict[str, Any] = {"keys": {}, "total_records": 0}
    if data_path_exists(PLATFORM_LIBRARY_INDEX_PATH):
        try:
            loaded = read_data_json(PLATFORM_LIBRARY_INDEX_PATH, {})
            if isinstance(loaded, dict):
                index = loaded
                index.setdefault("keys", {})
        except Exception:
            pass

    added = 0
    now = brasilia_now().isoformat()
    lines: list[str] = []
    for item in items:
        key = f"monitoring:{item.id}:{item.date}"
        if key in index["keys"]:
            index["keys"][key]["latest_seen_at"] = now
            continue
        added += 1
        library_id = f"LIB-{len(index['keys']) + added:012d}"
        record = {
            "library_id": library_id,
            "library_key": key,
            "library_type": "monitoring_item",
            "priority": priority,
            "first_seen_at": now,
            "latest_seen_at": now,
            "source": "Compras.gov.br Dados Abertos",
            "title": item.title,
            "url": item_source_url(item),
            "public_codes": {
                "cnpj": "".join(char for char in str(item.supplier_cnpj or "") if char.isdigit()),
                "uf": item.uf or "",
                "city": item.city or "",
                "entity": item.entity or "",
            },
            "payload": item.model_dump(mode="json"),
            "normalized_title": normalize_text(item.title),
        }
        lines.append(json.dumps(record, ensure_ascii=False, default=json_default) + "\n")
        index["keys"][key] = {
            "library_id": library_id,
            "library_type": "monitoring_item",
            "first_seen_at": now,
            "latest_seen_at": now,
        }

    if lines:
        append_data_text(PLATFORM_LIBRARY_PATH, "".join(lines))

    index["total_records"] = len(index["keys"])
    index["updated_at"] = now
    write_json(PLATFORM_LIBRARY_INDEX_PATH, index)
    update_platform_codes_from_items(items)
    return int(index["total_records"]), added


async def fetch_compras_contracts(
    page: int,
    page_size: int,
    query: str | None = None,
    content_date_from: date | None = None,
    content_date_to: date | None = None,
) -> list[dict[str, Any]]:
    windows = contract_date_windows(content_date_from, content_date_to)

    clean_query = (query or "").strip()
    query_parts = split_related_query(clean_query)
    if len(query_parts) > 1:
        collected: dict[str, dict[str, Any]] = {}
        for part in query_parts:
            for contract in await fetch_compras_contracts(1, max(page * page_size, page_size), part, content_date_from, content_date_to):
                key = str(contract.get("idCompra") or contract.get("numeroContrato") or contract.get("numeroControlePncpCompra") or "")
                if key:
                    collected[key] = contract
        merged = list(collected.values())
        merged.sort(key=lambda contract: (contract_content_date(contract), str(contract.get("idCompra") or "")), reverse=True)
        start = (page - 1) * page_size
        return merged[start : start + page_size]

    digits = "".join(ch for ch in clean_query if ch.isdigit())
    if clean_query and len(digits) > 14:
        params: dict[str, Any] = {
            "pagina": max(page, 1),
            "tamanhoPagina": max(min(page_size, 100), 10),
            "idCompra": digits,
        }
        data = await get_json(COMPRAS_CONTRATOS_URL, params=params)
        contracts = data.get("resultado", []) if isinstance(data, dict) else []
        if contracts:
            return contracts

    if clean_query and len(digits) not in {11, 14}:
        normalized = normalize_text(clean_query)
        matches: list[dict[str, Any]] = []
        for start_date, end_date in windows:
            for source_page in range(1, 7):
                data = await get_json(
                    COMPRAS_CONTRATOS_URL,
                    params={
                        "pagina": source_page,
                        "tamanhoPagina": 100,
                        "dataVigenciaInicialMin": start_date,
                        "dataVigenciaInicialMax": end_date,
                    },
                )
                contracts = data.get("resultado", []) if isinstance(data, dict) else []
                matches.extend(
                    contract
                    for contract in contracts
                    if normalized
                    and (
                        normalized in normalize_text(contract.get("idCompra"))
                        or normalized in normalize_text(contract.get("numeroContrato"))
                        or normalized in normalize_text(contract.get("objeto"))
                        or normalized in normalize_text(contract.get("nomeRazaoSocialFornecedor"))
                        or normalized in normalize_text(contract.get("nomeOrgao"))
                        or normalized in normalize_text(contract.get("nomeUnidadeGestora"))
                    )
                )
                if len(matches) >= page * page_size:
                    break
            if matches:
                break

        start = (page - 1) * page_size
        return matches[start : start + page_size]

    for start_date, end_date in windows:
        params: dict[str, Any] = {
            "pagina": max(page, 1),
            "tamanhoPagina": max(min(page_size, 100), 10),
            "dataVigenciaInicialMin": start_date,
            "dataVigenciaInicialMax": end_date,
        }

        if len(digits) in {11, 14}:
            params["niFornecedor"] = digits

        data = await get_json(COMPRAS_CONTRATOS_URL, params=params)
        contracts = data.get("resultado", []) if isinstance(data, dict) else []
        if contracts:
            return contracts

    return []


def contract_search_text(contract: dict[str, Any]) -> str:
    unidade = contract.get("unidadeOrgao") if isinstance(contract.get("unidadeOrgao"), dict) else {}
    orgao = contract.get("orgaoEntidade") if isinstance(contract.get("orgaoEntidade"), dict) else {}
    parts = [
        contract.get("idCompra"),
        contract.get("numeroContrato"),
        contract.get("numeroContratoEmpenho"),
        contract.get("numeroControlePNCP"),
        contract.get("numeroControlePncpCompra"),
        contract.get("objeto"),
        contract.get("objetoContrato"),
        contract.get("objetoCompra"),
        contract.get("nomeRazaoSocialFornecedor"),
        contract.get("niFornecedor"),
        contract.get("nomeUnidadeGestora"),
        contract.get("nomeOrgao"),
        unidade.get("nomeUnidade"),
        unidade.get("municipioNome"),
        unidade.get("ufSigla"),
        orgao.get("razaoSocial"),
        orgao.get("cnpj"),
    ]
    return " ".join(str(part or "") for part in parts)


def normalize_pncp_contract(contract: dict[str, Any]) -> dict[str, Any]:
    unidade = contract.get("unidadeOrgao") if isinstance(contract.get("unidadeOrgao"), dict) else {}
    orgao = contract.get("orgaoEntidade") if isinstance(contract.get("orgaoEntidade"), dict) else {}
    normalized = dict(contract)
    normalized["coibe_source"] = "pncp"
    normalized.setdefault("objeto", contract.get("objetoContrato") or contract.get("objetoCompra"))
    normalized.setdefault("valorGlobal", contract.get("valorGlobal") or contract.get("valorInicial") or contract.get("valorTotalHomologado") or contract.get("valorTotalEstimado"))
    normalized.setdefault("dataVigenciaInicial", contract.get("dataVigenciaInicio") or contract.get("dataAssinatura") or contract.get("dataPublicacaoPncp"))
    normalized.setdefault("dataHoraInclusao", contract.get("dataInclusao") or contract.get("dataAtualizacaoGlobal"))
    normalized.setdefault("nomeUnidadeGestora", unidade.get("nomeUnidade"))
    normalized.setdefault("nomeOrgao", orgao.get("razaoSocial"))
    normalized.setdefault("codigoUnidadeGestora", unidade.get("codigoUnidade"))
    normalized.setdefault("numeroContrato", contract.get("numeroContratoEmpenho") or contract.get("sequencialContrato"))
    normalized.setdefault("idCompra", contract.get("numeroControlePNCP") or contract.get("numeroControlePncpCompra"))
    return normalized


def record_fingerprint(parts: list[Any]) -> str:
    raw = "|".join(normalize_text(part) for part in parts)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


async def fetch_pncp_contracts(
    page: int,
    page_size: int,
    query: str | None = None,
    content_date_from: date | None = None,
    content_date_to: date | None = None,
) -> list[dict[str, Any]]:
    windows = contract_date_windows(content_date_from, content_date_to)
    clean_query = (query or "").strip()
    normalized_query = normalize_text(clean_query)
    digits = "".join(ch for ch in clean_query if ch.isdigit())
    collected: list[dict[str, Any]] = []
    max_page_size = max(min(page_size, 100), 10)

    for start_date, end_date in windows:
        params: dict[str, Any] = {
            "dataInicial": pncp_date_param(start_date),
            "dataFinal": pncp_date_param(end_date),
            "pagina": max(page, 1),
            "tamanhoPagina": max_page_size,
        }
        if len(digits) == 14 and not normalized_query:
            params["cnpjOrgao"] = digits

        try:
            data = await get_json(PNCP_CONTRATOS_URL, params=params)
        except HTTPException:
            continue

        contracts = [normalize_pncp_contract(contract) for contract in response_items(data)]
        if clean_query:
            contracts = [
                contract
                for contract in contracts
                if normalized_query in normalize_text(contract_search_text(contract))
                or (digits and digits in "".join(ch for ch in contract_search_text(contract) if ch.isdigit()))
            ]
        collected.extend(contracts)
        if collected:
            break

    return collected[:page_size]


def contract_stable_key(contract: dict[str, Any]) -> str:
    source = str(contract.get("coibe_source") or "compras")
    keys = [
        contract.get("idCompra"),
        contract.get("numeroControlePNCP"),
        contract.get("numeroControlePncpCompra"),
        contract.get("numeroContrato"),
        contract.get("numeroContratoEmpenho"),
        contract.get("sequencialContrato"),
    ]
    stable = next((str(key) for key in keys if key), "")
    if not stable:
        stable = record_fingerprint(
            [
                source,
                contract.get("niFornecedor"),
                contract_content_date(contract).isoformat(),
                contract.get("valorGlobal"),
                contract.get("objeto") or contract.get("objetoContrato"),
            ]
        )
    return stable if source == "compras" else f"{source}:{stable}"


async def fetch_public_contracts(
    page: int,
    page_size: int,
    query: str | None = None,
    content_date_from: date | None = None,
    content_date_to: date | None = None,
) -> list[dict[str, Any]]:
    sources: list[list[dict[str, Any]]] = []
    if "compras" in PUBLIC_CONTRACT_SOURCES:
        try:
            sources.append(await fetch_compras_contracts(page, page_size, query, content_date_from, content_date_to))
        except HTTPException:
            sources.append([])
    if "pncp" in PUBLIC_CONTRACT_SOURCES:
        sources.append(await fetch_pncp_contracts(page, page_size, query, content_date_from, content_date_to))

    merged: dict[str, dict[str, Any]] = {}
    for contracts in sources:
        for contract in contracts:
            merged[contract_stable_key(contract)] = contract

    output = list(merged.values())
    output.sort(key=lambda contract: (contract_content_date(contract), contract_value(contract)), reverse=True)
    return output[:page_size]


async def fetch_uasg_location(codigo_uasg: str | None) -> dict[str, Any] | None:
    if not codigo_uasg:
        return None
    if codigo_uasg in UASG_LOCATION_CACHE:
        return UASG_LOCATION_CACHE[codigo_uasg]
    try:
        data = await get_json(
            COMPRAS_UASG_URL,
            params={"pagina": 1, "codigoUasg": codigo_uasg, "statusUasg": "true"},
        )
    except HTTPException:
        UASG_LOCATION_CACHE[codigo_uasg] = None
        return None

    rows = data.get("resultado", []) if isinstance(data, dict) else []
    UASG_LOCATION_CACHE[codigo_uasg] = rows[0] if rows else None
    return UASG_LOCATION_CACHE[codigo_uasg]


async def fetch_querido_diario_sources(query: str, size: int = 1) -> list[MonitoringSource]:
    try:
        data = await get_json(
            QUERIDO_DIARIO_GAZETTES_URL,
            params={
                "querystring": query,
                "excerpt_size": 500,
                "number_of_excerpts": 1,
                "size": size,
            },
        )
    except HTTPException:
        return []

    sources = []
    for gazette in data.get("gazettes", [])[:size]:
        territory = f"{gazette.get('territory_name', 'Município')} ({gazette.get('state_code', '-')})"
        sources.append(
            MonitoringSource(
                label=f"Querido Diário - {territory} - {gazette.get('date')}",
                url=gazette.get("url") or gazette.get("txt_url") or "https://queridodiario.ok.org.br/",
                kind="Diário Oficial municipal",
            )
        )
    return sources


def coords_for(city: str | None, uf: str | None) -> tuple[float, float]:
    if city:
        normalized_city = city.strip().upper()
        if normalized_city in CITY_COORDS:
            return CITY_COORDS[normalized_city]
    if uf and uf.upper() in UF_CAPITAL_COORDS:
        return UF_CAPITAL_COORDS[uf.upper()]
    return (-14.235, -51.9253)


def parse_company_opening_date(company: dict[str, Any] | None) -> date | None:
    if not company:
        return None
    for key in ("data_inicio_atividade", "abertura", "data_abertura"):
        value = company.get(key)
        if not value:
            continue
        try:
            return parse_brazilian_date(str(value))
        except HTTPException:
            continue
    return None


def parse_company_capital(company: dict[str, Any] | None) -> Decimal | None:
    if not company:
        return None
    value = company.get("capital_social")
    if value in {None, ""}:
        return None
    parsed = parse_decimal(value)
    return parsed if parsed is not None else None


def company_cnae_text(company: dict[str, Any] | None) -> str:
    if not company:
        return ""
    parts = [
        company.get("cnae_fiscal_descricao"),
        company.get("cnae_fiscal"),
    ]
    for activity in company.get("cnaes_secundarios") or []:
        if isinstance(activity, dict):
            parts.extend([activity.get("descricao"), activity.get("codigo")])
    return " ".join(str(part or "") for part in parts)


def company_physical_activity(company: dict[str, Any] | None, object_text: str) -> bool:
    return requires_physical_presence(object_text) or requires_physical_presence(company_cnae_text(company))


def maturity_attention_flag(company: dict[str, Any] | None, supplier_cnpj: str | None, value: Decimal, contract_date: date) -> RedFlagResult | None:
    opening_date = parse_company_opening_date(company)
    capital_social = parse_company_capital(company)
    if not supplier_cnpj or value <= RED_FLAG_01_MIN_CONTRACT_VALUE:
        return None
    if capital_social is not None and capital_social > 0 and value >= capital_social * Decimal("10"):
        return RedFlagResult(
            code="RF01-CAP",
            title="Valor Incompativel com Capital Social",
            has_risk=True,
            risk_level="médio",
            message="Valor do contrato supera em mais de 10 vezes o capital social informado na fonte publica.",
            evidence={
                "supplier_cnpj": supplier_cnpj,
                "capital_social": str(capital_social),
                "valor_global": str(value),
                "ratio_contract_to_capital": round(float(value / capital_social), 2),
            },
            criteria={"rule": "valor_contrato >= 10 x capital_social E valor_contrato > R$ 500.000"},
        )
    if opening_date is None:
        return RedFlagResult(
            code="RF01",
            title="Maturidade do CNPJ Indeterminada",
            has_risk=False,
            risk_level="indeterminado",
            message="Contrato acima do limite de valor, mas a data de abertura do CNPJ nao foi obtida na fonte publica.",
            evidence={"supplier_cnpj": supplier_cnpj, "valor_global": str(value), "company_opening_date": None},
            criteria={"rule": "idade_cnpj < 180 dias E valor_contrato > R$ 500.000"},
        )

    age_days = (contract_date - opening_date).days
    has_risk = age_days < RED_FLAG_01_MAX_AGE_DAYS
    if not has_risk:
        return None
    return RedFlagResult(
        code="RF01",
        title="Maturidade Suspeita do CNPJ",
        has_risk=True,
        risk_level="alto",
        message=f"Empresa vencedora foi aberta ha {age_days} dias corridos antes do contrato de alto valor.",
        evidence={
            "supplier_cnpj": supplier_cnpj,
            "company_opening_date": opening_date.isoformat(),
            "contract_date": contract_date.isoformat(),
            "cnpj_age_days": age_days,
            "valor_global": str(value),
        },
        criteria={"rule": "idade_cnpj < 180 dias E valor_contrato > R$ 500.000"},
    )


def logistic_attention_flag(company: dict[str, Any] | None, item_city: str | None, item_uf: str | None, object_text: str, value: Decimal) -> RedFlagResult | None:
    if not company or not company_physical_activity(company, object_text):
        return None
    company_city = company.get("municipio")
    company_uf = company.get("uf")
    if not company_city or not company_uf or not item_uf:
        return None
    company_lat, company_lng = coords_for(str(company_city), str(company_uf))
    agency_lat, agency_lng = coords_for(item_city, item_uf)
    distance = haversine_distance_km(company_lat, company_lng, agency_lat, agency_lng)
    has_risk = distance > 800 and str(company_uf).upper() != str(item_uf).upper() and value >= Decimal("500000")
    if not has_risk:
        return None
    return RedFlagResult(
        code="RF03",
        title="Inviabilidade Logistica Potencial",
        has_risk=True,
        risk_level="alto",
        message=f"Fornecedor em {company_city}/{company_uf} esta a aproximadamente {distance:.0f} km do orgao em {item_city or 'municipio nao informado'}/{item_uf} para objeto com presenca fisica.",
        evidence={
            "supplier_location": f"{company_city}/{company_uf}",
            "agency_location": f"{item_city or ''}/{item_uf or ''}",
            "distance_km": round(distance, 2),
            "valor_global": str(value),
            "cnae": company_cnae_text(company)[:240],
            "branch_registry_available": False,
        },
        criteria={"rule": "CNAE/objeto fisico E distancia > 800 km E valor >= R$ 500.000; filial local pendente de base cadastral ampliada"},
    )


def fragmentation_attention_flag(
    supplier_cnpj: str | None,
    entity: str,
    contract_date: date,
    value: Decimal,
    object_text: str,
    modality_text: str = "",
) -> RedFlagResult | None:
    supplier_digits = "".join(ch for ch in str(supplier_cnpj or "") if ch.isdigit())
    if not supplier_digits:
        return None
    normalized_modality = normalize_text(modality_text)
    if normalized_modality and "DISPENSA" not in normalized_modality:
        return None
    legal_limit = Decimal("114400") if "obra" in normalize_text(object_text).lower() else Decimal("57200")
    if value > legal_limit:
        return None

    window_start = contract_date - timedelta(days=60)
    related = []
    for item in load_local_monitoring_items():
        if "".join(ch for ch in str(item.supplier_cnpj or "") if ch.isdigit()) != supplier_digits:
            continue
        if normalize_text(item.entity) != normalize_text(entity):
            continue
        if window_start <= item.date <= contract_date and item.value <= legal_limit:
            related.append(item)

    total = sum((item.value for item in related), value)
    if len(related) + 1 < 2 or total <= legal_limit:
        return None
    return RedFlagResult(
        code="RF04-FRAC",
        title="Fracionamento de Despesas Potencial",
        has_risk=True,
        risk_level="alto",
        message=f"{len(related) + 1} contratos do mesmo fornecedor/orgao em 60 dias somam {money(total)}, acima do limite de referencia para dispensa.",
        evidence={
            "supplier_cnpj": supplier_digits,
            "entity": entity,
            "window_days": 60,
            "related_contracts": len(related) + 1,
            "total_window_value": str(total),
            "reference_limit": str(legal_limit),
            "modality": modality_text or "nao informada",
        },
        criteria={"rule": "mesmo fornecedor + mesmo orgao + modalidade dispensa quando disponivel + contratos abaixo do limite em 60 dias + soma acima do limite"},
    )


def conflict_interest_attention_flags(company: dict[str, Any] | None, supplier_cnpj: str | None) -> list[RedFlagResult]:
    supplier_digits = "".join(ch for ch in str(supplier_cnpj or "") if ch.isdigit())
    if not supplier_digits:
        return []
    qsa = company.get("qsa") if isinstance(company, dict) else []
    qsa_terms: set[str] = {supplier_digits}
    for partner in qsa or []:
        if not isinstance(partner, dict):
            continue
        for key in ("cnpj_cpf_do_socio", "cpf_socio", "cpf", "nome_socio", "nome"):
            value = str(partner.get(key) or "").strip()
            digits = "".join(ch for ch in value if ch.isdigit())
            if len(digits) >= 6:
                qsa_terms.add(digits)
            if len(value) >= 5:
                qsa_terms.add(normalize_text(value))

    records = load_public_records()
    matches = []
    for record in records:
        searchable_source = normalize_text(record.get("source"))
        searchable_type = normalize_text(record.get("record_type"))
        if not any(term in f"{searchable_source} {searchable_type}" for term in ("TSE", "DOACAO", "ELEITORAL", "CAMPANHA")):
            continue
        haystack = normalize_text(json.dumps(record, ensure_ascii=False))
        raw_haystack = json.dumps(record, ensure_ascii=False)
        if any(term and (term in haystack or term in raw_haystack) for term in qsa_terms):
            matches.append(record)
    if not matches:
        return []
    return [
        RedFlagResult(
            code="RF05",
            title="Possivel Conflito de Interesses em Doacoes/Relacoes",
            has_risk=True,
            risk_level="alto",
            message="CNPJ do fornecedor apareceu em registro publico eleitoral carregado na plataforma.",
            evidence={"supplier_cnpj": supplier_digits, "matching_records": len(matches), "qsa_terms_checked": len(qsa_terms)},
            criteria={"rule": "CNPJ fornecedor, CPF/socio do QSA ou nome do socio consta em base eleitoral publica carregada"},
        )
    ]


def supplier_sanction_attention_flags(public_evidence: list[dict[str, Any]], supplier_cnpj: str | None) -> list[RedFlagResult]:
    sanction_records = [
        record
        for record in public_evidence
        if str(record.get("record_type") or "") in {"portal_transparencia_ceis", "portal_transparencia_cnep"}
    ]
    if not sanction_records:
        return []
    return [
        RedFlagResult(
            code="RF-SANCAO",
            title="Fornecedor com Registro em Cadastro Publico de Sancoes",
            has_risk=True,
            risk_level="alto",
            message="O fornecedor apareceu em cadastro publico de sancoes consultado no Portal da Transparencia.",
            evidence={
                "supplier_cnpj": supplier_cnpj,
                "matching_records": sum(int(record.get("matches_count") or 0) for record in sanction_records),
                "sources": [record.get("title") for record in sanction_records],
            },
            criteria={"rule": "CNPJ fornecedor consta em CEIS ou CNEP no Portal da Transparencia"},
        )
    ]


def pncp_source_url(contract: dict[str, Any]) -> str:
    numero_controle = contract.get("numeroControlePNCP") or contract.get("numeroControlePncpCompra")
    if numero_controle:
        return f"{PNCP_CONTRATOS_URL}?{urlencode({'dataInicial': pncp_date_param(contract_content_date(contract)), 'dataFinal': pncp_date_param(contract_content_date(contract)), 'pagina': 1, 'tamanhoPagina': 10})}"
    return "https://www.gov.br/pncp/pt-br"


def build_official_sources(
    contract: dict[str, Any],
    public_evidence: list[dict[str, Any]],
    querido_sources: list[MonitoringSource],
) -> list[MonitoringSource]:
    sources: list[MonitoringSource] = []
    if contract.get("numeroControlePNCP") or contract.get("numeroControlePncpCompra") or contract.get("coibe_source") == "pncp":
        sources.append(
            MonitoringSource(
                label="PNCP - contrato/contratacao publica",
                url=pncp_source_url(contract),
                kind="API oficial nacional de contratacoes publicas",
            )
        )
    if contract.get("coibe_source") != "pncp" or contract.get("idCompra") or contract.get("codigoUnidadeGestora"):
        sources.append(
            MonitoringSource(
                label=f"Compras.gov.br Dados Abertos - contrato {contract.get('idCompra') or contract.get('numeroContrato')}",
                url=compras_contract_url_from_contract(contract),
                kind="API oficial federal de compras e contratos",
            )
        )
    for evidence in public_evidence:
        url = str(evidence.get("url") or "")
        if not url:
            continue
        sources.append(
            MonitoringSource(
                label=str(evidence.get("title") or evidence.get("source") or "Fonte publica"),
                url=url,
                kind=str(evidence.get("record_type") or "registro_publico"),
            )
        )
    sources.extend(querido_sources)

    deduped: dict[tuple[str, str], MonitoringSource] = {}
    for source in sources:
        deduped[(source.label, source.url)] = source
    return list(deduped.values())


async def contract_to_monitoring_item(
    contract: dict[str, Any],
    include_diario: bool = True,
    ml_estimate: dict[str, Any] | None = None,
) -> MonitoringItem:
    unidade = contract.get("unidadeOrgao") if isinstance(contract.get("unidadeOrgao"), dict) else {}
    orgao = contract.get("orgaoEntidade") if isinstance(contract.get("orgaoEntidade"), dict) else {}
    location = await fetch_uasg_location(str(contract.get("codigoUnidadeGestora") or ""))
    city = (location.get("nomeMunicipioIbge") if location else None) or unidade.get("municipioNome")
    uf = (location.get("siglaUf") if location else None) or unidade.get("ufSigla")
    contract_date = contract_content_date(contract)
    value = Decimal(str(contract.get("valorGlobal") or 0))
    flags, risk_score, risk_level, estimated_variation = classify_contract_risk(contract)
    supplier_cnpj = contract.get("niFornecedor")
    supplier_digits = "".join(ch for ch in str(supplier_cnpj or "") if ch.isdigit())
    company_details = None
    object_text = str(contract.get("objeto") or contract.get("objetoContrato") or contract.get("objetoCompra") or "")
    if supplier_digits and (value > RED_FLAG_01_MIN_CONTRACT_VALUE or requires_physical_presence(object_text)):
        company_details = await fetch_cnpj_details_cached(supplier_digits)
    public_evidence: list[dict[str, Any]] = []
    if company_details:
        public_evidence.append(
            {
                "source": "Brasil API / Receita Federal",
                "record_type": "cnpj_cadastro",
                "title": "Cadastro publico do fornecedor",
                "url": f"https://brasilapi.com.br/api/cnpj/v1/{supplier_digits}",
                "matches_count": 1,
                "sample": [
                    {
                        "razao_social": company_details.get("razao_social"),
                        "nome_fantasia": company_details.get("nome_fantasia"),
                        "data_inicio_atividade": company_details.get("data_inicio_atividade") or company_details.get("abertura"),
                        "capital_social": company_details.get("capital_social"),
                        "uf": company_details.get("uf"),
                        "municipio": company_details.get("municipio"),
                    }
                ],
            }
        )
    if len(public_evidence) < PUBLIC_DATA_ENRICHMENT_LIMIT:
        public_evidence.extend(await fetch_portal_supplier_evidence(supplier_digits))

    for extra_flag in [
        maturity_attention_flag(company_details, supplier_digits, value, contract_date),
        logistic_attention_flag(company_details, city, uf, str(contract.get("objeto") or contract.get("objetoContrato") or ""), value),
        fragmentation_attention_flag(
            supplier_digits,
            str(contract.get("nomeUnidadeGestora") or contract.get("nomeOrgao") or unidade.get("nomeUnidade") or orgao.get("razaoSocial") or ""),
            contract_date,
            value,
            str(contract.get("objeto") or contract.get("objetoContrato") or ""),
            str(contract.get("modalidadeCompra") or contract.get("modalidade") or contract.get("nomeModalidade") or ""),
        ),
    ]:
        if extra_flag:
            flags.append(extra_flag)
    flags.extend(conflict_interest_attention_flags(company_details, supplier_digits))
    flags.extend(supplier_sanction_attention_flags(public_evidence, supplier_digits))

    if ml_estimate:
        estimated_variation = max(estimated_variation, ml_estimate.get("estimated_variation", Decimal("0")))
        if ml_estimate.get("is_anomaly"):
            flags.append(
                RedFlagResult(
                    code="ML01",
                    title="Possivel Superfaturamento por Comparacao de Valores",
                    has_risk=True,
                    risk_level="alto",
                    message=(
                        f"Valor global ficou {ml_estimate.get('percent_above_baseline')}% acima da media "
                        f"de {ml_estimate.get('sample_size')} contratos comparaveis."
                    ),
                    evidence={
                        "baseline": str(ml_estimate.get("baseline")),
                        "standard_deviation": str(ml_estimate.get("standard_deviation")),
                        "z_score": ml_estimate.get("z_score"),
                        "percent_above_baseline": ml_estimate.get("percent_above_baseline"),
                        "sample_size": ml_estimate.get("sample_size"),
                        "category": ml_estimate.get("category"),
                        "estimated_variation": str(ml_estimate.get("estimated_variation")),
                        "model": ml_estimate.get("model"),
                        "score": ml_estimate.get("score"),
                    },
                    criteria={"rule": "detecção por IsolationForest/z-score sobre contratos coletados"},
                )
            )
        elif ml_estimate.get("is_uncertainty_floor"):
            flags.append(
                RedFlagResult(
                    code="ML00",
                    title="Exposição Estatística Monitorada",
                    has_risk=False,
                    risk_level="baixo",
                    message="Estimativa mínima de variação calculada para manter o contrato sob acompanhamento nacional.",
                    evidence={
                        "baseline": str(ml_estimate.get("baseline")),
                        "estimated_variation": str(ml_estimate.get("estimated_variation")),
                        "model": ml_estimate.get("model"),
                    },
                    criteria={"rule": "3% do valor quando contrato está abaixo do baseline do lote"},
                )
            )
    flags = [normalize_attention_flag_text(flag) for flag in compact_attention_flags(flags)]
    risk_score, risk_level = score_risk(flags)

    source_query = " ".join(object_text.split()[:5]) or "licitacao contrato"
    querido_sources = await fetch_querido_diario_sources(source_query, size=1) if include_diario else []
    official_sources = build_official_sources(contract, public_evidence, querido_sources)
    stable_id = contract_stable_key(contract)
    entity_name = str(
        contract.get("nomeUnidadeGestora")
        or contract.get("nomeOrgao")
        or unidade.get("nomeUnidade")
        or orgao.get("razaoSocial")
        or "Orgao nao informado"
    )

    report = MonitoringReport(
        id=f"COIBE-{stable_id}",
        summary=monitoring_summary_for_contract(contract, flags, risk_level, value),
        risk_score=risk_score,
        risk_level=risk_level,
        red_flags=flags,
        official_sources=official_sources,
        public_evidence=public_evidence,
        generated_at=brasilia_now(),
        ml_model="Regras COIBE.IA + priorização estatística; IsolationForest disponível em /api/analyze-superpricing",
    )

    return MonitoringItem(
        id=stable_id,
        date=contract_date,
        title=str(object_text or "Contrato publico sem descricao")[:140],
        location=f"{city or 'Município não informado'}, {uf or 'UF'}",
        city=city,
        uf=uf,
        entity=entity_name,
        supplier_name=contract.get("nomeRazaoSocialFornecedor"),
        supplier_cnpj=contract.get("niFornecedor"),
        value=value,
        formatted_value=money(value),
        estimated_variation=estimated_variation,
        formatted_variation=money(estimated_variation),
        risk_score=risk_score,
        risk_level=risk_level,
        object=object_text,
        report=report,
    )


async def search_deputies(query: str) -> list[UniversalSearchResult]:
    try:
        data = await get_json(
            CAMARA_DEPUTADOS_URL,
            params={"nome": query, "itens": 10, "ordem": "ASC", "ordenarPor": "nome"},
        )
    except HTTPException:
        return []

    results = []
    for deputy in data.get("dados", []):
        results.append(
            UniversalSearchResult(
                type="politico_deputado",
                title=deputy.get("nome", "Deputado"),
                subtitle=f"{deputy.get('siglaPartido', '-')}/{deputy.get('siglaUf', '-')}",
                source="Câmara dos Deputados - Dados Abertos",
                url=deputy.get("uri"),
                payload=deputy,
            )
        )
    return results


async def search_senators(query: str) -> list[UniversalSearchResult]:
    try:
        data = await get_json(SENADO_SENADORES_URL)
    except HTTPException:
        return []

    normalized = query.strip().lower()
    senators = (
        data.get("ListaParlamentarEmExercicio", {})
        .get("Parlamentares", {})
        .get("Parlamentar", [])
    )
    results = []
    for senator in senators:
        ident = senator.get("IdentificacaoParlamentar", {})
        name = ident.get("NomeParlamentar", "")
        full_name = ident.get("NomeCompletoParlamentar", "")
        if normalized in name.lower() or normalized in full_name.lower():
            mandate = senator.get("Mandato", {})
            uf = mandate.get("UfParlamentar") or ident.get("UfParlamentar") or "-"
            party = ident.get("SiglaPartidoParlamentar", "-")
            results.append(
                UniversalSearchResult(
                    type="politico_senador",
                    title=name or full_name or "Senador",
                    subtitle=f"{party}/{uf}",
                    source="Senado Federal - Dados Abertos",
                    url="https://legis.senado.leg.br/dadosabertos/senador/lista/atual",
                    payload=senator,
                )
            )
    return results[:10]


async def search_political_parties(query: str) -> list[UniversalSearchResult]:
    normalized = normalize_text(query)
    if len(normalized) < 2:
        return []

    params = {"itens": 20, "ordem": "ASC", "ordenarPor": "sigla"}
    if len(normalized) <= 8:
        params["sigla"] = query.strip().upper()

    try:
        data = await get_json(CAMARA_PARTIDOS_URL, params=params)
    except HTTPException:
        return []

    results: list[UniversalSearchResult] = []
    for party in data.get("dados", []):
        sigla = str(party.get("sigla") or "")
        nome = str(party.get("nome") or "")
        if normalized not in normalize_text(sigla) and normalized not in normalize_text(nome):
            continue
        party_id = party.get("id")
        url = party.get("uri") or (f"{CAMARA_PARTIDOS_URL}/{party_id}" if party_id else CAMARA_PARTIDOS_URL)
        results.append(
            UniversalSearchResult(
                type="partido_politico",
                title=f"{sigla} - {nome}".strip(" -"),
                subtitle="Partido político registrado nos Dados Abertos da Câmara",
                source="Câmara dos Deputados - Dados Abertos / Partidos",
                url=url,
                payload=party,
            )
        )
    return results[:10]


async def search_related_political_people(query: str) -> list[UniversalSearchResult]:
    normalized = normalize_text(query)
    if len(normalized) < 3:
        return []

    results: list[UniversalSearchResult] = []
    for person in POLITICAL_RELATED_PEOPLE:
        aliases = [normalize_text(alias) for alias in person["aliases"]]
        title = normalize_text(person["title"])
        if normalized not in title and not any(normalized in alias or alias in normalized for alias in aliases):
            continue
        results.append(
            UniversalSearchResult(
                type="politico_relacionado",
                title=person["title"],
                subtitle=person["subtitle"],
                source="COIBE.IA - busca politica relacionada",
                url=person.get("url"),
                payload={
                    "query": query,
                    "related_queries": person["related_queries"],
                    "kind": "political_related_person",
                },
            )
        )
    return results


def stf_process_link(query: str) -> str:
    normalized = query.strip().upper()
    match = re.search(r"\b(ADI|ADPF|ADC|ADO|RE|ARE|HC|MS|MI|RCL|PET|INQ|AP|ACO)\s*[-/ ]?\s*(\d{1,7})\b", normalized)
    if match:
        return f"{STF_PROCESSOS_URL}?{urlencode({'classe': match.group(1), 'numeroProcesso': match.group(2)})}"
    return f"{STF_PROCESSOS_URL}?{urlencode({'termo': query.strip()})}"


def stf_jurisprudence_link(query: str) -> str:
    params = {
        "base": "acordaos",
        "pesquisa_inteiro_teor": "false",
        "sinonimo": "true",
        "plural": "true",
        "radicais": "false",
        "buscaExata": "false",
        "page": "1",
        "pageSize": "10",
        "queryString": query.strip(),
    }
    return f"{STF_JURISPRUDENCIA_URL}?{urlencode(params)}"


async def search_stf_public(query: str) -> list[UniversalSearchResult]:
    normalized = normalize_text(query)
    stf_terms = {"STF", "SUPREMO", "SUPREMO TRIBUNAL FEDERAL", "ADI", "ADPF", "ADC", "RE", "ARE", "HC", "RCL"}
    looks_like_stf = any(term in normalized for term in stf_terms) or bool(
        re.search(r"\b(ADI|ADPF|ADC|ADO|RE|ARE|HC|MS|MI|RCL|PET|INQ|AP|ACO)\s*[-/ ]?\s*\d{1,7}\b", query.upper())
    )
    if not looks_like_stf:
        return []

    clean_query = re.sub(r"\bSTF\b|\bSUPREMO TRIBUNAL FEDERAL\b|\bSUPREMO\b", "", query, flags=re.IGNORECASE).strip() or query.strip()
    return [
        UniversalSearchResult(
            type="stf_processo",
            title=f"Consulta Processual STF - {clean_query}",
            subtitle="Portal oficial do Supremo Tribunal Federal",
            source="STF - Consulta Processual Pública",
            url=stf_process_link(clean_query),
            payload={"query": clean_query, "kind": "processos_publicos_stf"},
        ),
        UniversalSearchResult(
            type="stf_jurisprudencia",
            title=f"Jurisprudência STF - {clean_query}",
            subtitle="Pesquisa pública de acórdãos e decisões do STF",
            source="STF - Jurisprudência",
            url=stf_jurisprudence_link(clean_query),
            payload={"query": clean_query, "kind": "jurisprudencia_stf"},
        ),
    ]


async def search_states_and_cities(query: str) -> list[UniversalSearchResult]:
    normalized = normalize_text(query)
    results: list[UniversalSearchResult] = []

    try:
        states = await get_json(IBGE_STATES_URL)
        for state in states:
            state_name = normalize_text(state.get("nome", ""))
            state_code = normalize_text(state.get("sigla", ""))
            is_state_code_query = len(normalized) == 2
            if (is_state_code_query and normalized == state_code) or (not is_state_code_query and normalized in state_name):
                results.append(
                    UniversalSearchResult(
                        type="estado",
                        title=state.get("nome", "Estado"),
                        subtitle=f"UF {state.get('sigla')} - Região {state.get('regiao', {}).get('nome', '-')}",
                        source="IBGE Localidades",
                        url="https://servicodados.ibge.gov.br/api/docs/localidades",
                        payload=state,
                    )
                )
    except HTTPException:
        pass

    city = None
    try:
        city = await fetch_city_from_ibge(query)
    except HTTPException:
        city = None
    if city:
        micro = city.get("microrregiao", {})
        meso = micro.get("mesorregiao", {})
        uf = meso.get("UF", {})
        results.append(
            UniversalSearchResult(
                type="municipio",
                title=city.get("nome", "Município"),
                subtitle=f"{uf.get('sigla', '-')}, {uf.get('nome', '-')}",
                source="IBGE Localidades",
                url="https://servicodados.ibge.gov.br/api/docs/localidades",
                payload=city,
            )
        )
    return results[:10]


async def search_cnpj_if_possible(query: str) -> list[UniversalSearchResult]:
    digits = "".join(ch for ch in query if ch.isdigit())
    if len(digits) != 14:
        return []
    try:
        company = await fetch_cnpj_from_brasil_api(digits)
    except HTTPException:
        return []
    return [
        UniversalSearchResult(
            type="cnpj",
            title=company.get("razao_social") or company.get("nome_fantasia") or digits,
            subtitle=f"{company.get('municipio', '-')}/{company.get('uf', '-')} - CNPJ {digits}",
            source="Brasil API / Receita Federal",
            url=f"https://brasilapi.com.br/api/cnpj/v1/{digits}",
            payload=company,
        )
    ]


async def search_contracts_universal(query: str) -> list[UniversalSearchResult]:
    contracts = await fetch_public_contracts(page=1, page_size=10, query=query)
    results = []
    for contract in contracts:
        value = Decimal(str(contract.get("valorGlobal") or 0))
        flags, score, risk_level, _ = classify_contract_risk(contract)
        source = "PNCP" if contract.get("coibe_source") == "pncp" else "Compras.gov.br Dados Abertos"
        results.append(
            UniversalSearchResult(
                type="contrato",
                title=str(contract.get("objeto") or contract.get("objetoContrato") or "Contrato público")[:140],
                subtitle=f"{contract.get('nomeUnidadeGestora') or contract.get('nomeOrgao') or 'Órgão não informado'} - {money(value)}",
                source=source,
                url=pncp_source_url(contract) if contract.get("coibe_source") == "pncp" else compras_contract_url_from_contract(contract),
                risk_level=risk_level,
                payload={
                    **contract,
                    "coibe_risk_score": score,
                    "coibe_red_flags": [flag.model_dump() for flag in flags],
                },
            )
        )
    return results


async def get_state_name_map() -> dict[str, str]:
    try:
        states = await get_json(IBGE_STATES_URL)
    except HTTPException:
        return {}
    return {state.get("sigla"): state.get("nome") for state in states if state.get("sigla")}


async def build_state_risks(page_size: int = 80) -> list[StateRiskPoint]:
    return await build_state_risks_from_source(page_size=page_size, use_local=True)


async def build_state_risks_from_source(page_size: int = 80, use_local: bool = True) -> list[StateRiskPoint]:
    local_items = load_local_monitoring_items() if use_local else []
    if local_items:
        names = await get_state_name_map()
        grouped: dict[str, dict[str, Any]] = {}
        for item in local_items[: max(page_size, len(local_items))]:
            uf = (item.uf or "BR").upper()
            bucket = grouped.setdefault(
                uf,
                {
                    "uf": uf,
                    "state_name": names.get(uf),
                    "risk_score": 0,
                    "total_value": Decimal("0"),
                    "alerts_count": 0,
                },
            )
            bucket["risk_score"] = max(bucket["risk_score"], item.risk_score)
            bucket["total_value"] += item.value
            bucket["alerts_count"] += 1

        points = [StateRiskPoint(**value) for value in grouped.values()]
        points.sort(key=lambda point: (point.risk_score, point.total_value), reverse=True)
        return points

    contracts = await fetch_public_contracts(page=1, page_size=page_size)
    names = await get_state_name_map()
    estimates = estimate_variations_with_ml(contracts)
    grouped: dict[str, dict[str, Any]] = {}

    for contract in contracts:
        item = await contract_to_monitoring_item(
            contract,
            include_diario=False,
            ml_estimate=estimates.get(contract_stable_key(contract)),
        )
        uf = (item.uf or "BR").upper()
        bucket = grouped.setdefault(
            uf,
            {
                "uf": uf,
                "state_name": names.get(uf),
                "risk_score": 0,
                "total_value": Decimal("0"),
                "alerts_count": 0,
            },
        )
        bucket["risk_score"] = max(bucket["risk_score"], item.risk_score)
        bucket["total_value"] += item.value
        bucket["alerts_count"] += 1

    points = [StateRiskPoint(**value) for value in grouped.values()]
    points.sort(key=lambda point: (point.risk_score, point.total_value), reverse=True)
    return points


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse("static/index.html")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/storage/status")
async def storage_status(_admin: None = Depends(require_admin_token)) -> dict[str, Any]:
    if not ENABLE_STORAGE_STATUS:
        raise HTTPException(status_code=404, detail="Diagnostico de storage desativado.")

    expected_files: list[dict[str, Any]] = []
    for relative_key, local_path in S3_DATA_FILES:
        bucket_key = s3_key_for_data_file(relative_key)
        file_status: dict[str, Any] = {
            "local_path": str(local_path),
            "bucket_key": bucket_key,
            "exists": data_path_exists(local_path),
            "size_bytes": None,
        }
        if DATA_S3_SYNC_ENABLED and DATA_S3_BUCKET:
            s3 = s3_client()
            if s3 is not None:
                try:
                    head = s3.head_object(Bucket=DATA_S3_BUCKET, Key=bucket_key)
                    file_status["size_bytes"] = int(head.get("ContentLength") or 0)
                except Exception:
                    pass
        elif local_path.exists():
            file_status["size_bytes"] = local_path.stat().st_size
        expected_files.append(file_status)

    sample_keys: list[str] = []
    is_truncated = False
    if DATA_S3_SYNC_ENABLED and DATA_S3_BUCKET:
        s3 = s3_client()
        if s3 is not None:
            try:
                response = s3.list_objects_v2(
                    Bucket=DATA_S3_BUCKET,
                    Prefix=s3_key_for_data_file(""),
                    MaxKeys=25,
                )
                sample_keys = [item["Key"] for item in response.get("Contents", []) if item.get("Key")]
                is_truncated = bool(response.get("IsTruncated"))
            except Exception as exc:
                sample_keys = [f"Erro ao listar bucket: {exc}"]

    return {
        "mode": "s3" if DATA_S3_SYNC_ENABLED else "local",
        "bucket": DATA_S3_BUCKET or None,
        "prefix": DATA_S3_PREFIX,
        "endpoint_configured": bool(DATA_S3_ENDPOINT_URL),
        "write_through": DATA_S3_WRITE_THROUGH_ENABLED,
        "local_cache": DATA_LOCAL_CACHE_ENABLED,
        "expected_files": expected_files,
        "sample_keys": sample_keys,
        "sample_truncated": is_truncated,
    }


@app.get("/api/sources", response_model=list[PublicDataSourceStatus])
async def public_sources() -> list[PublicDataSourceStatus]:
    return [
        PublicDataSourceStatus(
            name="Portal da Transparência CGU",
            kind="contratos_despesas_servidores_convenios",
            url="https://api.portaldatransparencia.gov.br/swagger-ui/index.html",
            status="ativo" if PORTAL_TRANSPARENCIA_API_KEY else "aguardando PORTAL_TRANSPARENCIA_API_KEY no .env",
            auto_update="consulta online com header chave-api-dados",
            coverage="Contratos, licitações, despesas, convênios, servidores, CEIS, CNEP e notas fiscais federais.",
        ),
        PublicDataSourceStatus(
            name="Compras.gov.br Dados Abertos",
            kind="contratos_compras",
            url=COMPRAS_CONTRATOS_URL,
            status="ativo",
            auto_update="janela automática: 45, 120, 240 dias; fallback para última janela com dados",
            coverage="Contratos, fornecedores, UASG, itens, PNCP e compras públicas federais.",
        ),
        PublicDataSourceStatus(
            name="PNCP API Consulta",
            kind="contratos_contratacoes_atas_nacional",
            url=PNCP_CONTRATOS_URL,
            status="ativo" if "pncp" in PUBLIC_CONTRACT_SOURCES else "desativado em COIBE_PUBLIC_CONTRACT_SOURCES",
            auto_update="consulta online por data de publicacao; consolidada com Compras.gov.br no feed e monitor",
            coverage="Contratos, empenhos, contratacoes, atas e documentos do Portal Nacional de Contratacoes Publicas.",
        ),
        PublicDataSourceStatus(
            name="Câmara dos Deputados Dados Abertos",
            kind="politicos_deputados",
            url="https://dadosabertos.camara.leg.br/",
            status="ativo",
            auto_update="consulta online em tempo real",
            coverage="Deputados, partidos, órgãos, proposições, votações e despesas parlamentares.",
        ),
        PublicDataSourceStatus(
            name="Senado Federal Dados Abertos",
            kind="politicos_senadores",
            url="https://legis.senado.leg.br/dadosabertos/",
            status="ativo",
            auto_update="consulta online em tempo real",
            coverage="Senadores em exercício e dados legislativos do Senado.",
        ),
        PublicDataSourceStatus(
            name="STF Consulta Pública",
            kind="judiciario_stf",
            url=STF_PROCESSOS_URL,
            status="ativo",
            auto_update="link oficial de consulta pública e jurisprudência",
            coverage="Consulta processual pública e jurisprudência do Supremo Tribunal Federal.",
        ),
        PublicDataSourceStatus(
            name="Brasil API CNPJ",
            kind="cnpj_empresas",
            url="https://brasilapi.com.br/",
            status="ativo",
            auto_update="consulta online por CNPJ",
            coverage="Dados cadastrais de CNPJ, data de abertura, capital social e QSA.",
        ),
        PublicDataSourceStatus(
            name="IBGE Localidades",
            kind="estados_municipios",
            url="https://servicodados.ibge.gov.br/api/docs/localidades",
            status="ativo",
            auto_update="consulta online em tempo real",
            coverage="Estados, municípios, regiões e códigos oficiais.",
        ),
        PublicDataSourceStatus(
            name="Querido Diário",
            kind="diarios_oficiais",
            url="https://api.queridodiario.ok.org.br/docs",
            status="ativo",
            auto_update="consulta online por palavra-chave",
            coverage="Diários oficiais municipais indexados pelo projeto.",
        ),
    ]


@app.get("/api/local-monitor/status", response_model=LocalMonitorStatus)
async def local_monitor_status() -> LocalMonitorStatus:
    platform_items = load_local_monitoring_items()
    platform_metrics = platform_monitoring_metrics(platform_items)
    database_items_count = len(platform_items)
    public_records_count = count_json_array(LOCAL_PUBLIC_RECORDS_PATH)
    latest_data: dict[str, Any] | None = None
    if not data_path_exists(LOCAL_MONITOR_LATEST_PATH):
        library_status = read_library_status()
        return LocalMonitorStatus(
            running=False,
            latest_file_exists=False,
            latest_analysis_path=str(LOCAL_MONITOR_LATEST_PATH),
            database_path=str(LOCAL_MONITOR_DB_PATH),
            public_records_path=str(LOCAL_PUBLIC_RECORDS_PATH),
            database_items_count=database_items_count,
            public_records_count=public_records_count,
            library_records_count=library_status.records_count,
            library_records_added=library_status.records_added_last_cycle,
            public_codes_count=library_status.public_codes_count,
            **platform_metrics,
            message="A análise contínua ainda não gerou o primeiro ciclo.",
        )

    try:
        data = read_data_json(LOCAL_MONITOR_LATEST_PATH, {})
        latest_data = data
    except Exception:
        library_status = read_library_status()
        return LocalMonitorStatus(
            running=False,
            latest_file_exists=True,
            latest_analysis_path=str(LOCAL_MONITOR_LATEST_PATH),
            database_path=str(LOCAL_MONITOR_DB_PATH),
            public_records_path=str(LOCAL_PUBLIC_RECORDS_PATH),
            database_items_count=database_items_count,
            public_records_count=public_records_count,
            library_records_count=library_status.records_count,
            library_records_added=library_status.records_added_last_cycle,
            public_codes_count=library_status.public_codes_count,
            **platform_metrics,
            message="A análise existe, mas não pôde ser lida.",
        )

    generated_at = data.get("generated_at")
    running = False
    if generated_at:
        try:
            generated_dt = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
            running = datetime.now(generated_dt.tzinfo) - generated_dt < timedelta(hours=2)
        except ValueError:
            running = False

    library_status = read_library_status(latest_data)
    collector_state = data.get("collector_state") if isinstance(data.get("collector_state"), dict) else {}
    return LocalMonitorStatus(
        running=running,
        latest_file_exists=True,
        latest_analysis_path=str(LOCAL_MONITOR_LATEST_PATH),
        database_path=str(LOCAL_MONITOR_DB_PATH),
        public_records_path=str(LOCAL_PUBLIC_RECORDS_PATH),
        generated_at=generated_at,
        items_analyzed=max(int(data.get("items_analyzed") or 0), database_items_count),
        alerts_count=int(data.get("alerts_count") or 0),
        database_items_count=database_items_count,
        public_records_count=public_records_count,
        library_records_count=library_status.records_count,
        library_records_added=library_status.records_added_last_cycle,
        public_codes_count=library_status.public_codes_count,
        collector_state=collector_state,
        **platform_metrics,
        message="Análise contínua atualizada recentemente." if running else "Análise contínua aguardando nova atualização.",
    )


@app.get("/api/monitoring/status", response_model=LocalMonitorStatus)
async def monitoring_status() -> LocalMonitorStatus:
    return await local_monitor_status()


@app.get("/api/library/status", response_model=LibraryStatusResponse)
async def library_status() -> LibraryStatusResponse:
    latest_data = None
    if data_path_exists(LOCAL_MONITOR_LATEST_PATH):
        try:
            latest_data = read_data_json(LOCAL_MONITOR_LATEST_PATH, {})
        except Exception:
            latest_data = None
    return read_library_status(latest_data)


@app.get("/api/search", response_model=UniversalSearchResponse)
async def universal_search(q: str = Query(..., min_length=2)) -> UniversalSearchResponse:
    cached = load_cached_universal_search(q)
    if cached:
        return cached

    results: list[UniversalSearchResult] = []
    batches = await asyncio.gather(
        search_states_and_cities(q),
        search_deputies(q),
        search_senators(q),
        search_related_political_people(q),
        search_cnpj_if_possible(q),
        search_political_parties(q),
        search_stf_public(q),
        return_exceptions=True,
    )
    for batch in batches:
        if isinstance(batch, list):
            results.extend(batch)
    results.extend(superpricing_search_results(q))
    if not results or any(word in q.lower() for word in ["contrato", "compra", "licitação", "fornecedor", "cnpj"]):
        results.extend(await search_contracts_universal(q))
    results = sort_search_results(results)

    response = UniversalSearchResponse(
        query=q,
        generated_at=brasilia_now(),
        sources=[
            "Brasil API",
            "Câmara dos Deputados Dados Abertos",
            "Senado Federal Dados Abertos",
            "Câmara dos Deputados Dados Abertos - Partidos",
            "STF Consulta Processual/Jurisprudência",
            "IBGE Localidades",
            "Compras.gov.br Dados Abertos",
            "PNCP API Consulta",
            "Portal da Transparencia CGU",
        ],
        results=results[:30],
    )
    save_cached_universal_search(q, response)
    return response


@app.get("/api/search/local", response_model=LocalSearchResponse)
async def local_fuzzy_search(q: str = Query(..., min_length=2), limit: int = Query(10, ge=1, le=30)) -> LocalSearchResponse:
    limit_value = int(limit) if isinstance(limit, int) else 10
    scored: list[tuple[float, UniversalSearchResult]] = []
    risk_results = superpricing_search_results(q, limit=max(3, min(limit_value, 8)))

    for item in load_local_monitoring_items():
        text = " ".join(
            str(value or "")
            for value in [
                item.title,
                item.entity,
                item.supplier_name,
                item.supplier_cnpj,
                item.city,
                item.uf,
                item.object,
            ]
        )
        score = similarity(q, text)
        if score >= 0.35:
            scored.append(
                (
                    score,
                    UniversalSearchResult(
                        type="contrato",
                        title=item.title,
                        subtitle=f"{item.entity} - {item.formatted_value}",
                        source="Índice COIBE.IA",
                        url=item_source_url(item),
                        risk_level=item.risk_level,
                        payload={
                            "id": item.id,
                            "score": score,
                            "uf": item.uf,
                            "supplier_cnpj": item.supplier_cnpj,
                            "coibe_dedup_hash": public_record_hash(item.supplier_cnpj, item.date, item.value),
                        },
                    ),
                )
            )

    for record in load_public_records():
        text = " ".join(
            str(value or "")
            for value in [
                record.get("title"),
                record.get("subtitle"),
                record.get("source"),
                record.get("query"),
                record.get("record_type"),
            ]
        )
        score = similarity(q, text)
        if score >= 0.45:
            scored.append(
                (
                    score,
                    UniversalSearchResult(
                        type=str(record.get("record_type") or "registro_publico"),
                        title=str(record.get("title") or "Registro publico"),
                        subtitle=record.get("subtitle"),
                        source=str(record.get("source") or "Base pública COIBE.IA"),
                        url=record.get("url"),
                        risk_level=str(record.get("risk_level") or "baixo"),
                        payload={**record, "score": score},
                    ),
                )
            )

    scored.sort(key=lambda row: (-row[0], search_result_order(row[1])))
    ordered_results = sort_search_results([*risk_results, *[result for _, result in scored[:limit_value]]])[:limit_value]
    return LocalSearchResponse(
        query=q,
        generated_at=brasilia_now(),
        results=ordered_results,
    )


@app.get("/api/search/autocomplete", response_model=LocalSearchResponse)
async def search_autocomplete(q: str = Query(..., min_length=2), limit: int = Query(10, ge=1, le=20)) -> LocalSearchResponse:
    return await local_fuzzy_search(q=q, limit=limit)


@app.get("/api/search/index", response_model=LocalSearchResponse)
async def indexed_search(q: str = Query(..., min_length=2), limit: int = Query(10, ge=1, le=30)) -> LocalSearchResponse:
    return await local_fuzzy_search(q=q, limit=limit)


@app.get("/api/public-data/portal-transparencia/status")
async def portal_transparencia_status() -> dict[str, Any]:
    return {
        "configured": bool(PORTAL_TRANSPARENCIA_API_KEY),
        "base_url": PORTAL_TRANSPARENCIA_BASE_URL,
        "email_configured": bool(PORTAL_TRANSPARENCIA_EMAIL),
        "header": "chave-api-dados",
        "docs": "https://api.portaldatransparencia.gov.br/swagger-ui/index.html",
    }


@app.get("/api/public-data/portal-transparencia/proxy")
async def portal_transparencia_proxy(
    path: str = Query(..., description="Caminho da API, ex: contratos ou contratos/{id}"),
    _admin: None = Depends(require_admin_token),
) -> Any:
    return await get_portal_transparencia_json(path)


@app.get("/api/monitoring/feed", response_model=MonitoringFeedResponse)
async def monitoring_feed(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=10, le=100),
    q: str | None = Query(None, min_length=2),
    uf: str | None = Query(None, min_length=2, max_length=2),
    risk_level: str | None = Query(None),
    size_order: str | None = Query(None, pattern="^(data|asc|desc)$"),
    date_from: date | None = Query(None, description="Data inicial do conteúdo/contrato, não da análise."),
    date_to: date | None = Query(None, description="Data final do conteúdo/contrato, não da análise."),
    source: str = Query("auto", pattern="^(auto|local|live)$"),
) -> MonitoringFeedResponse:
    if source != "live":
        local_items = load_local_monitoring_items(q=q, uf=uf, date_from=date_from, date_to=date_to)
        if local_items or source == "local":
            local_items = filter_and_order_monitoring_items(local_items, risk_level, size_order)
            items, has_more = paginate_monitoring_items(local_items, page, page_size)
            return MonitoringFeedResponse(
                page=page,
                page_size=page_size,
                has_more=has_more,
                total_returned=len(items),
                sources=[
                    "Base COIBE.IA autoatualizável",
                    "Compras.gov.br Dados Abertos",
                    "Querido Diario",
                    "PNCP",
                    "IBGE UASG/municipios",
                ],
                items=items,
            )

    if uf:
        contracts = await fetch_public_contracts(1, 100, q, date_from, date_to)
        estimates = estimate_variations_with_ml(contracts)
        enriched = await asyncio.gather(*[
            contract_to_monitoring_item(
                contract,
                include_diario=False,
                ml_estimate=estimates.get(contract_stable_key(contract)),
            )
            for contract in contracts
        ])
        filtered = [item for item in enriched if (item.uf or "").upper() == uf.upper()]
        if date_from:
            filtered = [item for item in filtered if item.date >= date_from]
        if date_to:
            filtered = [item for item in filtered if item.date <= date_to]
        filtered = filter_and_order_monitoring_items(filtered, risk_level, size_order)
        start = (page - 1) * page_size
        items = filtered[start : start + page_size]
        has_more = start + page_size < len(filtered)
    else:
        filtered_live_feed = bool(risk_level or size_order or date_from or date_to)
        contracts = await fetch_public_contracts(
            1 if filtered_live_feed else page,
            100 if filtered_live_feed else page_size,
            q,
            date_from,
            date_to,
        )
        estimates = estimate_variations_with_ml(contracts)
        enriched = await asyncio.gather(*[
            contract_to_monitoring_item(
                contract,
                ml_estimate=estimates.get(contract_stable_key(contract)),
            )
            for contract in contracts[: 100 if filtered_live_feed else page_size]
        ])
        if date_from:
            enriched = [item for item in enriched if item.date >= date_from]
        if date_to:
            enriched = [item for item in enriched if item.date <= date_to]
        if filtered_live_feed:
            enriched = filter_and_order_monitoring_items(enriched, risk_level, size_order)
            start = (page - 1) * page_size
            items = enriched[start : start + page_size]
            has_more = start + page_size < len(enriched)
        else:
            items = filter_and_order_monitoring_items(enriched, None, None)
            has_more = len(contracts) >= page_size

    return MonitoringFeedResponse(
        page=page,
        page_size=page_size,
        has_more=has_more,
        total_returned=len(items),
        sources=[
            "Compras.gov.br Dados Abertos",
            "Querido Diário",
            "PNCP",
            "IBGE UASG/municípios",
        ],
        items=items,
    )


@app.get("/api/monitoring/search", response_model=MonitoringFeedResponse)
async def monitoring_search(
    q: str = Query(..., min_length=2),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=10, le=50),
) -> MonitoringFeedResponse:
    return await monitoring_feed(page=page, page_size=page_size, q=q, uf=None)


@app.post("/api/monitoring/priority-scan", response_model=PriorityScanResponse)
async def monitoring_priority_scan(
    q: str | None = Query(None, min_length=2),
    uf: str | None = Query(None, min_length=2, max_length=2),
    pages: int = Query(4, ge=1, le=20),
    page_size: int = Query(50, ge=10, le=100),
    limit: int = Query(60, ge=1, le=200),
    _admin: None = Depends(require_admin_token),
) -> PriorityScanResponse:
    priority = (uf or q or "plataforma").upper()
    collected: dict[str, dict[str, Any]] = {}

    for page in range(1, pages + 1):
        contracts = await fetch_public_contracts(page=page, page_size=page_size, query=q)
        for contract in contracts:
            key = contract_stable_key(contract)
            if key:
                collected[key] = contract

    contracts = list(collected.values())
    estimates = estimate_variations_with_ml(contracts)
    items: list[MonitoringItem] = []
    for contract in contracts:
        key = contract_stable_key(contract)
        item = await contract_to_monitoring_item(
            contract,
            include_diario=False,
            ml_estimate=estimates.get(key),
        )
        if uf and (item.uf or "").upper() != uf.upper():
            continue
        items.append(item)

    items.sort(key=lambda item: (item.risk_score, item.value, item.date), reverse=True)
    selected_items = items[:limit]
    _, items_added = save_monitoring_items(selected_items)
    library_records_count, library_records_added = append_items_to_library(selected_items, priority)

    return PriorityScanResponse(
        generated_at=brasilia_now(),
        priority=priority,
        items_found=len(items),
        items_added=items_added,
        library_records_added=library_records_added,
        library_records_count=library_records_count,
        items=selected_items,
    )


@app.get("/api/monitoring/map", response_model=MonitoringMapResponse)
async def monitoring_map(
    page_size: int = Query(50, ge=10, le=100),
) -> MonitoringMapResponse:
    contracts = await fetch_public_contracts(page=1, page_size=page_size)
    grouped: dict[str, dict[str, Any]] = {}

    for contract in contracts:
        item = await contract_to_monitoring_item(contract, include_diario=False)
        key = f"{item.city or 'Brasil'}-{item.uf or 'BR'}"
        lat, lng = coords_for(item.city, item.uf)
        bucket = grouped.setdefault(
            key,
            {
                "city": item.city or "Brasil",
                "uf": item.uf or "BR",
                "lat": lat,
                "lng": lng,
                "risk_score": 0,
                "total_value": Decimal("0"),
                "alerts_count": 0,
            },
        )
        bucket["risk_score"] = max(bucket["risk_score"], item.risk_score)
        bucket["total_value"] += item.value
        bucket["alerts_count"] += 1

    points = [MonitoringMapPoint(**bucket) for bucket in grouped.values()]
    points.sort(key=lambda point: (point.risk_score, point.total_value), reverse=True)

    return MonitoringMapResponse(
        sources=["Compras.gov.br Dados Abertos", "PNCP", "IBGE UASG/municipios"],
        points=points,
    )


@app.get("/api/monitoring/state-map", response_model=StateMapResponse)
async def monitoring_state_map(
    page_size: int = Query(80, ge=10, le=100),
    source: str = Query("auto", pattern="^(auto|local|live)$"),
) -> StateMapResponse:
    return StateMapResponse(
        sources=["Compras.gov.br Dados Abertos", "PNCP", "IBGE UASG/municipios", "IBGE Malhas Territoriais"],
        states=await build_state_risks_from_source(page_size, use_local=source != "live"),
    )


@app.get("/api/public-data/ibge/states-geojson")
async def ibge_states_geojson() -> dict[str, Any]:
    return await get_json(
        IBGE_STATES_GEOJSON_URL,
        params={
            "formato": "application/vnd.geo+json",
            "qualidade": "intermediaria",
            "intrarregiao": "UF",
        },
    )


@app.get("/api/public-data/cnpj/{cnpj}")
async def public_cnpj_data(cnpj: str) -> dict[str, Any]:
    normalized_cnpj = normalize_cnpj(cnpj)
    return await fetch_cnpj_from_brasil_api(normalized_cnpj)


@app.get("/api/public-data/ibge/city")
async def public_city_data(nome: str = Query(..., min_length=2)) -> dict[str, Any]:
    city = await fetch_city_from_ibge(nome)
    if city is None:
        raise HTTPException(status_code=404, detail="Município não encontrado no IBGE.")
    return city


@app.get("/api/analyze-cnpj/{cnpj}", response_model=CnpjAnalysisResponse)
async def analyze_cnpj(
    cnpj: str,
    valor_contrato: Decimal = Query(
        Decimal("0"),
        ge=0,
        description="Valor do contrato em reais. Exemplo: 750000.00",
    ),
    data_assinatura: date = Query(
        default_factory=brasilia_today,
        description="Data de assinatura do contrato no formato AAAA-MM-DD.",
    ),
) -> CnpjAnalysisResponse:
    normalized_cnpj = normalize_cnpj(cnpj)
    data = await fetch_cnpj_from_brasil_api(normalized_cnpj)

    opening_date = parse_brazilian_date(data.get("data_inicio_atividade"))
    red_flag_01 = apply_red_flag_01(opening_date, valor_contrato, data_assinatura)

    return CnpjAnalysisResponse(
        cnpj=normalized_cnpj,
        razao_social=data.get("razao_social"),
        nome_fantasia=data.get("nome_fantasia"),
        municipio=data.get("municipio"),
        uf=data.get("uf"),
        capital_social=parse_decimal(data.get("capital_social")),
        abertura=opening_date,
        qsa=data.get("qsa") or [],
        red_flags=[red_flag_01],
    )


@app.post("/api/analyze-contract", response_model=ContractAnalysisResponse)
async def analyze_contract(
    payload: ContractAnalysisRequest,
    _admin: None = Depends(require_admin_token),
) -> ContractAnalysisResponse:
    company = await analyze_cnpj(payload.cnpj, payload.valor_contrato, payload.data_assinatura)
    flags = [*company.red_flags, apply_reference_price_flag(payload.objeto, payload.preco_unitario)]

    if payload.cidade:
        try:
            city = await fetch_city_from_ibge(payload.cidade)
        except HTTPException:
            city = None
        if city is None:
            flags.append(
                RedFlagResult(
                    code="RF06",
                    title="Município não validado",
                    has_risk=True,
                    risk_level="médio",
                    message="Município informado não foi confirmado na base pública do IBGE.",
                    evidence={"cidade": payload.cidade},
                )
            )

    risk_score, risk_level = score_risk(flags)
    high_flags = [flag.title for flag in flags if flag.risk_level == "alto"]
    summary = (
        f"Risco {risk_level}. Principais alertas: {', '.join(high_flags)}."
        if high_flags
        else f"Risco {risk_level}. Nenhum alerta alto foi identificado com as fontes atuais."
    )

    return ContractAnalysisResponse(
        contract=payload,
        company=company,
        red_flags=flags,
        risk_score=risk_score,
        risk_level=risk_level,
        summary=summary,
        checked_sources=["Brasil API", "IBGE Localidades", "COIBE reference prices"],
    )


@app.post("/api/analyze-superpricing", response_model=PurchaseAnalysisResponse)
async def analyze_superpricing(
    payload: PurchaseAnalysisRequest,
    _admin: None = Depends(require_admin_token),
) -> PurchaseAnalysisResponse:
    purchases = payload.compras or default_computer_purchases()
    if len(purchases) < 6:
        raise HTTPException(status_code=400, detail="Envie ao menos 6 compras para análise estatística.")
    return analyze_purchase_anomalies(purchases, payload.contamination)


@app.get("/api/analyze-local-superpricing", response_model=LocalSuperpricingResponse)
async def analyze_local_superpricing(
    q: str | None = Query(None, min_length=2),
    uf: str | None = Query(None, min_length=2, max_length=2),
    limit: int = Query(20, ge=1, le=100),
) -> LocalSuperpricingResponse:
    if q:
        all_items = load_local_monitoring_items(uf=uf)
        scored_items = [
            (
                similarity(q, " ".join([item.object or item.title, item.title or ""])),
                item,
            )
            for item in all_items
        ]
        items = [item for score, item in scored_items if score >= 0.42]
        if len(items) < 3:
            items = [item for score, item in scored_items if score >= 0.34]
    else:
        items = load_local_monitoring_items(uf=uf)

    values = [float(item.value or 0) for item in items if float(item.value or 0) > 0]
    if len(values) < 3:
        raise HTTPException(status_code=400, detail="Base insuficiente para comparar preços. Aguarde a coleta contínua reunir mais dados.")

    baseline = mean(values)
    deviation = pstdev(values) or 1
    output: list[LocalSuperpricingItem] = []

    for item in items:
        value = float(item.value or 0)
        z_score = (value - baseline) / deviation
        is_anomaly = z_score >= 3 or item.risk_level == "alto"
        output.append(
            LocalSuperpricingItem(
                id=item.id,
                date=item.date,
                title=item.title,
                entity=item.entity,
                supplier_name=item.supplier_name,
                uf=item.uf,
                value=item.value,
                baseline=Decimal(str(round(baseline, 2))),
                z_score=round(z_score, 4),
                is_anomaly=is_anomaly,
                risk_level="alto" if is_anomaly else item.risk_level,
                source_url=item_source_url(item),
            )
        )

    output.sort(key=lambda item: (not item.is_anomaly, -item.z_score, -float(item.value)))
    return LocalSuperpricingResponse(
        query=q,
        uf=uf.upper() if uf else None,
        items_compared=len(items),
        baseline_average=Decimal(str(round(baseline, 2))),
        standard_deviation=Decimal(str(round(deviation, 2))),
        model="z-score regional sobre base acumulada; pronto para substituir por embeddings persistidos em PostgreSQL com pgvector",
        items=output[:limit],
    )


@app.get("/api/analyze-superpricing/index", response_model=LocalSuperpricingResponse)
async def analyze_indexed_superpricing(
    q: str | None = Query(None, min_length=2),
    uf: str | None = Query(None, min_length=2, max_length=2),
    limit: int = Query(20, ge=1, le=100),
) -> LocalSuperpricingResponse:
    return await analyze_local_superpricing(q=q, uf=uf, limit=limit)


@app.post("/api/analyze-spatial-risk", response_model=SpatialRiskResponse)
async def analyze_spatial_risk(
    payload: SpatialRiskRequest,
    _admin: None = Depends(require_admin_token),
) -> SpatialRiskResponse:
    distance = haversine_distance_km(
        payload.company_lat,
        payload.company_lng,
        payload.agency_lat,
        payload.agency_lng,
    )
    physical = requires_physical_presence(payload.activity)
    high_value = payload.contract_value >= Decimal("500000")
    alert = distance > payload.threshold_km and physical and high_value

    if alert:
        risk_level = "alto"
        message = "Distancia elevada entre fornecedor e orgao para atividade com presenca fisica e contrato de alto valor."
    elif distance > payload.threshold_km and physical:
        risk_level = "médio"
        message = "Distancia elevada para atividade presencial, mas valor abaixo do limite alto."
    else:
        risk_level = "baixo"
        message = "Sem alerta logistico com os parametros informados."

    return SpatialRiskResponse(
        distance_km=round(distance, 2),
        requires_physical_presence=physical,
        alert_logistico=alert,
        risk_level=risk_level,
        message=message,
    )


@app.get("/api/pipeline/readiness", response_model=PipelineReadinessResponse)
async def pipeline_readiness() -> PipelineReadinessResponse:
    return PipelineReadinessResponse(
        storage_mode="platform-index",
        zero_file_storage=True,
        pipeline_order=["connectors_and_scrapers", "raw_database_merge", "risk_rules_and_ml"],
        implemented_now=[
            "coleta sem salvar arquivos baixados",
            "normalizacao de texto e valores",
            "hash MD5 CNPJ+data+valor para deduplicacao",
            "busca fuzzy sobre índice da plataforma para autocomplete",
            "z-score sobre base acumulada",
            "calculo espacial por Haversine para alerta logistico",
        ],
        production_targets=[
            "PostgreSQL com particionamento por ano/UF",
            "embeddings persistidos em PostgreSQL com pgvector",
            "distancia geografica no PostGIS com ST_DistanceSphere",
            "OpenSearch/Elasticsearch para indice invertido em producao",
            "worker Celery/RabbitMQ para sincronizacao incremental",
        ],
    )


@app.post("/api/scrape/public-page", response_model=ScrapeResponse)
async def scrape_public_page(
    payload: ScrapeRequest,
    _admin: None = Depends(require_admin_token),
) -> ScrapeResponse:
    final_url, response_text = await fetch_public_page_text(str(payload.url))

    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(response_text, "html.parser")
        title = soup.title.string.strip() if soup.title and soup.title.string else None
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = " ".join(soup.get_text(" ").split())
    except Exception:
        title = None
        text = " ".join(response_text.split())

    lowered = text.lower()
    hits = {keyword: lowered.count(keyword.lower()) for keyword in payload.keywords}

    return ScrapeResponse(
        url=final_url,
        title=title,
        text_excerpt=text[:3000],
        keyword_hits=hits,
    )
