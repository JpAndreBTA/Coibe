import os
import asyncio
import hashlib
import hmac
import ipaddress
import math
import re
import site
import signal
import socket
import subprocess
import sys
import time
import unicodedata
import warnings
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
from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, HttpUrl

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
IBGE_MUNICIPALITY_GEOJSON_URL = "https://servicodados.ibge.gov.br/api/v3/malhas/municipios/{city_id}"
OSM_NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
COMPRAS_CONTRATOS_URL = "https://dadosabertos.compras.gov.br/modulo-contratos/1_consultarContratos"
COMPRAS_UASG_URL = "https://dadosabertos.compras.gov.br/modulo-uasg/1_consultarUasg"
QUERIDO_DIARIO_GAZETTES_URL = "https://api.queridodiario.ok.org.br/gazettes"
CAMARA_DEPUTADOS_URL = "https://dadosabertos.camara.leg.br/api/v2/deputados"
CAMARA_PARTIDOS_URL = "https://dadosabertos.camara.leg.br/api/v2/partidos"
SENADO_SENADORES_URL = "https://legis.senado.leg.br/dadosabertos/senador/lista/atual.json"
STF_PROCESSOS_URL = "https://portal.stf.jus.br/processos/listarProcessos.asp"
STF_JURISPRUDENCIA_URL = "https://jurisprudencia.stf.jus.br/pages/search"
TSE_PARTIDOS_URL = "https://www.tse.jus.br/partidos"
TSE_CONTAS_ELEITORAIS_URL = "https://divulgacandcontas.tse.jus.br/divulga/"
TCU_PROCESSOS_URL = "https://pesquisa.apps.tcu.gov.br/#/pesquisa/processo"
IBGE_STATES_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
IBGE_STATES_GEOJSON_URL = "https://servicodados.ibge.gov.br/api/v3/malhas/paises/BR"
POLITICAL_HIGH_VALUE_CONTRACT_THRESHOLD = Decimal(os.getenv("COIBE_POLITICAL_HIGH_VALUE_CONTRACT_THRESHOLD", "1000000"))
POLITICAL_HIGH_VALUE_PERSON_THRESHOLD = Decimal(os.getenv("COIBE_POLITICAL_HIGH_VALUE_PERSON_THRESHOLD", "250000"))
MODELS_DIR = Path(os.getenv("COIBE_MODELS_DIR", "Models"))
MONITOR_MODEL_STATE_PATH = MODELS_DIR / "monitor_model_state.json"
MONITOR_MODEL_TRAINING_PATH = MODELS_DIR / "monitor_training_history.jsonl"
MONITOR_MODEL_MEMORY_PATH = MODELS_DIR / "coibe_adaptive_memory.jsonl"
MONITOR_MODEL_CONFIG_PATH = MODELS_DIR / "monitor_config.json"
MONITOR_MODEL_REGISTRY_PATH = MODELS_DIR / "model_registry.json"
MONITOR_SELECTED_MODEL_ID = os.getenv("COIBE_MONITOR_SELECTED_MODEL_ID", "coibe-adaptive-default")
COIBE_ML_USE_GPU = os.getenv("COIBE_ML_USE_GPU", "false").lower() in {"1", "true", "yes"}
COIBE_GPU_MEMORY_LIMIT_MB = int(os.getenv("COIBE_GPU_MEMORY_LIMIT_MB", "2048"))
COIBE_ML_USE_SHARED_MEMORY = os.getenv("COIBE_ML_USE_SHARED_MEMORY", "true").lower() in {"1", "true", "yes"}
COIBE_SHARED_MEMORY_LIMIT_MB = int(os.getenv("COIBE_SHARED_MEMORY_LIMIT_MB", "4096"))
CUDA_DLL_HANDLES: list[Any] = []
FALLBACK_CONTRACTS_START = "2025-09-01"
FALLBACK_CONTRACTS_END = "2025-09-30"
AUTO_CONTRACT_WINDOWS_DAYS = (45, 120, 240)
RED_FLAG_01_MAX_AGE_DAYS = 180
RED_FLAG_01_MIN_CONTRACT_VALUE = Decimal("500000")
LOGISTIC_DISTANCE_THRESHOLD_KM = 1000
LOGISTIC_MIN_CONTRACT_VALUE = Decimal("500000")
LOGISTIC_FIXED_PRESENCE_TERMS = (
    "OBRA",
    "CONSTRUCAO",
    "REFORMA",
    "AMPLIACAO",
    "ENGENHARIA",
    "PAVIMENTACAO",
    "ASFALTO",
    "MANUTENCAO PREDIAL",
    "MANUTENCAO PREVENTIVA",
    "MANUTENCAO CORRETIVA",
    "INSTALACAO",
    "IMPLANTACAO",
    "LIMPEZA",
    "CONSERVACAO",
    "JARDINAGEM",
    "VIGILANCIA",
    "SEGURANCA PATRIMONIAL",
    "CONTROLE DE ACESSO",
    "PORTARIA",
    "TERCEIRIZACAO DE MAO DE OBRA",
    "MAO DE OBRA DEDICADA",
    "POSTOS FIXOS",
    "MERENDA ESCOLAR",
    "ALIMENTACAO ESCOLAR",
)
LOGISTIC_MOBILE_OR_DELIVERY_TERMS = (
    "TRANSPORTE",
    "FRETE",
    "ENTREGA",
    "DISTRIBUICAO",
    "LOGISTICA",
    "MUDANCA",
    "REMOCAO",
    "DESLOCAMENTO",
    "VIAGEM",
    "PASSAGEM",
    "LOCOMOCAO",
    "MOTORISTA",
    "VEICULO",
    "VEICULOS",
    "ONIBUS",
    "AMBULANCIA",
    "CAMINHAO",
    "COLETA",
    "CORREIOS",
    "POSTAGEM",
)
LOGISTIC_SUPPLY_ONLY_TERMS = (
    "AQUISICAO",
    "COMPRA",
    "FORNECIMENTO",
    "MATERIAL",
    "MATERIAIS",
    "EQUIPAMENTO",
    "EQUIPAMENTOS",
    "INSUMO",
    "INSUMOS",
    "GENEROS ALIMENTICIOS",
    "MEDICAMENTO",
    "MEDICAMENTOS",
)
UASG_LOCATION_CACHE: dict[str, dict[str, Any] | None] = {}
CNPJ_DETAILS_CACHE: dict[str, dict[str, Any] | None] = {}
IBGE_CITIES_CACHE: dict[str, Any] = {"loaded_at": 0.0, "items": []}
IBGE_CITIES_CACHE_TTL_SECONDS = int(os.getenv("COIBE_IBGE_CITIES_CACHE_TTL_SECONDS", "86400"))
LOCAL_MONITOR_LATEST_PATH = Path(os.getenv("COIBE_MONITOR_LATEST_PATH", "data/processed/latest_analysis.json"))
LOCAL_MONITOR_DB_PATH = Path(os.getenv("COIBE_MONITOR_DB_PATH", "data/processed/monitoring_items.json"))
LOCAL_PUBLIC_RECORDS_PATH = Path(os.getenv("COIBE_PUBLIC_RECORDS_PATH", "data/processed/public_api_records.json"))
PUBLIC_RECORD_FEED_LIMIT = int(os.getenv("COIBE_PUBLIC_RECORD_FEED_LIMIT", "300"))
PUBLIC_RECORD_FEED_REFERENCE_TYPES = {
    "municipio",
    "estado",
    "localidade",
    "ibge_localidade",
    "ibge_municipio",
}
PUBLIC_RECORD_FEED_FINANCIAL_TYPES = {
    "risco_superfaturamento",
    "overprice_risk",
    "price_risk",
    "public_contract",
    "contract",
    "expense",
    "empenho",
}
PLATFORM_LIBRARY_PATH = Path(os.getenv("COIBE_LIBRARY_PATH", "data/library/library_records.jsonl"))
PLATFORM_LIBRARY_INDEX_PATH = Path(os.getenv("COIBE_LIBRARY_INDEX_PATH", "data/library/library_index.json"))
PLATFORM_PUBLIC_CODES_PATH = Path(os.getenv("COIBE_PUBLIC_CODES_PATH", "data/library/public_codes.json"))
SEARCH_CACHE_DIR = Path(os.getenv("COIBE_SEARCH_CACHE_DIR", "data/cache/search"))
SEARCH_CACHE_TTL_HOURS = float(os.getenv("COIBE_SEARCH_CACHE_TTL_HOURS", "24"))
IBGE_STATES_GEOJSON_CACHE_PATH = Path(os.getenv("COIBE_IBGE_STATES_GEOJSON_CACHE_PATH", "data/cache/ibge-states-geojson.json"))
MONITORING_MAP_CACHE_PATH = Path(os.getenv("COIBE_MONITORING_MAP_CACHE_PATH", "data/cache/monitoring-map.json"))
MONITORING_STATE_MAP_CACHE_PATH = Path(os.getenv("COIBE_MONITORING_STATE_MAP_CACHE_PATH", "data/cache/monitoring-state-map.json"))
CITY_COORDINATES_CACHE_PATH = Path(os.getenv("COIBE_CITY_COORDINATES_CACHE_PATH", "data/cache/city-coordinates.json"))
MAP_POINTS_CACHE_VERSION = 3
CITY_COORDINATES_LOOKUP_LIMIT = int(os.getenv("COIBE_CITY_COORDINATES_LOOKUP_LIMIT", "500"))
CITY_GEOCODING_DELAY_SECONDS = float(os.getenv("COIBE_CITY_GEOCODING_DELAY_SECONDS", "0.25"))
API_RESPONSE_CACHE_TTL_SECONDS = int(os.getenv("COIBE_API_RESPONSE_CACHE_TTL_SECONDS", "20"))
API_RESPONSE_CACHE_MAX_ENTRIES = int(os.getenv("COIBE_API_RESPONSE_CACHE_MAX_ENTRIES", "512"))
PUBLIC_API_CACHE_TTL_SECONDS = int(os.getenv("COIBE_PUBLIC_API_CACHE_TTL_SECONDS", "180"))
PUBLIC_API_STALE_TTL_SECONDS = int(os.getenv("COIBE_PUBLIC_API_STALE_TTL_SECONDS", "86400"))
PUBLIC_API_ERROR_COOLDOWN_SECONDS = int(os.getenv("COIBE_PUBLIC_API_ERROR_COOLDOWN_SECONDS", "120"))
PUBLIC_API_MIN_INTERVAL_SECONDS = float(os.getenv("COIBE_PUBLIC_API_MIN_INTERVAL_SECONDS", "0.25"))
RATE_LIMIT_BUCKET_MAX_KEYS = int(os.getenv("COIBE_RATE_LIMIT_BUCKET_MAX_KEYS", "5000"))
MAX_REQUEST_BODY_BYTES = int(os.getenv("COIBE_MAX_REQUEST_BODY_BYTES", str(256 * 1024)))
MAX_QUERY_STRING_BYTES = int(os.getenv("COIBE_MAX_QUERY_STRING_BYTES", "2048"))
TRUST_X_FORWARDED_FOR = os.getenv("COIBE_TRUST_X_FORWARDED_FOR", "false").lower() in {"1", "true", "yes"}
MAP_CACHE_TTL_SECONDS = int(os.getenv("COIBE_MAP_CACHE_TTL_SECONDS", "120"))
POSTGIS_DATABASE_URL = os.getenv("COIBE_POSTGIS_DATABASE_URL", os.getenv("DATABASE_URL", "")).strip()
POSTGIS_ENABLED = bool(POSTGIS_DATABASE_URL) and os.getenv("COIBE_POSTGIS_ENABLED", "true").lower() not in {"0", "false", "no"}
POSTGIS_SCHEMA_READY = False
AUTO_MONITOR_ENABLED = os.getenv("COIBE_AUTO_MONITOR", "true").lower() not in {"0", "false", "no"}
AUTO_MONITOR_INTERVAL_MINUTES = float(os.getenv("COIBE_AUTO_MONITOR_INTERVAL_MINUTES", "15"))
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
CORS_ORIGIN_REGEX = os.getenv(
    "COIBE_CORS_ORIGIN_REGEX",
    "" if IS_PRODUCTION else r"http://localhost(:\d+)?|http://127\.0\.0\.1(:\d+)?",
).strip() or None
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
        "subtitle": "Presidente da Republica - prioridade de monitoramento",
        "role": "Presidente da Republica",
        "party": "PT",
        "priority_score": 100,
        "related_queries": ["Lula", "Luiz Inacio Lula da Silva", "PT", "Presidencia", "Governo Federal"],
        "url": "https://www.gov.br/planalto/",
    },
    {
        "title": "Geraldo Alckmin",
        "aliases": ["geraldo alckmin", "alckmin", "geraldo jose rodrigues alckmin filho"],
        "subtitle": "Vice-presidente da Republica - prioridade de monitoramento",
        "role": "Vice-presidente da Republica",
        "party": "PSB",
        "priority_score": 95,
        "related_queries": ["Geraldo Alckmin", "Alckmin", "PSB", "Vice-Presidencia", "MDIC"],
        "url": "https://www.gov.br/planalto/pt-br/vice-presidencia",
    },
    {
        "title": "Jair Messias Bolsonaro",
        "aliases": ["jair bolsonaro", "bolsonaro", "jair messias bolsonaro"],
        "subtitle": "Ex-presidente da Republica - prioridade de monitoramento historico",
        "role": "Ex-presidente da Republica",
        "party": "PL",
        "priority_score": 80,
        "related_queries": ["Jair Bolsonaro", "Bolsonaro", "PL", "Presidencia", "Governo Federal"],
        "url": "https://www.tse.jus.br/",
    },
]

POLITICAL_PRIORITY_PARTIES = {
    "PT": 100,
    "PSB": 85,
    "PL": 75,
}


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

UF_NAMES = {
    "AC": "Acre",
    "AL": "Alagoas",
    "AM": "Amazonas",
    "AP": "Amapa",
    "BA": "Bahia",
    "CE": "Ceara",
    "DF": "Distrito Federal",
    "ES": "Espirito Santo",
    "GO": "Goias",
    "MA": "Maranhao",
    "MG": "Minas Gerais",
    "MS": "Mato Grosso do Sul",
    "MT": "Mato Grosso",
    "PA": "Para",
    "PB": "Paraiba",
    "PE": "Pernambuco",
    "PI": "Piaui",
    "PR": "Parana",
    "RJ": "Rio de Janeiro",
    "RN": "Rio Grande do Norte",
    "RO": "Rondonia",
    "RR": "Roraima",
    "RS": "Rio Grande do Sul",
    "SC": "Santa Catarina",
    "SE": "Sergipe",
    "SP": "Sao Paulo",
    "TO": "Tocantins",
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
    keywords: list[str] = Field(default_factory=list, max_length=25)


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
    spatial_source: str = "ibge_uasg_centroid"


class MonitoringMapResponse(BaseModel):
    sources: list[str]
    points: list[MonitoringMapPoint]
    generated_at: datetime | None = None
    cache_status: str = "live"
    postgis_enabled: bool = False
    total_returned: int = 0


class StateRiskPoint(BaseModel):
    uf: str
    state_name: str | None = None
    risk_score: int
    total_value: Decimal
    alerts_count: int


class StateMapResponse(BaseModel):
    sources: list[str]
    states: list[StateRiskPoint]
    generated_at: datetime | None = None
    cache_status: str = "live"


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
    from_cache: bool = False
    cache_status: str = "live"
    cached_at: datetime | None = None
    public_api_checked: bool = True


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
    model_status: dict[str, Any] = Field(default_factory=dict)
    autonomous_agents: dict[str, Any] = Field(default_factory=dict)
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


class MonitorModelConfig(BaseModel):
    use_gpu: bool = False
    gpu_memory_limit_mb: int = Field(default=2048, ge=256, le=49152)
    use_shared_memory: bool = True
    shared_memory_limit_mb: int = Field(default=4096, ge=512, le=131072)
    scan_profile: str = Field(default="balanced", pattern="^(conservative|balanced|heavy|no-delay-heavy)$")
    max_concurrent_requests: int = Field(default=4, ge=1, le=24)
    public_api_concurrency: int = Field(default=3, ge=1, le=8)
    public_api_source_mode: str = Field(default="hybrid", pattern="^(live|hybrid|cache-first)$")
    internet_sweep_concurrency: int = Field(default=4, ge=1, le=24)
    gpu_acceleration_level: str = Field(default="balanced", pattern="^(off|balanced|aggressive)$")
    training_sample_limit: int = Field(default=1000, ge=100, le=10000)
    analysis_batch_size: int = Field(default=256, ge=16, le=5000)
    learning_batch_size: int = Field(default=512, ge=16, le=10000)
    selected_model_id: str = "coibe-adaptive-default"
    deep_learning_enabled: bool = True
    quantization_mode: str = "dynamic-int8"
    model_format: str = "joblib+onnx-manifest+quant-manifest"
    internet_sweep_enabled: bool = True
    web_fallback_enabled: bool = True
    internet_sweep_pages_per_cycle: int = Field(default=6, ge=0, le=50)
    internet_sweep_timeout_seconds: int = Field(default=12, ge=3, le=120)
    research_timeout_seconds: int = Field(default=90, ge=15, le=900)
    research_rounds: int = Field(default=10, ge=1, le=100)
    feed_page_size: int = Field(default=50, ge=10, le=100)
    political_party_scan_limit: int = Field(default=12, ge=1, le=24)
    political_people_scan_limit: int = Field(default=24, ge=1, le=36)
    learned_terms_per_cycle: int = Field(default=12, ge=1, le=100)
    search_terms_per_cycle: int = Field(default=8, ge=0, le=60)
    search_delay_seconds: float = Field(default=2.0, ge=0, le=60)
    priority_feed_queries_per_cycle: int = Field(default=4, ge=0, le=20)


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


class PoliticalRiskFactor(BaseModel):
    level: str
    title: str
    message: str
    evidence: dict[str, Any] = Field(default_factory=dict)
    source: str
    url: str | None = None


class PoliticalScanItem(BaseModel):
    id: str
    type: str
    name: str
    subtitle: str | None = None
    party: str | None = None
    role: str | None = None
    uf: str | None = None
    total_public_money: Decimal = Decimal("0")
    travel_public_money: Decimal = Decimal("0")
    records_count: int = 0
    attention_level: str = "baixo"
    summary: str
    people: list[str] = Field(default_factory=list)
    analysis_types: list[str] = Field(default_factory=list)
    analysis_details: list[dict[str, Any]] = Field(default_factory=list)
    analyzed_at: datetime | None = None
    priority_score: int = 0
    priority_reason: str | None = None
    sources: list[MonitoringSource] = Field(default_factory=list)
    risks: list[PoliticalRiskFactor] = Field(default_factory=list)


class PoliticalScanResponse(BaseModel):
    generated_at: datetime
    kind: str
    sources: list[str]
    items: list[PoliticalScanItem]
    page: int = 1
    page_size: int = 0
    has_more: bool = False
    total_returned: int = 0


RATE_LIMIT_BUCKETS: dict[tuple[str, str], list[float]] = {}
API_RESPONSE_CACHE: dict[str, tuple[float, Any]] = {}
API_RESPONSE_LOCKS: dict[str, asyncio.Lock] = {}
PUBLIC_API_CACHE: dict[str, tuple[float, float, Any]] = {}
PUBLIC_API_LOCKS: dict[str, asyncio.Lock] = {}
PUBLIC_API_HOST_LOCKS: dict[str, asyncio.Lock] = {}
PUBLIC_API_HOST_LAST_CALL: dict[str, float] = {}
PUBLIC_API_HOST_COOLDOWN: dict[str, float] = {}
POLITICAL_LOCAL_ITEMS_CACHE: dict[str, Any] = {"loaded_at": 0.0, "items": []}
POLITICAL_BACKGROUND_REFRESHES: set[str] = set()
SENATORS_PUBLIC_CACHE: dict[str, Any] = {"loaded_at": 0.0, "items": []}
SENATORS_PUBLIC_CACHE_TTL_SECONDS = int(os.getenv("COIBE_SENATORS_CACHE_TTL_SECONDS", "21600"))
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
    "/api/political/parties",
    "/api/political/politicians",
}


def client_ip_from_request(request: Request) -> str:
    forwarded = request.headers.get("cf-connecting-ip")
    if not forwarded and TRUST_X_FORWARDED_FOR:
        forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",", 1)[0].strip()
    return request.client.host if request.client else "unknown"


def request_host_name(request: Request) -> str:
    host = (request.headers.get("host") or "").strip().lower()
    if host.startswith("[") and "]" in host:
        return host[1 : host.index("]")]
    return host.split(":", 1)[0]


def is_strict_local_request(request: Request) -> bool:
    host = request_host_name(request)
    if host not in {"127.0.0.1", "localhost", "::1"}:
        return False
    try:
        peer_ip = ipaddress.ip_address(request.client.host if request.client else "")
    except ValueError:
        return False
    return peer_ip.is_loopback


def check_rate_limit(request: Request) -> None:
    if not RATE_LIMIT_ENABLED:
        return
    if is_strict_local_request(request) and request.headers.get("x-coibe-monitor") == "local-monitor":
        return

    now = time.monotonic()
    window = max(RATE_LIMIT_WINDOW_SECONDS, 1)
    limit = RATE_LIMIT_HEAVY_REQUESTS if request.url.path in HEAVY_RATE_LIMIT_PATHS else RATE_LIMIT_PUBLIC_REQUESTS
    key = (client_ip_from_request(request), request.url.path)
    timestamps = [stamp for stamp in RATE_LIMIT_BUCKETS.get(key, []) if now - stamp < window]
    if len(timestamps) >= limit:
        RATE_LIMIT_BUCKETS[key] = timestamps
        oldest = min(timestamps) if timestamps else now
        retry_after = max(1, int(math.ceil(window - (now - oldest))))
        raise HTTPException(
            status_code=429,
            detail="Muitas requisicoes. Tente novamente em instantes.",
            headers={"Retry-After": str(retry_after)},
        )
    timestamps.append(now)
    RATE_LIMIT_BUCKETS[key] = timestamps
    if len(RATE_LIMIT_BUCKETS) > RATE_LIMIT_BUCKET_MAX_KEYS:
        expired_keys = [
            bucket_key
            for bucket_key, bucket_timestamps in RATE_LIMIT_BUCKETS.items()
            if not any(now - stamp < window for stamp in bucket_timestamps)
        ]
        for bucket_key in expired_keys:
            RATE_LIMIT_BUCKETS.pop(bucket_key, None)
        while len(RATE_LIMIT_BUCKETS) > RATE_LIMIT_BUCKET_MAX_KEYS:
            RATE_LIMIT_BUCKETS.pop(next(iter(RATE_LIMIT_BUCKETS)), None)


async def cached_api_value(key: str, ttl_seconds: int, builder):
    now = time.monotonic()
    cached = API_RESPONSE_CACHE.get(key)
    if cached and cached[0] > now:
        return cached[1]

    lock = API_RESPONSE_LOCKS.setdefault(key, asyncio.Lock())
    async with lock:
        cached = API_RESPONSE_CACHE.get(key)
        if cached and cached[0] > time.monotonic():
            return cached[1]
        value = await builder()
        if len(API_RESPONSE_CACHE) >= API_RESPONSE_CACHE_MAX_ENTRIES:
            current = time.monotonic()
            expired_keys = [cache_key for cache_key, (expires_at, _) in API_RESPONSE_CACHE.items() if expires_at <= current]
            for cache_key in expired_keys:
                API_RESPONSE_CACHE.pop(cache_key, None)
                API_RESPONSE_LOCKS.pop(cache_key, None)
            while len(API_RESPONSE_CACHE) >= API_RESPONSE_CACHE_MAX_ENTRIES:
                cache_key = next(iter(API_RESPONSE_CACHE))
                API_RESPONSE_CACHE.pop(cache_key, None)
                API_RESPONSE_LOCKS.pop(cache_key, None)
        API_RESPONSE_CACHE[key] = (time.monotonic() + max(1, ttl_seconds), value)
        return value


def read_ttl_json(path: Path, ttl_seconds: int, default: Any = None) -> Any:
    if not data_path_exists(path):
        return default
    try:
        if not DATA_S3_SYNC_ENABLED and time.time() - path.stat().st_mtime > ttl_seconds:
            return default
    except Exception:
        return default
    return read_data_json(path, default)


def write_ttl_json(path: Path, payload: Any) -> None:
    write_json(path, payload)


def require_admin_token(x_coibe_admin_token: str | None = Header(default=None)) -> None:
    if not REQUIRE_ADMIN_TOKEN:
        return
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=503, detail="Endpoint administrativo indisponivel sem COIBE_ADMIN_TOKEN.")
    if not x_coibe_admin_token or not hmac.compare_digest(x_coibe_admin_token, ADMIN_TOKEN):
        raise HTTPException(status_code=403, detail="Token administrativo invalido ou ausente.")


def allow_local_or_admin_live_scan(request: Request, x_coibe_admin_token: str | None = None) -> None:
    if is_strict_local_request(request):
        return
    require_admin_token(x_coibe_admin_token)


def require_local_request(request: Request) -> None:
    if is_strict_local_request(request):
        return
    raise HTTPException(status_code=404, detail="Recurso disponivel apenas localmente.")


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
    allow_origins=CORS_ORIGINS or ([] if IS_PRODUCTION else ["*"]),
    allow_origin_regex=CORS_ORIGIN_REGEX,
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    if len(request.scope.get("query_string", b"")) > MAX_QUERY_STRING_BYTES:
        return JSONResponse(status_code=414, content={"detail": "Query string maior que o limite permitido."})
    if request.method in {"POST", "PUT", "PATCH"}:
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                if int(content_length) > MAX_REQUEST_BODY_BYTES:
                    return JSONResponse(status_code=413, content={"detail": "Corpo da requisicao maior que o limite permitido."})
            except ValueError:
                return JSONResponse(status_code=400, content={"detail": "Content-Length invalido."})
        chunks: list[bytes] = []
        total = 0
        async for chunk in request.stream():
            total += len(chunk)
            if total > MAX_REQUEST_BODY_BYTES:
                return JSONResponse(status_code=413, content={"detail": "Corpo da requisicao maior que o limite permitido."})
            chunks.append(chunk)
        body = b"".join(chunks)
        request._body = body
        request._stream_consumed = False

        async def receive_limited_body() -> dict[str, Any]:
            nonlocal body
            chunk, body = body, b""
            return {"type": "http.request", "body": chunk, "more_body": False}

        request._receive = receive_limited_body
    try:
        check_rate_limit(request)
    except HTTPException as exc:
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail}, headers=exc.headers)
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
    return response


app.mount("/static", StaticFiles(directory="static"), name="static")


def process_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                timeout=2,
                check=False,
            )
            return str(pid) in result.stdout
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
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
    config = read_monitor_model_config()
    command = [
        sys.executable,
        "local_monitor.py",
        "--api-base",
        AUTO_MONITOR_API_BASE,
        "--interval-minutes",
        str(AUTO_MONITOR_INTERVAL_MINUTES),
        "--pages",
        str(config.research_rounds),
        "--page-size",
        str(config.feed_page_size),
        "--startup-delay-seconds",
        "8",
    ]
    creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    process = subprocess.Popen(command, stdout=stdout, stderr=stderr, creationflags=creationflags, env=monitor_process_env(config))
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


def public_api_cache_key(url: str, params: dict[str, Any] | None = None) -> str:
    normalized_params = urlencode(sorted((params or {}).items()))
    return f"{url}?{normalized_params}"


def public_api_retry_after(response: httpx.Response | None) -> float:
    if response is None:
        return float(PUBLIC_API_ERROR_COOLDOWN_SECONDS)
    retry_after = response.headers.get("retry-after")
    try:
        return max(float(retry_after or 0), 1.0)
    except ValueError:
        return float(PUBLIC_API_ERROR_COOLDOWN_SECONDS)


async def get_json(url: str, params: dict[str, Any] | None = None) -> Any:
    now = time.monotonic()
    cache_key = public_api_cache_key(url, params)
    host = urlparse(url).hostname or "public-api"
    cached = PUBLIC_API_CACHE.get(cache_key)
    if cached and cached[0] > now:
        return cached[2]

    lock = PUBLIC_API_LOCKS.setdefault(cache_key, asyncio.Lock())
    async with lock:
        now = time.monotonic()
        cached = PUBLIC_API_CACHE.get(cache_key)
        if cached and cached[0] > now:
            return cached[2]
        cooldown_until = PUBLIC_API_HOST_COOLDOWN.get(host, 0.0)
        if cooldown_until > now and cached and cached[1] > now:
            return cached[2]
        if cooldown_until > now:
            raise HTTPException(
                status_code=429,
                detail=f"Fonte pública em cooldown por proteção/rate limit: {host}",
                headers={"Retry-After": str(int(max(1, cooldown_until - now)))},
            )

        host_lock = PUBLIC_API_HOST_LOCKS.setdefault(host, asyncio.Lock())
        async with host_lock:
            last_call = PUBLIC_API_HOST_LAST_CALL.get(host, 0.0)
            wait_seconds = PUBLIC_API_MIN_INTERVAL_SECONDS - (time.monotonic() - last_call)
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
            PUBLIC_API_HOST_LAST_CALL[host] = time.monotonic()

            timeout = httpx.Timeout(12.0, connect=5.0)
            headers = {"User-Agent": "COIBE.IA public-data-risk-engine/0.4 (+cache; public-interest-monitoring)"}
            async with httpx.AsyncClient(timeout=timeout, headers=headers, follow_redirects=True) as client:
                try:
                    response = await client.get(url, params=params)
                    response.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code in {403, 408, 409, 425, 429, 500, 502, 503, 504}:
                        retry_after = public_api_retry_after(exc.response)
                        PUBLIC_API_HOST_COOLDOWN[host] = time.monotonic() + max(retry_after, PUBLIC_API_ERROR_COOLDOWN_SECONDS)
                        if cached and cached[1] > time.monotonic():
                            return cached[2]
                    raise HTTPException(
                        status_code=502,
                        detail=f"Fonte pública retornou HTTP {exc.response.status_code}: {url}",
                    ) from exc
                except httpx.TimeoutException as exc:
                    PUBLIC_API_HOST_COOLDOWN[host] = time.monotonic() + max(15, PUBLIC_API_ERROR_COOLDOWN_SECONDS // 2)
                    if cached and cached[1] > time.monotonic():
                        return cached[2]
                    raise HTTPException(status_code=504, detail=f"Timeout ao consultar fonte pública: {url}") from exc
                except httpx.HTTPError as exc:
                    PUBLIC_API_HOST_COOLDOWN[host] = time.monotonic() + max(15, PUBLIC_API_ERROR_COOLDOWN_SECONDS // 2)
                    if cached and cached[1] > time.monotonic():
                        return cached[2]
                    raise HTTPException(status_code=502, detail=f"Falha ao consultar fonte pública: {url}") from exc

            payload = response.json()
            fresh_until = time.monotonic() + max(1, PUBLIC_API_CACHE_TTL_SECONDS)
            stale_until = time.monotonic() + max(PUBLIC_API_STALE_TTL_SECONDS, PUBLIC_API_CACHE_TTL_SECONDS)
            PUBLIC_API_CACHE[cache_key] = (fresh_until, stale_until, payload)
            if len(PUBLIC_API_CACHE) > API_RESPONSE_CACHE_MAX_ENTRIES:
                current = time.monotonic()
                expired = [key for key, (_, stale_until_value, _) in PUBLIC_API_CACHE.items() if stale_until_value <= current]
                for key in expired:
                    PUBLIC_API_CACHE.pop(key, None)
                    PUBLIC_API_LOCKS.pop(key, None)
                while len(PUBLIC_API_CACHE) > API_RESPONSE_CACHE_MAX_ENTRIES:
                    key = next(iter(PUBLIC_API_CACHE))
                    PUBLIC_API_CACHE.pop(key, None)
                    PUBLIC_API_LOCKS.pop(key, None)
            return payload


def sanitize_portal_transparencia_path(path: str) -> str:
    raw_path = str(path or "").strip().lstrip("/")
    parsed = urlparse(raw_path)
    if parsed.scheme or parsed.netloc:
        raise HTTPException(status_code=400, detail="Caminho de proxy invalido.")
    if not raw_path or len(raw_path) > 180:
        raise HTTPException(status_code=400, detail="Caminho de proxy invalido.")
    if "\\" in raw_path or ".." in raw_path or any(ord(char) < 32 for char in raw_path):
        raise HTTPException(status_code=400, detail="Caminho de proxy bloqueado.")
    allowed = re.fullmatch(r"[A-Za-z0-9/_\-.]+", raw_path)
    if not allowed:
        raise HTTPException(status_code=400, detail="Caminho de proxy bloqueado.")
    return raw_path


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

    safe_path = sanitize_portal_transparencia_path(path)
    url = f"{PORTAL_TRANSPARENCIA_BASE_URL}/{safe_path}"
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

    try:
        return response.json()
    except ValueError as exc:
        raise HTTPException(
            status_code=502,
            detail="Portal da Transparência retornou resposta não JSON ou vazia.",
        ) from exc


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
        if len(CNPJ_DETAILS_CACHE) > 1000:
            CNPJ_DETAILS_CACHE.pop(next(iter(CNPJ_DETAILS_CACHE)), None)
        CNPJ_DETAILS_CACHE[digits] = await fetch_cnpj_from_brasil_api(digits)
    except HTTPException:
        CNPJ_DETAILS_CACHE[digits] = None
    return CNPJ_DETAILS_CACHE[digits]


async def fetch_ibge_cities_cached() -> list[dict[str, Any]]:
    now = time.monotonic()
    if IBGE_CITIES_CACHE["items"] and now - float(IBGE_CITIES_CACHE["loaded_at"] or 0) < IBGE_CITIES_CACHE_TTL_SECONDS:
        return IBGE_CITIES_CACHE["items"]
    cities = await get_json(IBGE_CITY_SEARCH_URL)
    if isinstance(cities, list):
        IBGE_CITIES_CACHE["loaded_at"] = now
        IBGE_CITIES_CACHE["items"] = [city for city in cities if isinstance(city, dict)]
    return IBGE_CITIES_CACHE["items"]


async def fetch_city_from_ibge(city_name: str, uf: str | None = None) -> dict[str, Any] | None:
    cities = await fetch_ibge_cities_cached()
    normalized = normalize_text(city_name)
    normalized_uf = (uf or "").strip().upper()
    candidates: list[tuple[dict[str, Any], str]] = []
    for city in cities:
        microrregiao = city.get("microrregiao") or {}
        mesorregiao = microrregiao.get("mesorregiao") if isinstance(microrregiao, dict) else {}
        uf_data = (mesorregiao or {}).get("UF") if isinstance(mesorregiao, dict) else {}
        city_uf = (uf_data or {}).get("sigla") if isinstance(uf_data, dict) else None
        if normalized_uf and str(city_uf or "").upper() != normalized_uf:
            continue
        city_normalized = normalize_text(city.get("nome", ""))
        if city_normalized == normalized:
            return city
        candidates.append((city, city_normalized))

    best_city = None
    best_ratio = 0.0
    compact_normalized = normalized.replace(" ", "")
    for city, city_normalized in candidates:
        compact_city = city_normalized.replace(" ", "")
        ratio = max(
            SequenceMatcher(None, normalized, city_normalized).ratio(),
            SequenceMatcher(None, compact_normalized, compact_city).ratio(),
        )
        if compact_normalized and (compact_normalized in compact_city or compact_city in compact_normalized):
            ratio = max(ratio, 0.94)
        if ratio > best_ratio:
            best_city = city
            best_ratio = ratio
    if best_city is not None and best_ratio >= 0.86:
        return best_city
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


def first_public_record_text(payload: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def first_public_record_decimal(payload: dict[str, Any], *keys: str) -> Decimal | None:
    for key in keys:
        parsed = parse_decimal(payload.get(key))
        if parsed is not None:
            return parsed
    return None


def public_record_money_from_text(text: str) -> list[Decimal]:
    values: list[Decimal] = []
    for match in re.finditer(r"R\$\s*([0-9][0-9\.]*,\d{2})", text or ""):
        parsed = parse_decimal(match.group(1))
        if parsed is not None:
            values.append(parsed)
    return values


def public_record_comparable_values(record: dict[str, Any], payload: dict[str, Any]) -> tuple[Decimal, Decimal, Decimal | None, int | None]:
    value = first_public_record_decimal(
        payload,
        "value",
        "valorGlobal",
        "valor_global",
        "valor",
        "valorContrato",
        "valor_contrato",
        "valorInicial",
        "contract_value",
        "total_public_money",
        "travel_public_money",
        "paid_amount",
        "amount",
        "total_value",
    ) or Decimal("0")
    variation = first_public_record_decimal(
        payload,
        "estimated_variation",
        "estimated_overprice",
        "possible_overprice",
        "valor_acima_media",
        "above_average_value",
        "overprice_value",
    ) or Decimal("0")
    baseline = first_public_record_decimal(
        payload,
        "baseline",
        "average_amount",
        "baseline_average",
        "valor_medio",
        "media_comparavel",
        "reference_value",
        "comparable_value",
    )
    sample_size_decimal = first_public_record_decimal(
        payload,
        "sample_size",
        "comparison_sample_size",
        "comparable_count",
        "matching_records",
        "records_count",
    )
    sample_size = int(sample_size_decimal) if sample_size_decimal is not None and sample_size_decimal >= 0 else None

    subtitle_values = public_record_money_from_text(str(record.get("subtitle") or payload.get("summary") or ""))
    if value <= 0 and subtitle_values:
        value = subtitle_values[0]
    if variation <= 0 and len(subtitle_values) >= 2:
        variation = subtitle_values[1]
    if baseline is None and value > 0 and variation > 0 and value > variation:
        baseline = value - variation
    if variation <= 0 and baseline is not None and value > baseline:
        variation = value - baseline
    return value, variation, baseline, sample_size


def public_record_has_feed_signal(record_type: str, risk_score: int, value: Decimal, variation: Decimal, baseline: Decimal | None, title: str = "") -> bool:
    normalized_type = normalize_text(record_type).lower()
    normalized_title = normalize_text(title).lower()
    has_financial_signal = value > 0 or variation > 0 or baseline is not None
    if normalized_type in {"connector_source", "connector_status", "architecture_status", "state_risk"}:
        return False
    if normalized_type in PUBLIC_RECORD_FEED_REFERENCE_TYPES and not has_financial_signal and risk_score < 35:
        return False
    is_financial_risk = (
        normalized_type in PUBLIC_RECORD_FEED_FINANCIAL_TYPES
        or "superfaturamento" in normalized_title
        or "valor acima" in normalized_title
        or "preco" in normalized_title
        or "preço" in normalized_title
    )
    if is_financial_risk and not has_financial_signal:
        return False
    return True


def compact_public_record_title(text: str, limit: int = 150) -> str:
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip(" -|•")
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[:limit].rstrip(" -|•,.;") + "..."


def public_record_political_analysis_headline(record_type: str, payload: dict[str, Any]) -> str | None:
    risks = payload.get("risks") if isinstance(payload.get("risks"), list) else []
    weighted_risks: list[tuple[int, dict[str, Any]]] = []
    risk_weight = {"alto": 3, "médio": 2, "medio": 2, "baixo": 1, "indeterminado": 0}
    for index, risk in enumerate(risks):
        if not isinstance(risk, dict):
            continue
        level = normalize_text(risk.get("level") or "").lower()
        weighted_risks.append((risk_weight.get(level, 0) * 100 - index, risk))
    weighted_risks.sort(key=lambda row: row[0], reverse=True)

    for _, risk in weighted_risks:
        evidence = risk.get("evidence") if isinstance(risk.get("evidence"), dict) else {}
        matches = evidence.get("matches_count") or evidence.get("contracts_in_window") or evidence.get("related_contracts")
        related_total = parse_decimal(evidence.get("total_related_value") or evidence.get("total_window_value") or evidence.get("window_total_value"))
        if matches and related_total and related_total > 0:
            return f"{matches} contrato(s) ligados ao recorte somam {money(related_total)}"

        total_public_money = parse_decimal(evidence.get("total_public_money"))
        travel_public_money = parse_decimal(evidence.get("travel_public_money"))
        records_count = parse_decimal(evidence.get("records_count"))
        if total_public_money and total_public_money > 0:
            if travel_public_money and travel_public_money > 0:
                return f"{money(total_public_money)} em despesas analisadas, com {money(travel_public_money)} em viagens/deslocamentos"
            if records_count and records_count > 0:
                return f"{money(total_public_money)} em despesas analisadas em {int(records_count)} registro(s)"
            return f"{money(total_public_money)} em valores publicos analisados"

        matching_records = parse_decimal(evidence.get("matching_records"))
        if matching_records and matching_records > 0:
            title = str(risk.get("title") or "registros publicos para checar").strip()
            return f"{int(matching_records)} registro(s) relacionados - {title}"

    total = parse_decimal(payload.get("total_public_money")) or Decimal("0")
    travel = parse_decimal(payload.get("travel_public_money")) or Decimal("0")
    records_count = parse_decimal(payload.get("records_count"))
    if total > 0:
        if record_type == "political_parties":
            if travel > 0:
                return f"{money(total)} em despesas de parlamentares, com {money(travel)} em viagens/deslocamentos"
            return f"{money(total)} em despesas de parlamentares analisadas"
        if records_count and records_count > 0:
            return f"{money(total)} em contratos, pagamentos e registros ligados; {int(records_count)} fonte(s) cruzada(s)"
        return f"{money(total)} em contratos, pagamentos e registros ligados"

    summary = str(payload.get("summary") or "").strip()
    return summary or None


def public_record_political_display_title(record_type: str, title: str, payload: dict[str, Any], subtitle: str) -> str:
    if record_type not in {"political_people", "political_parties"}:
        return title

    name = str(payload.get("name") or title or "").strip()
    if not name:
        return title

    headline = public_record_political_analysis_headline(record_type, payload)
    if headline and normalize_text(headline) != normalize_text(name):
        return compact_public_record_title(f"{name} - {headline}")

    priority_reason = str(payload.get("priority_reason") or "").strip()
    if priority_reason and normalize_text(priority_reason) != normalize_text(name):
        return compact_public_record_title(f"{name} - {priority_reason}")

    subtitle_text = str(payload.get("subtitle") or subtitle or "").strip()
    normalized_subtitle = normalize_text(subtitle_text).lower()
    if subtitle_text and not normalized_subtitle.startswith("0 registro") and normalized_subtitle != normalize_text(name).lower():
        return compact_public_record_title(f"{name} - {subtitle_text}")

    return name


def public_record_feed_item(record: dict[str, Any], index: int) -> MonitoringItem | None:
    collected_at = parse_iso_datetime(str(record.get("collected_at") or "")) or brasilia_now()
    payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
    title = str(record.get("title") or payload.get("title") or record.get("source") or "Fonte publica atualizada")
    source = str(record.get("source") or "Fonte publica")
    subtitle = str(record.get("subtitle") or payload.get("summary") or payload.get("text_excerpt") or title)
    url = str(record.get("url") or payload.get("url") or "")
    record_type = str(record.get("record_type") or "public_record")
    title = public_record_political_display_title(record_type, title, payload, subtitle)
    record_key = str(record.get("record_key") or f"{record_type}:{source}:{title}:{index}")
    stable_id = hashlib.sha256(record_key.encode("utf-8", errors="ignore")).hexdigest()[:24]
    risk_level = str(record.get("risk_level") or payload.get("attention_level") or "baixo")
    normalized_level = normalize_text(risk_level).lower()
    risk_score = 60 if normalized_level == "alto" else 35 if normalized_level in {"medio", "médio"} else 12

    value, variation, baseline, sample_size = public_record_comparable_values(record, payload)
    if record_type in {"political_people", "political_parties"} and not public_record_political_analysis_headline(record_type, payload):
        normalized_subtitle = normalize_text(subtitle).lower()
        if normalized_subtitle.startswith("0 registro") or not payload:
            return None
    if not public_record_has_feed_signal(record_type, risk_score, value, variation, baseline, title):
        return None

    content_date = parse_iso_datetime(str(payload.get("date") or payload.get("data") or payload.get("published_at") or ""))
    item_date = content_date.date() if content_date else collected_at.date()
    entity = first_public_record_text(payload, "entity", "nomeUnidadeGestora", "nomeOrgao", "orgao", "orgao_nome") or source
    supplier_name = first_public_record_text(payload, "supplier_name", "nomeFornecedor", "nomeRazaoSocialFornecedor", "fornecedor", "supplier")
    supplier_cnpj = first_public_record_text(payload, "supplier_cnpj", "niFornecedor", "cnpj", "cnpjFornecedor")
    uf = first_public_record_text(payload, "uf", "UF", "siglaUf")
    city = first_public_record_text(payload, "city", "cidade", "municipio")
    formatted_value = money(value) if value > 0 else "Sem valor informado"
    formatted_variation = money(variation) if variation > 0 else "Sem variacao estimada"
    evidence = {
        "record_type": record_type,
        "source": source,
        "title": title,
        "url": url,
        "collected_at": collected_at.isoformat(),
        "payload_excerpt": subtitle[:1000],
        "value": str(value),
        "estimated_variation": str(variation),
        "has_financial_value": value > 0,
    }
    if baseline is not None:
        evidence["baseline"] = str(baseline)
    if sample_size is not None:
        evidence["sample_size"] = sample_size
    try:
        return MonitoringItem(
            id=f"public-record-{stable_id}",
            date=item_date,
            title=title,
            location=source,
            city=city,
            uf=uf,
            entity=entity,
            supplier_name=supplier_name,
            supplier_cnpj=supplier_cnpj,
            value=value,
            formatted_value=formatted_value,
            estimated_variation=variation,
            formatted_variation=formatted_variation,
            risk_score=risk_score,
            risk_level=risk_level if risk_level else "baixo",
            object=subtitle[:1200] or title,
            report=MonitoringReport(
                id=f"public-record-report-{stable_id}",
                summary="Registro publico recente coletado pelo monitor para manter o feed atualizado.",
                risk_score=risk_score,
                risk_level=risk_level if risk_level else "baixo",
                red_flags=[
                    RedFlagResult(
                        code="PUBLIC-RECORD-UPDATE",
                        title="Fonte publica atualizada",
                        has_risk=risk_score >= 35 or variation > 0,
                        risk_level=risk_level if risk_level else "baixo",
                        message="Registro recente entrou pela base de fontes publicas e deve ser usado como triagem/contexto.",
                        evidence=evidence,
                        criteria={"rule": "registro publico coletado/atualizado pelo monitor"},
                    )
                ],
                official_sources=[MonitoringSource(label=source, url=url, kind=record_type)] if url else [],
                public_evidence=[evidence],
                generated_at=collected_at,
                ml_model="coibe-public-record-feed",
            ),
        )
    except Exception:
        return None


def enrich_persisted_public_record_title(item: MonitoringItem) -> MonitoringItem:
    evidence = next((flag.evidence for flag in item.report.red_flags or [] if isinstance(flag.evidence, dict)), {})
    record_type = str(evidence.get("record_type") or "")
    if record_type not in {"political_people", "political_parties"}:
        return item

    title = str(item.title or "").strip()
    normalized_title = normalize_text(title).lower()
    if "r$" in title.lower() or "despesa" in normalized_title or "contrato" in normalized_title or "registro(s)" in normalized_title:
        return item

    excerpt = str(evidence.get("payload_excerpt") or item.object or item.report.summary or "").strip()
    normalized_excerpt = normalize_text(excerpt).lower()
    if not excerpt or normalized_excerpt.startswith("0 registro") or normalize_text(title).lower() == normalized_excerpt:
        return item

    item.title = compact_public_record_title(f"{title} - {excerpt}")
    for flag in item.report.red_flags or []:
        if isinstance(flag.evidence, dict):
            flag.evidence["title"] = item.title
    return item


def load_public_record_feed_items(limit: int = PUBLIC_RECORD_FEED_LIMIT) -> list[MonitoringItem]:
    if limit <= 0 or not data_path_exists(LOCAL_PUBLIC_RECORDS_PATH):
        return []
    raw_records = read_data_json(LOCAL_PUBLIC_RECORDS_PATH, [])
    if not isinstance(raw_records, list):
        return []
    records = [record for record in raw_records if isinstance(record, dict)]
    records.sort(key=lambda record: str(record.get("collected_at") or ""), reverse=True)
    items: list[MonitoringItem] = []
    seen: set[str] = set()
    for record in records:
        record_type = str(record.get("record_type") or "")
        if record_type in {"connector_source", "connector_status", "architecture_status", "state_risk"}:
            continue
        key = str(record.get("record_key") or record.get("url") or record.get("title") or "")
        if key in seen:
            continue
        seen.add(key)
        item = public_record_feed_item(record, len(items))
        if item is not None:
            items.append(item)
        if len(items) >= limit:
            break
    return items


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
    if item.id.startswith("public-record-"):
        return item

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
        baseline = evidence_number(evidence, "baseline")
        estimated_variation = evidence_number(evidence, "estimated_variation")
        if baseline is not None and baseline > 0 and item.value > 0:
            comparisons.append(f"media comparavel de {money(Decimal(str(baseline)))} para valor analisado de {money(item.value)}")
        if estimated_variation is not None and estimated_variation > 0:
            comparisons.append(f"risco de valor acima da media em {money(Decimal(str(estimated_variation)))}")

        percent_above = evidence_number(evidence, "percent_above_baseline")
        sample_size = evidence_number(evidence, "sample_size")
        if percent_above is not None:
            comparisons.append(
                f"valor {percent_text(percent_above)} acima da media de {number_text(sample_size) or 'varios'} registros comparaveis"
            )
        elif sample_size is not None and sample_size > 0:
            comparisons.append(f"comparacao feita contra {number_text(sample_size)} registro(s) publico(s)")

        cnpj_age = evidence_number(evidence, "cnpj_age_days")
        if cnpj_age is not None:
            comparisons.append(f"empresa com {number_text(cnpj_age, ' dias')} de abertura")

        ratio = evidence_number(evidence, "ratio_contract_to_capital")
        if ratio is not None:
            comparisons.append(f"valor equivale a {ratio:,.1f}x o capital social declarado".replace(",", "X").replace(".", ",").replace("X", "."))

        distance = evidence_number(evidence, "distance_km")
        if distance is not None:
            comparisons.append(f"fornecedor a cerca de {number_text(distance, ' km')} do orgao contratante")

        related_contracts = evidence_number(evidence, "related_contracts") or evidence_number(evidence, "contracts_in_window")
        total_window = evidence_number(evidence, "total_window_value") or evidence_number(evidence, "window_total_value")
        if related_contracts is not None and total_window is not None:
            comparisons.append(f"{number_text(related_contracts)} registros proximos somam {money(Decimal(str(total_window)))}")

        matching_records = evidence_number(evidence, "matching_records")
        if matching_records is not None:
            comparisons.append(f"{number_text(matching_records)} registro(s) publico(s) relacionado(s) a socio, pessoa ou empresa")

    unique_comparisons = list(dict.fromkeys(comparisons))[:3]
    comparison_sentence = (
        "Principais comparacoes: " + "; ".join(unique_comparisons) + "."
        if unique_comparisons
        else "Nenhum desvio forte apareceu nos dados comparaveis disponiveis; o item segue monitorado para novas bases publicas."
    )
    value_sentence = (
        f"valor analisado {money(item.value)}"
        if item.value > 0
        else "sem valor financeiro direto informado na fonte"
    )
    supplier_sentence = f"fornecedor/pessoa {item.supplier_name}" if item.supplier_name else "fornecedor/pessoa nao informado"
    return (
        f"Risco {item.risk_level}: prioridade de leitura definida por comparacoes publicas de valor, "
        f"historico de fornecedor, socios, empresa, localidade e orgao. Item analisado: {item.title}, "
        f"{supplier_sentence}, {value_sentence}. "
        f"{comparison_sentence} A busca pode cruzar este contexto por produto, pessoa, CNPJ, empresa, partido, STF, cidade, estado ou orgao."
    )


def load_local_monitoring_items(
    q: str | None = None,
    uf: str | None = None,
    city: str | None = None,
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

    public_record_items = load_public_record_feed_items()
    if public_record_items:
        existing_keys = {
            f"{raw_item.get('id')}:{raw_item.get('date')}"
            for raw_item in raw_items
            if isinstance(raw_item, dict)
        }
        for public_item in public_record_items:
            key = f"{public_item.id}:{public_item.date.isoformat()}"
            if key not in existing_keys:
                raw_items.append(public_item.model_dump(mode="json"))
                existing_keys.add(key)

    query_parts = split_related_query(q)
    normalized_queries = [normalize_text(part) for part in query_parts]
    query_digits_parts = ["".join(ch for ch in part if ch.isdigit()) for part in query_parts]
    uf_filter = (uf or "").strip().upper()
    city_filter = normalize_text(city)
    items: list[MonitoringItem] = []

    for raw_item in raw_items:
        try:
            item = MonitoringItem.model_validate(raw_item)
        except Exception:
            continue
        item = ensure_precise_compras_source(item)
        item.report.red_flags = [normalize_attention_flag_text(flag) for flag in item.report.red_flags]
        item.report.summary = monitoring_summary_for_item(item)
        item = enrich_persisted_public_record_title(item)

        if uf_filter and (item.uf or "").upper() != uf_filter:
            continue

        if city_filter:
            item_city = normalize_text(item.city or item.location)
            if city_filter != item_city and city_filter not in item_city:
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


def monitoring_item_recent_order(item: MonitoringItem) -> tuple[bool, str, bool, int, str, str]:
    # Future validity dates are real contract metadata, but they should not outrank already published recent feed content.
    has_financial_signal = item.value > 0 or item.estimated_variation > 0
    if not has_financial_signal:
        for flag in item.report.red_flags or []:
            evidence = flag.evidence or {}
            if evidence_number(evidence, "baseline") is not None or evidence_number(evidence, "estimated_variation") is not None:
                has_financial_signal = True
                break
    return (
        item.date <= brasilia_today(),
        item.date.isoformat(),
        has_financial_signal,
        item.risk_score,
        item.report.generated_at.isoformat(),
        item.id,
    )


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
    elif size_order == "data_asc":
        filtered.sort(key=lambda item: (item.date.isoformat(), item.report.generated_at.isoformat(), item.id))
    else:
        filtered.sort(key=monitoring_item_recent_order, reverse=True)

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


def raw_analysis_metrics(data: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(data, dict):
        return platform_monitoring_metrics([])
    raw_items = data.get("alerts") if isinstance(data.get("alerts"), list) else data.get("items")
    if not isinstance(raw_items, list):
        return platform_monitoring_metrics([])
    total_value = Decimal("0")
    estimated_variation_total = Decimal("0")
    high_alerts = 0
    entities: set[str] = set()
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        value = parse_decimal(raw.get("value")) or Decimal("0")
        variation = parse_decimal(raw.get("estimated_variation")) or Decimal("0")
        total_value += value
        estimated_variation_total += variation
        level = normalize_text(raw.get("risk_level")).lower()
        if level == "alto":
            high_alerts += 1
        entity = normalize_text(raw.get("entity"))
        if entity:
            entities.add(entity)
    return {
        "total_value": total_value,
        "estimated_variation_total": estimated_variation_total,
        "high_alerts_count": high_alerts,
        "monitored_entities_count": len(entities),
    }


def merge_metric_max(primary: dict[str, Any], secondary: dict[str, Any]) -> dict[str, Any]:
    merged = dict(primary)
    for key in ("total_value", "estimated_variation_total", "high_alerts_count", "monitored_entities_count"):
        left = primary.get(key, Decimal("0"))
        right = secondary.get(key, Decimal("0"))
        try:
            merged[key] = max(left, right)
        except TypeError:
            merged[key] = right or left
    return merged


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


def read_monitor_model_state() -> dict[str, Any]:
    empty_state = {
        "version": "coibe-monitor-v1",
        "updated_at": None,
        "cycles": 0,
        "learned_terms": [],
        "learned_checks": [],
        "learned_targets": [],
        "cache_profile": {},
        "last_training": None,
    }

    def normalize_model_counters(raw_state: dict[str, Any]) -> dict[str, Any]:
        cache_profile = raw_state.get("cache_profile") if isinstance(raw_state.get("cache_profile"), dict) else {}
        last_training = raw_state.get("last_training") if isinstance(raw_state.get("last_training"), dict) else {}
        try:
            trained_items = int(last_training.get("items_analyzed") or 0)
        except Exception:
            trained_items = 0
        try:
            cache_items = int(cache_profile.get("items_seen") or 0)
        except Exception:
            cache_items = 0
        if trained_items > cache_items:
            cache_profile = {**cache_profile, "items_seen": trained_items}
            cache_profile.setdefault("model_training_sample_seen", min(trained_items, 500))
            cache_profile.setdefault("cached_training_items_seen", min(trained_items, 500))
            cache_profile.setdefault("cycle_items_seen", 0)
            raw_state = {**raw_state, "cache_profile": cache_profile}
        return raw_state

    if not MONITOR_MODEL_STATE_PATH.exists():
        state = empty_state
    else:
        try:
            loaded = json.loads(MONITOR_MODEL_STATE_PATH.read_text(encoding="utf-8-sig"))
        except Exception:
            loaded = empty_state
        state = loaded if isinstance(loaded, dict) else empty_state

    latest = read_data_json(LOCAL_MONITOR_LATEST_PATH, {})
    recovered = latest.get("model") if isinstance(latest, dict) and isinstance(latest.get("model"), dict) else {}
    recovered_terms = recovered.get("learned_terms") if isinstance(recovered.get("learned_terms"), list) else []
    recovered_checks = recovered.get("learned_checks") if isinstance(recovered.get("learned_checks"), list) else []
    recovered_targets = recovered.get("learned_targets") if isinstance(recovered.get("learned_targets"), list) else []
    current_terms = state.get("learned_terms") if isinstance(state.get("learned_terms"), list) else []
    current_checks = state.get("learned_checks") if isinstance(state.get("learned_checks"), list) else []
    current_targets = state.get("learned_targets") if isinstance(state.get("learned_targets"), list) else []
    should_recover = False
    if recovered_terms or recovered_checks or recovered_targets:
        should_recover = (
            len(recovered_terms) > len(current_terms)
            or len(recovered_checks) > len(current_checks)
            or len(recovered_targets) > len(current_targets)
        )
        try:
            current_updated = datetime.fromisoformat(str(state.get("updated_at")).replace("Z", "+00:00")) if state.get("updated_at") else None
            recovered_updated = datetime.fromisoformat(str(recovered.get("updated_at")).replace("Z", "+00:00")) if recovered.get("updated_at") else None
            should_recover = should_recover or bool(recovered_updated and (not current_updated or recovered_updated > current_updated))
        except Exception:
            should_recover = should_recover or not current_terms

    if should_recover:
        recovered_state = normalize_model_counters({**empty_state, **recovered})
        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        MONITOR_MODEL_STATE_PATH.write_text(
            json.dumps(recovered_state, ensure_ascii=False, indent=2, default=json_default),
            encoding="utf-8",
        )
        return recovered_state
    return normalize_model_counters(state)


def default_monitor_model_config() -> MonitorModelConfig:
    return MonitorModelConfig(
        use_gpu=COIBE_ML_USE_GPU,
        gpu_memory_limit_mb=COIBE_GPU_MEMORY_LIMIT_MB,
        use_shared_memory=COIBE_ML_USE_SHARED_MEMORY,
        shared_memory_limit_mb=COIBE_SHARED_MEMORY_LIMIT_MB,
        scan_profile=os.getenv("COIBE_MONITOR_SCAN_PROFILE", "balanced"),
        max_concurrent_requests=int(os.getenv("COIBE_MONITOR_MAX_CONCURRENT_REQUESTS", "4")),
        public_api_concurrency=int(os.getenv("COIBE_MONITOR_PUBLIC_API_CONCURRENCY", "3")),
        public_api_source_mode=os.getenv("COIBE_MONITOR_PUBLIC_API_SOURCE_MODE", "hybrid"),
        internet_sweep_concurrency=int(os.getenv("COIBE_MONITOR_INTERNET_SWEEP_CONCURRENCY", "4")),
        gpu_acceleration_level=os.getenv("COIBE_MONITOR_GPU_ACCELERATION_LEVEL", "balanced"),
        training_sample_limit=int(os.getenv("COIBE_MONITOR_TRAINING_SAMPLE_LIMIT", "1000")),
        analysis_batch_size=int(os.getenv("COIBE_MONITOR_ANALYSIS_BATCH_SIZE", "256")),
        learning_batch_size=int(os.getenv("COIBE_MONITOR_LEARNING_BATCH_SIZE", "512")),
        selected_model_id=MONITOR_SELECTED_MODEL_ID,
        deep_learning_enabled=True,
        quantization_mode="dynamic-int8",
        model_format="joblib+onnx-manifest+quant-manifest",
        internet_sweep_enabled=True,
        web_fallback_enabled=os.getenv("COIBE_MONITOR_WEB_FALLBACK_ENABLED", "true").lower() in {"1", "true", "yes"},
        internet_sweep_pages_per_cycle=int(os.getenv("COIBE_MONITOR_INTERNET_SWEEP_PAGES_PER_CYCLE", "6")),
        internet_sweep_timeout_seconds=int(os.getenv("COIBE_MONITOR_INTERNET_SWEEP_TIMEOUT_SECONDS", "12")),
        research_timeout_seconds=int(os.getenv("COIBE_RESEARCH_TIMEOUT_SECONDS", "90")),
        research_rounds=AUTO_MONITOR_PAGES,
        feed_page_size=AUTO_MONITOR_PAGE_SIZE,
        political_party_scan_limit=int(os.getenv("COIBE_POLITICAL_PARTY_SCAN_LIMIT", "12")),
        political_people_scan_limit=int(os.getenv("COIBE_POLITICAL_PEOPLE_SCAN_LIMIT", "24")),
        learned_terms_per_cycle=int(os.getenv("COIBE_MODEL_LEARNED_TERMS_PER_CYCLE", "12")),
        search_terms_per_cycle=int(os.getenv("COIBE_MONITOR_SEARCH_TERMS_PER_CYCLE", "8")),
        search_delay_seconds=float(os.getenv("COIBE_MONITOR_SEARCH_DELAY_SECONDS", "2.0")),
        priority_feed_queries_per_cycle=int(os.getenv("COIBE_MONITOR_PRIORITY_FEED_QUERIES_PER_CYCLE", "4")),
    )


def read_monitor_model_config() -> MonitorModelConfig:
    if not MONITOR_MODEL_CONFIG_PATH.exists():
        return default_monitor_model_config()
    try:
        loaded = json.loads(MONITOR_MODEL_CONFIG_PATH.read_text(encoding="utf-8"))
        return MonitorModelConfig.model_validate(loaded)
    except Exception:
        return default_monitor_model_config()


def write_monitor_model_config(config: MonitorModelConfig) -> MonitorModelConfig:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    payload = config.model_dump()
    payload["updated_at"] = brasilia_now().isoformat()
    payload["local_only"] = True
    payload["note"] = "Configuracao local do monitor; nao expor em endpoint publico."
    MONITOR_MODEL_CONFIG_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return config


def monitor_process_env(config: MonitorModelConfig) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "COIBE_ML_USE_GPU": "true" if config.use_gpu else "false",
            "COIBE_GPU_MEMORY_LIMIT_MB": str(config.gpu_memory_limit_mb),
            "COIBE_ML_USE_SHARED_MEMORY": "true" if config.use_shared_memory else "false",
            "COIBE_SHARED_MEMORY_LIMIT_MB": str(config.shared_memory_limit_mb),
            "COIBE_MONITOR_SCAN_PROFILE": config.scan_profile,
            "COIBE_MONITOR_MAX_CONCURRENT_REQUESTS": str(config.max_concurrent_requests),
            "COIBE_MONITOR_PUBLIC_API_CONCURRENCY": str(config.public_api_concurrency),
            "COIBE_MONITOR_PUBLIC_API_SOURCE_MODE": config.public_api_source_mode,
            "COIBE_MONITOR_INTERNET_SWEEP_CONCURRENCY": str(config.internet_sweep_concurrency),
            "COIBE_MONITOR_GPU_ACCELERATION_LEVEL": config.gpu_acceleration_level,
            "COIBE_MONITOR_TRAINING_SAMPLE_LIMIT": str(config.training_sample_limit),
            "COIBE_MONITOR_ANALYSIS_BATCH_SIZE": str(config.analysis_batch_size),
            "COIBE_MONITOR_LEARNING_BATCH_SIZE": str(config.learning_batch_size),
            "COIBE_MONITOR_INTERNET_SWEEP_ENABLED": "true" if config.internet_sweep_enabled else "false",
            "COIBE_MONITOR_WEB_FALLBACK_ENABLED": "true" if config.web_fallback_enabled else "false",
            "COIBE_MONITOR_INTERNET_SWEEP_PAGES_PER_CYCLE": str(config.internet_sweep_pages_per_cycle),
            "COIBE_MONITOR_INTERNET_SWEEP_TIMEOUT_SECONDS": str(config.internet_sweep_timeout_seconds),
            "COIBE_RESEARCH_TIMEOUT_SECONDS": str(config.research_timeout_seconds),
            "COIBE_MODEL_LEARNED_TERMS_PER_CYCLE": str(config.learned_terms_per_cycle),
            "COIBE_MONITOR_SEARCH_TERMS_PER_CYCLE": str(config.search_terms_per_cycle),
            "COIBE_MONITOR_SEARCH_DELAY_SECONDS": str(config.search_delay_seconds),
            "COIBE_MONITOR_PRIORITY_FEED_QUERIES_PER_CYCLE": str(config.priority_feed_queries_per_cycle),
            "COIBE_MONITOR_SELECTED_MODEL_ID": config.selected_model_id,
            "COIBE_MONITOR_DEEP_LEARNING_ENABLED": "true" if config.deep_learning_enabled else "false",
            "COIBE_MONITOR_QUANTIZATION_MODE": config.quantization_mode,
            "COIBE_POLITICAL_PARTY_SCAN_LIMIT": str(config.political_party_scan_limit),
            "COIBE_POLITICAL_PEOPLE_SCAN_LIMIT": str(config.political_people_scan_limit),
        }
    )
    return env


def gpu_status() -> dict[str, Any]:
    config = read_monitor_model_config()
    prepare_cuda_dll_paths()
    status = {
        "enabled_by_env": COIBE_ML_USE_GPU,
        "enabled_by_config": config.use_gpu,
        "enabled": bool(config.use_gpu),
        "memory_limit_mb": config.gpu_memory_limit_mb,
        "shared_memory_enabled": config.use_shared_memory,
        "shared_memory_limit_mb": config.shared_memory_limit_mb,
        "available": False,
        "name": None,
        "memory_total_mb": None,
        "gpu_library": None,
        "gpu_runtime_ready": False,
        "note": "GPU solicitada pela configuracao local; processos de monitor recebem COIBE_ML_USE_GPU automaticamente ao iniciar.",
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
        import cupy  # type: ignore

        status["gpu_library"] = f"cupy {cupy.__version__}"
        probe = cupy.asarray([1, 2, 3], dtype=cupy.float32)
        status["gpu_runtime_ready"] = bool(float(cupy.sum(probe).get()) == 6.0)
    except Exception:
        pass
    return status


def monitor_training_status() -> dict[str, Any]:
    pid = None
    if AUTO_MONITOR_PID_PATH.exists():
        try:
            raw_pid = AUTO_MONITOR_PID_PATH.read_text(encoding="utf-8").strip()
            pid = int(raw_pid) if raw_pid.isdigit() else None
        except Exception:
            pid = None
    running = bool(pid and process_is_running(pid))
    if pid and not running:
        try:
            AUTO_MONITOR_PID_PATH.unlink(missing_ok=True)
        except Exception:
            pass
    return {"running": running, "pid": pid if running else None, "pid_path": str(AUTO_MONITOR_PID_PATH)}


def start_monitor_training_process() -> dict[str, Any]:
    status = monitor_training_status()
    if status["running"]:
        return status
    logs_dir = Path("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)
    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)
    AUTO_MONITOR_PID_PATH.parent.mkdir(parents=True, exist_ok=True)
    config = read_monitor_model_config()
    command = [
        sys.executable,
        "local_monitor.py",
        "--api-base",
        AUTO_MONITOR_API_BASE,
        "--interval-minutes",
        str(AUTO_MONITOR_INTERVAL_MINUTES),
        "--pages",
        str(config.research_rounds),
        "--page-size",
        str(config.feed_page_size),
    ]
    creationflags = 0
    terminal_mode = "log_file"
    stdout_target: Any = (logs_dir / "training.out.log").open("a", encoding="utf-8")
    stderr_target: Any = (logs_dir / "training.err.log").open("a", encoding="utf-8")
    if os.name == "nt":
        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(subprocess, "CREATE_NEW_CONSOLE", 0)
        terminal_mode = "visible_console"
        stdout_target.close()
        stderr_target.close()
        stdout_target = None
        stderr_target = None
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=stdout_target,
            stderr=stderr_target,
            cwd=str(Path.cwd()),
            close_fds=True,
            creationflags=creationflags,
            env=monitor_process_env(config),
        )
    finally:
        if stdout_target is not None:
            stdout_target.close()
        if stderr_target is not None:
            stderr_target.close()
    AUTO_MONITOR_PID_PATH.write_text(str(process.pid), encoding="utf-8")
    return {"running": True, "pid": process.pid, "pid_path": str(AUTO_MONITOR_PID_PATH), "terminal": terminal_mode}


def stop_monitor_training_process() -> dict[str, Any]:
    status = monitor_training_status()
    pid = status.get("pid")
    if pid:
        terminate_process(int(pid))
    try:
        AUTO_MONITOR_PID_PATH.unlink(missing_ok=True)
    except Exception:
        pass
    return {"running": False, "pid": None, "pid_path": str(AUTO_MONITOR_PID_PATH)}


def default_model_registry(selected_model_id: str | None = None) -> dict[str, Any]:
    selected = selected_model_id or MONITOR_SELECTED_MODEL_ID
    return {
        "default_model_id": selected,
        "models": [
            {
                "id": "coibe-adaptive-default",
                "name": "COIBE Adaptativo Padrao",
                "kind": "rules+statistics+learning",
                "path": str(MONITOR_MODEL_STATE_PATH),
                "memory_path": str(MONITOR_MODEL_MEMORY_PATH),
                "selected": selected == "coibe-adaptive-default",
                "available": True,
                "quantization_compatible": True,
            },
            {
                "id": "coibe-deep-mlp",
                "name": "COIBE Deep Learning MLP",
                "kind": "tfidf+mlp",
                "path": str((MODELS_DIR / "coibe_adaptive_deep_model.joblib") if (MODELS_DIR / "coibe_adaptive_deep_model.joblib").exists() else (MODELS_DIR / "coibe_adaptive_deep_model.ai.json")),
                "selected": selected == "coibe-deep-mlp",
                "available": (MODELS_DIR / "coibe_adaptive_deep_model.joblib").exists() or (MODELS_DIR / "coibe_adaptive_deep_model.ai.json").exists(),
                "quantization_compatible": True,
                "onnx_manifest_path": str(MODELS_DIR / "coibe_adaptive_deep_model.onnx.json"),
                "quantization_manifest_path": str(MODELS_DIR / "coibe_adaptive_deep_model.quant.json"),
            },
        ],
    }


def read_model_registry(config: MonitorModelConfig | None = None, state: dict[str, Any] | None = None) -> dict[str, Any]:
    selected_model_id = (config.selected_model_id if config else None) or MONITOR_SELECTED_MODEL_ID
    registry = default_model_registry(selected_model_id)
    if MONITOR_MODEL_REGISTRY_PATH.exists():
        try:
            loaded = json.loads(MONITOR_MODEL_REGISTRY_PATH.read_text(encoding="utf-8-sig"))
            if isinstance(loaded, dict) and isinstance(loaded.get("models"), list):
                registry = loaded
        except Exception:
            registry = default_model_registry(selected_model_id)
    state_selected = state.get("selected_model", {}).get("id") if isinstance(state, dict) and isinstance(state.get("selected_model"), dict) else None
    selected = selected_model_id or state_selected or registry.get("default_model_id") or "coibe-adaptive-default"
    registry["default_model_id"] = selected
    registry["models"] = [
        {**model, "selected": str(model.get("id")) == selected}
        for model in registry.get("models", [])
        if isinstance(model, dict)
    ]
    if not any(model.get("id") == selected for model in registry["models"]):
        registry["models"].insert(
            0,
            {
                "id": selected,
                "name": selected,
                "kind": "custom",
                "path": "",
                "selected": True,
                "available": False,
                "quantization_compatible": True,
            },
        )
    try:
        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        if not MONITOR_MODEL_REGISTRY_PATH.exists():
            MONITOR_MODEL_REGISTRY_PATH.write_text(json.dumps(registry, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")
    except Exception:
        pass
    return registry


def monitor_model_quality_summary(state: dict[str, Any]) -> dict[str, Any]:
    artifacts = state.get("model_artifacts") if isinstance(state.get("model_artifacts"), dict) else {}
    cache_profile = state.get("cache_profile") if isinstance(state.get("cache_profile"), dict) else {}
    last_training = state.get("last_training") if isinstance(state.get("last_training"), dict) else {}
    dataset_profile = artifacts.get("dataset_profile") if isinstance(artifacts.get("dataset_profile"), dict) else {}
    validation_metrics = artifacts.get("validation_metrics") if isinstance(artifacts.get("validation_metrics"), dict) else None
    training_metrics = artifacts.get("training_metrics") if isinstance(artifacts.get("training_metrics"), dict) else None
    sample_size = (
        int(dataset_profile.get("sample_size") or 0)
        or int(last_training.get("model_training_sample") or 0)
        or int(cache_profile.get("model_training_sample_seen") or 0)
        or int(cache_profile.get("cached_training_items_seen") or 0)
    )
    class_count = int(dataset_profile.get("class_count") or 0)
    metric_source = "validation" if validation_metrics else "training" if training_metrics else None
    metrics = validation_metrics or training_metrics or {}
    accuracy = metrics.get("accuracy") if isinstance(metrics, dict) else None
    macro_f1 = metrics.get("macro_f1") if isinstance(metrics, dict) else None
    if accuracy is None and sample_size:
        status = "aguardando novo treino com metricas"
    elif accuracy is None:
        status = "sem amostra de treino registrada"
    else:
        status = "ok"
    return {
        "status": status,
        "accuracy": accuracy,
        "macro_f1": macro_f1,
        "metric_source": metric_source,
        "sample_size": sample_size,
        "class_count": class_count,
        "dataset_profile": dataset_profile,
        "validation_metrics": validation_metrics,
        "training_metrics": training_metrics,
    }


def monitor_evolution_memory_summary() -> dict[str, Any]:
    summary = {
        "events": 0,
        "unique_terms": 0,
        "unique_checks": 0,
        "unique_targets": 0,
        "total_observations": 0,
        "latest_cycle": None,
        "latest_updated_at": None,
    }
    if not MONITOR_MODEL_MEMORY_PATH.exists():
        return summary
    terms: set[str] = set()
    checks: set[str] = set()
    targets: set[str] = set()
    observations = 0
    try:
        with MONITOR_MODEL_MEMORY_PATH.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except Exception:
                    continue
                if not isinstance(entry, dict):
                    continue
                summary["events"] += 1
                summary["latest_cycle"] = entry.get("cycle", summary["latest_cycle"])
                summary["latest_updated_at"] = entry.get("updated_at") or summary["latest_updated_at"]
                new_terms = entry.get("new_terms") if isinstance(entry.get("new_terms"), list) else []
                top_checks = entry.get("top_checks") if isinstance(entry.get("top_checks"), list) else []
                top_targets = entry.get("top_targets") if isinstance(entry.get("top_targets"), list) else []
                observations += len(new_terms) + len(top_checks) + len(top_targets)
                for item in new_terms:
                    if isinstance(item, dict):
                        key = normalize_text(item.get("normalized") or item.get("term"))
                        if key:
                            terms.add(key)
                for item in top_checks:
                    if isinstance(item, dict):
                        key = normalize_text(item.get("id") or item.get("title"))
                        if key:
                            checks.add(key)
                for item in top_targets:
                    if isinstance(item, dict):
                        key = normalize_text(item.get("id") or item.get("term"))
                        if key:
                            targets.add(key)
    except Exception:
        return summary
    summary["unique_terms"] = len(terms)
    summary["unique_checks"] = len(checks)
    summary["unique_targets"] = len(targets)
    summary["total_observations"] = observations
    return summary


def restart_backend_visible_terminal() -> None:
    time.sleep(1.0)
    script = Path("start-coibe-backend.ps1").resolve()
    if os.name == "nt":
        subprocess.Popen(
            [
                "powershell",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-NoExit",
                "-File",
                str(script),
            ],
            cwd=str(Path.cwd()),
            creationflags=getattr(subprocess, "CREATE_NEW_CONSOLE", 0),
        )
    else:
        subprocess.Popen([sys.executable, "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", "8000"], cwd=str(Path.cwd()))
    time.sleep(0.8)
    os._exit(0)


def monitor_model_status() -> dict[str, Any]:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    state = read_monitor_model_state()
    config = read_monitor_model_config()
    registry = read_model_registry(config, state)
    training_lines = 0
    if MONITOR_MODEL_TRAINING_PATH.exists():
        try:
            with MONITOR_MODEL_TRAINING_PATH.open("r", encoding="utf-8") as handle:
                training_lines = sum(1 for _ in handle)
        except Exception:
            training_lines = 0
    memory_summary = monitor_evolution_memory_summary()
    memory_lines = int(memory_summary.get("events") or 0)
    learned_terms = state.get("learned_terms") if isinstance(state.get("learned_terms"), list) else []
    learned_checks = state.get("learned_checks") if isinstance(state.get("learned_checks"), list) else []
    learned_targets = state.get("learned_targets") if isinstance(state.get("learned_targets"), list) else []
    learned_total_count = len(learned_terms) + len(learned_checks) + len(learned_targets)
    learned_accumulated_total_count = max(
        learned_total_count,
        int(memory_summary.get("total_observations") or 0),
        int(memory_summary.get("unique_terms") or 0)
        + int(memory_summary.get("unique_checks") or 0)
        + int(memory_summary.get("unique_targets") or 0),
    )
    model_quality = monitor_model_quality_summary(state)
    return {
        "models_dir": str(MODELS_DIR),
        "state_path": str(MONITOR_MODEL_STATE_PATH),
        "training_path": str(MONITOR_MODEL_TRAINING_PATH),
        "evolution_memory_path": str(MONITOR_MODEL_MEMORY_PATH),
        "config_path": str(MONITOR_MODEL_CONFIG_PATH),
        "state_exists": MONITOR_MODEL_STATE_PATH.exists(),
        "training_history_exists": MONITOR_MODEL_TRAINING_PATH.exists(),
        "evolution_memory_exists": MONITOR_MODEL_MEMORY_PATH.exists(),
        "config": config.model_dump(),
        "training_events": training_lines,
        "evolution_memory_events": memory_lines,
        "version": state.get("version") or "coibe-monitor-v1",
        "updated_at": state.get("updated_at"),
        "cycles": int(state.get("cycles") or 0),
        "learned_total_count": learned_total_count,
        "learned_active_total_count": learned_total_count,
        "learned_accumulated_total_count": learned_accumulated_total_count,
        "learned_terms_count": len(learned_terms),
        "learned_terms": learned_terms[:25],
        "learned_checks_count": len(learned_checks),
        "learned_checks": learned_checks[:25],
        "learned_targets_count": len(learned_targets),
        "learned_targets": learned_targets[:25],
        "selected_model": next((model for model in registry.get("models", []) if model.get("selected")), None),
        "available_models": registry.get("models", []),
        "model_registry_path": str(MONITOR_MODEL_REGISTRY_PATH),
        "model_artifacts": state.get("model_artifacts") if isinstance(state.get("model_artifacts"), dict) else {},
        "model_quality": model_quality,
        "evolution_memory": state.get("evolution_memory") if isinstance(state.get("evolution_memory"), dict) else {},
        "evolution_memory_summary": memory_summary,
        "cache_profile": state.get("cache_profile") if isinstance(state.get("cache_profile"), dict) else {},
        "last_training": state.get("last_training"),
        "gpu": gpu_status(),
        "training_process": monitor_training_status(),
        "legal_safety": "O modelo aprende termos e prioridades para triagem; não conclui crime, culpa, parentesco ou irregularidade sem validação humana.",
    }


def public_monitor_model_summary() -> dict[str, Any]:
    state = read_monitor_model_state()
    config = read_monitor_model_config()
    registry = read_model_registry(config, state)
    learned_terms = state.get("learned_terms") if isinstance(state.get("learned_terms"), list) else []
    learned_checks = state.get("learned_checks") if isinstance(state.get("learned_checks"), list) else []
    learned_targets = state.get("learned_targets") if isinstance(state.get("learned_targets"), list) else []
    return {
        "version": state.get("version") or "coibe-monitor-v1",
        "updated_at": state.get("updated_at"),
        "cycles": int(state.get("cycles") or 0),
        "learned_terms_count": len(learned_terms),
        "learned_checks_count": len(learned_checks),
        "learned_targets_count": len(learned_targets),
        "learned_total_count": len(learned_terms) + len(learned_checks) + len(learned_targets),
        "selected_model": next((model for model in registry.get("models", []) if model.get("selected")), None),
        "available_models_count": len(registry.get("models", [])),
        "model_artifacts": state.get("model_artifacts") if isinstance(state.get("model_artifacts"), dict) else {},
        "model_quality": monitor_model_quality_summary(state),
        "evolution_memory": state.get("evolution_memory") if isinstance(state.get("evolution_memory"), dict) else {},
        "cache_profile": state.get("cache_profile") if isinstance(state.get("cache_profile"), dict) else {},
        "last_training": state.get("last_training") if isinstance(state.get("last_training"), dict) else {},
        "status": "ativo" if state.get("updated_at") else "aguardando primeiro ciclo",
    }


def normalize_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = re.sub(r"[^A-Za-z0-9]+", " ", text)
    return " ".join(text.upper().split())


def split_related_query(value: str | None) -> list[str]:
    parts = [part.strip() for part in re.split(r"[|;]+", str(value or "")) if part.strip()]
    return parts or ([str(value).strip()] if str(value or "").strip() else [])


SEARCH_TYPE_ORDER = {
    "politico_relacionado": 10,
    "politico_deputado": 11,
    "politico_senador": 12,
    "partido_politico": 13,
    "estado": 20,
    "municipio": 21,
    "cnpj": 40,
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
        response.from_cache = True
        response.cache_status = "hit"
        response.cached_at = cached_at
        response.public_api_checked = bool(payload.get("public_api_checked", bool(response.results)))
        response.sources = ["Cache local COIBE.IA", *[source for source in response.sources if source != "Cache local COIBE.IA"]]
        return response
    except Exception:
        return None


def save_cached_universal_search(query: str, response: UniversalSearchResponse) -> None:
    response.from_cache = False
    response.cache_status = "stored"
    response.public_api_checked = True
    payload = {
        "query": query,
        "normalized_query": normalize_text(query),
        "cached_at": brasilia_now().isoformat(),
        "public_api_checked": True,
        "result_count": len(response.results),
        "response": response.model_dump(mode="json"),
    }
    write_json(search_cache_path(query), payload)


def save_universal_search_public_records(query: str, response: UniversalSearchResponse) -> int:
    existing = load_public_records()
    by_key: dict[str, dict[str, Any]] = {}
    for index, record in enumerate(existing):
        key = str(record.get("record_key") or "")
        if not key:
            key = f"legacy:{index}:{record_fingerprint([record.get('title'), record.get('source'), record.get('url')])}"
            record["record_key"] = key
        by_key[key] = record
    added = 0
    collected_at = brasilia_now().isoformat()
    for result in response.results:
        stable = record_fingerprint([query, result.type, result.title, result.subtitle, result.url])
        record_key = f"search:{stable}"
        if record_key not in by_key:
            added += 1
        by_key[record_key] = {
            "record_key": record_key,
            "record_type": result.type,
            "source": result.source,
            "query": query,
            "title": result.title,
            "subtitle": result.subtitle,
            "url": result.url,
            "risk_level": result.risk_level,
            "collected_at": collected_at,
            "monitor_status": "pending_analysis",
            "analysis_requested_at": collected_at,
            "cached_from": "api/search",
            "payload": result.payload,
            "normalized_title": normalize_text(result.title),
            "normalized_source": normalize_text(result.source),
        }
    merged = list(by_key.values())
    merged.sort(key=lambda row: str(row.get("collected_at") or ""), reverse=True)
    write_json(LOCAL_PUBLIC_RECORDS_PATH, merged[:5000])
    return added


def save_political_scan_public_records(record_type: str, query: str | None, items: list[PoliticalScanItem]) -> int:
    if record_type not in {"political_people", "political_parties"} or not items:
        return 0
    existing = load_public_records()
    by_key: dict[str, dict[str, Any]] = {}
    for index, record in enumerate(existing):
        key = str(record.get("record_key") or "")
        if not key:
            key = f"legacy:{index}:{record_fingerprint([record.get('title'), record.get('source'), record.get('url')])}"
            record["record_key"] = key
        by_key[key] = record

    added = 0
    collected_at = brasilia_now().isoformat()
    source_label = "COIBE.IA/Camara/Senado - Politicos" if record_type == "political_people" else "COIBE.IA/Camara/TSE - Partidos"
    for item in items:
        payload = item.model_dump(mode="json")
        display_title = public_record_political_display_title(record_type, item.name, payload, item.subtitle or "")
        stable = record_fingerprint([record_type, item.id, item.name, item.party, item.role])
        record_key = f"{record_type}:{stable}"
        if record_key not in by_key:
            added += 1
        by_key[record_key] = {
            "record_key": record_key,
            "record_type": record_type,
            "source": source_label,
            "query": query,
            "title": display_title,
            "subtitle": item.subtitle,
            "url": item.sources[0].url if item.sources else None,
            "risk_level": item.attention_level,
            "collected_at": collected_at,
            "monitor_status": "pending_analysis",
            "analysis_requested_at": collected_at,
            "cached_from": f"api/{record_type}",
            "payload": payload,
            "normalized_title": normalize_text(display_title),
            "normalized_source": normalize_text(source_label),
        }

    merged = list(by_key.values())
    merged.sort(key=lambda row: str(row.get("collected_at") or ""), reverse=True)
    write_json(LOCAL_PUBLIC_RECORDS_PATH, merged[:5000])
    POLITICAL_LOCAL_ITEMS_CACHE["loaded_at"] = 0.0
    return added


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


def contains_normalized_term(text: str, terms: tuple[str, ...]) -> bool:
    return any(term in text for term in terms)


def logistic_presence_profile(activity: str) -> tuple[bool, str]:
    normalized = normalize_text(activity)
    if not normalized:
        return False, "sem texto de objeto/atividade"

    fixed_presence = contains_normalized_term(normalized, LOGISTIC_FIXED_PRESENCE_TERMS)
    mobile_or_delivery = contains_normalized_term(normalized, LOGISTIC_MOBILE_OR_DELIVERY_TERMS)
    supply_only = contains_normalized_term(normalized, LOGISTIC_SUPPLY_ONLY_TERMS)

    if mobile_or_delivery:
        return False, "objeto envolve transporte, entrega ou deslocamento"
    if supply_only and not fixed_presence:
        return False, "objeto parece compra/fornecimento sem execucao presencial obrigatoria"
    if fixed_presence:
        return True, "objeto exige equipe ou execucao presencial no local"
    return False, "sem indicio forte de execucao presencial fixa"


def requires_physical_presence(activity: str) -> bool:
    return logistic_presence_profile(activity)[0]


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


def city_coordinate_cache_key(city: str | None, uf: str | None) -> str:
    return f"{normalize_text(city)}|{str(uf or '').strip().upper()}"


def read_city_coordinates_cache() -> dict[str, Any]:
    cached = read_data_json(CITY_COORDINATES_CACHE_PATH, {})
    return cached if isinstance(cached, dict) else {}


def write_city_coordinates_cache(cache: dict[str, Any]) -> None:
    write_json(CITY_COORDINATES_CACHE_PATH, cache)


def lonlat_ring_area_centroid(ring: list[Any]) -> tuple[float, float, float] | None:
    area = 0.0
    cx = 0.0
    cy = 0.0
    for index in range(len(ring)):
        try:
            x1, y1 = float(ring[index][0]), float(ring[index][1])
            x2, y2 = float(ring[(index + 1) % len(ring)][0]), float(ring[(index + 1) % len(ring)][1])
        except (TypeError, ValueError, IndexError):
            continue
        cross = x1 * y2 - x2 * y1
        area += cross
        cx += (x1 + x2) * cross
        cy += (y1 + y2) * cross
    area /= 2.0
    if not area:
        return None
    return area, cx / (6.0 * area), cy / (6.0 * area)


def lonlat_geometry_centroid(geometry: dict[str, Any] | None) -> tuple[float, float] | None:
    if not isinstance(geometry, dict):
        return None
    coordinates = geometry.get("coordinates")
    if geometry.get("type") == "Polygon":
        rings = coordinates if isinstance(coordinates, list) else []
    elif geometry.get("type") == "MultiPolygon":
        rings = [ring for polygon in coordinates or [] for ring in polygon]
    else:
        rings = []

    best: tuple[float, float, float] | None = None
    for ring in rings:
        if not isinstance(ring, list):
            continue
        centroid = lonlat_ring_area_centroid(ring)
        if centroid and (best is None or abs(centroid[0]) > abs(best[0])):
            best = centroid
    if best is None:
        return None
    _, lng, lat = best
    if not (-180 <= lng <= 180 and -90 <= lat <= 90):
        return None
    return lat, lng


async def fetch_ibge_city_centroid(city: str | None, uf: str | None) -> tuple[float, float] | None:
    if not city:
        return None
    ibge_city = await fetch_city_from_ibge(city, uf)
    city_id = ibge_city.get("id") if isinstance(ibge_city, dict) else None
    if not city_id:
        return None
    data = await get_json(
        IBGE_MUNICIPALITY_GEOJSON_URL.format(city_id=city_id),
        params={"formato": "application/vnd.geo+json", "qualidade": "minima"},
    )
    features = data.get("features") if isinstance(data, dict) else None
    if not features:
        return None
    return lonlat_geometry_centroid(features[0].get("geometry") if isinstance(features[0], dict) else None)


async def geocode_city_with_nominatim(city: str | None, uf: str | None) -> tuple[float, float] | None:
    if not city:
        return None
    state_name = UF_NAMES.get(str(uf or "").upper(), str(uf or ""))
    query = ", ".join(part for part in [city, state_name, "Brasil"] if part)
    await asyncio.sleep(max(0.0, CITY_GEOCODING_DELAY_SECONDS))
    data = await get_json(
        OSM_NOMINATIM_SEARCH_URL,
        params={
            "q": query,
            "format": "jsonv2",
            "limit": 5,
            "countrycodes": "br",
            "addressdetails": 1,
        },
    )
    if not isinstance(data, list):
        return None
    normalized_city = normalize_text(city)
    normalized_uf = str(uf or "").upper()
    normalized_state = normalize_text(state_name)
    for row in data:
        if not isinstance(row, dict):
            continue
        address = row.get("address") if isinstance(row.get("address"), dict) else {}
        row_city = (
            address.get("city")
            or address.get("town")
            or address.get("village")
            or address.get("municipality")
            or address.get("county")
            or row.get("name")
            or ""
        )
        row_state = address.get("state") or ""
        if normalized_state and normalize_text(row_state) != normalized_state:
            continue
        city_ratio = SequenceMatcher(None, normalized_city, normalize_text(row_city)).ratio()
        if city_ratio < 0.78 and normalized_city not in normalize_text(row_city):
            continue
        lat = parse_decimal(row.get("lat"))
        lng = parse_decimal(row.get("lon"))
        if lat is None or lng is None:
            continue
        lat_float = float(lat)
        lng_float = float(lng)
        if not (-35 <= lat_float <= 7 and -75 <= lng_float <= -32):
            continue
        if normalized_uf and normalized_uf in UF_CAPITAL_COORDS:
            capital_lat, capital_lng = UF_CAPITAL_COORDS[normalized_uf]
            if abs(lat_float - capital_lat) < 0.01 and abs(lng_float - capital_lng) < 0.01 and normalize_text(row_city) != normalized_city:
                continue
        return lat_float, lng_float
    return None


async def coords_for_map_point(
    city: str | None,
    uf: str | None,
    cache: dict[str, Any],
    lookup_counter: dict[str, int],
) -> tuple[float, float, str, bool] | None:
    key = city_coordinate_cache_key(city, uf)
    cached = cache.get(key)
    if isinstance(cached, dict):
        lat = cached.get("lat")
        lng = cached.get("lng")
        source = str(cached.get("source") or "")
        city_has_name = bool(city and city.strip() and city.lower() != "brasil")
        if isinstance(lat, (int, float)) and isinstance(lng, (int, float)) and not (city_has_name and source == "uf_capital_fallback"):
            return float(lat), float(lng), str(cached.get("source") or "ibge_municipality_centroid_cache"), False

    if city and city.lower() != "brasil" and lookup_counter["count"] < CITY_COORDINATES_LOOKUP_LIMIT:
        lookup_counter["count"] += 1
        try:
            centroid = await fetch_ibge_city_centroid(city, uf)
        except HTTPException:
            centroid = None
        if centroid:
            lat, lng = centroid
            cache[key] = {
                "city": city,
                "uf": uf,
                "lat": lat,
                "lng": lng,
                "source": "ibge_municipality_centroid",
                "updated_at": brasilia_now().isoformat(),
            }
            return lat, lng, "ibge_municipality_centroid", True
        try:
            geocoded = await geocode_city_with_nominatim(city, uf)
        except HTTPException:
            geocoded = None
        if geocoded:
            lat, lng = geocoded
            cache[key] = {
                "city": city,
                "uf": uf,
                "lat": lat,
                "lng": lng,
                "source": "osm_nominatim_city",
                "updated_at": brasilia_now().isoformat(),
            }
            return lat, lng, "osm_nominatim_city", True

    if city and city.strip().upper() in CITY_COORDS:
        lat, lng = CITY_COORDS[city.strip().upper()]
        cache[key] = {
            "city": city,
            "uf": uf,
            "lat": lat,
            "lng": lng,
            "source": "city_reference",
            "updated_at": brasilia_now().isoformat(),
        }
        return lat, lng, "city_reference", True

    if city and city.strip() and city.lower() != "brasil":
        cache[key] = {
            "city": city,
            "uf": uf,
            "source": "unresolved_ibge_city",
            "updated_at": brasilia_now().isoformat(),
        }
        return None

    lat, lng = coords_for(city, uf)
    source = "uf_capital_fallback"
    cache[key] = {
        "city": city,
        "uf": uf,
        "lat": lat,
        "lng": lng,
        "source": source,
        "updated_at": brasilia_now().isoformat(),
    }
    return lat, lng, source, True


def filter_map_points(
    points: list[MonitoringMapPoint],
    q: str | None = None,
    uf: str | None = None,
    city: str | None = None,
    min_lat: float | None = None,
    max_lat: float | None = None,
    min_lng: float | None = None,
    max_lng: float | None = None,
) -> list[MonitoringMapPoint]:
    normalized_query = normalize_text(q if isinstance(q, str) else None)
    uf_filter = (uf if isinstance(uf, str) else "").strip().upper()
    city_filter = normalize_text(city)
    filtered: list[MonitoringMapPoint] = []
    for point in points:
        if uf_filter and point.uf.upper() != uf_filter:
            continue
        if city_filter and city_filter not in normalize_text(point.city):
            continue
        if normalized_query:
            text = normalize_text(f"{point.city} {point.uf}")
            if normalized_query not in text:
                continue
        if min_lat is not None and point.lat < min_lat:
            continue
        if max_lat is not None and point.lat > max_lat:
            continue
        if min_lng is not None and point.lng < min_lng:
            continue
        if max_lng is not None and point.lng > max_lng:
            continue
        filtered.append(point)
    filtered.sort(key=lambda point: (point.risk_score, point.total_value, point.alerts_count), reverse=True)
    return filtered


async def build_monitoring_map_points_from_items(items: list[MonitoringItem]) -> list[MonitoringMapPoint]:
    grouped: dict[str, dict[str, Any]] = {}
    for item in items:
        key = f"{item.city or 'Brasil'}-{item.uf or 'BR'}"
        bucket = grouped.setdefault(
            key,
            {
                "city": item.city or "Brasil",
                "uf": item.uf or "BR",
                "lat": 0.0,
                "lng": 0.0,
                "risk_score": 0,
                "total_value": Decimal("0"),
                "alerts_count": 0,
                "spatial_source": "pending",
            },
        )
        bucket["risk_score"] = max(bucket["risk_score"], item.risk_score)
        bucket["total_value"] += item.value
        bucket["alerts_count"] += 1

    coordinate_cache = read_city_coordinates_cache()
    lookup_counter = {"count": 0}
    cache_changed = False
    unresolved_keys: list[str] = []
    for bucket in grouped.values():
        resolved = await coords_for_map_point(
            str(bucket.get("city") or ""),
            str(bucket.get("uf") or ""),
            coordinate_cache,
            lookup_counter,
        )
        if resolved is None:
            unresolved_keys.append(f"{bucket.get('city')}-{bucket.get('uf')}")
            cache_changed = True
            continue
        lat, lng, source, changed = resolved
        bucket["lat"] = lat
        bucket["lng"] = lng
        bucket["spatial_source"] = source
        cache_changed = cache_changed or changed
    if cache_changed:
        write_city_coordinates_cache(coordinate_cache)

    points = [
        MonitoringMapPoint(**bucket)
        for key, bucket in grouped.items()
        if key not in unresolved_keys
    ]
    points.sort(key=lambda point: (point.risk_score, point.total_value, point.alerts_count), reverse=True)
    return points


def write_map_points_cache(points: list[MonitoringMapPoint]) -> None:
    write_ttl_json(
        MONITORING_MAP_CACHE_PATH,
        {
            "version": MAP_POINTS_CACHE_VERSION,
            "generated_at": brasilia_now().isoformat(),
            "points": [point.model_dump(mode="json") for point in points],
        },
    )


def read_map_points_cache() -> list[MonitoringMapPoint]:
    cached = read_ttl_json(MONITORING_MAP_CACHE_PATH, MAP_CACHE_TTL_SECONDS, {})
    if not isinstance(cached, dict) or cached.get("version") != MAP_POINTS_CACHE_VERSION:
        return []
    rows = cached.get("points")
    if not isinstance(rows, list):
        return []
    points: list[MonitoringMapPoint] = []
    for row in rows:
        try:
            points.append(MonitoringMapPoint.model_validate(row))
        except Exception:
            continue
    return points


def ensure_postgis_schema() -> bool:
    global POSTGIS_SCHEMA_READY
    if POSTGIS_SCHEMA_READY:
        return True
    if not POSTGIS_ENABLED:
        return False
    try:
        import psycopg

        with psycopg.connect(POSTGIS_DATABASE_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS coibe_monitoring_map_cache (
                        city text NOT NULL,
                        uf text NOT NULL,
                        geom geometry(Point, 4326) NOT NULL,
                        risk_score integer NOT NULL DEFAULT 0,
                        total_value numeric NOT NULL DEFAULT 0,
                        alerts_count integer NOT NULL DEFAULT 0,
                        updated_at timestamptz NOT NULL DEFAULT now(),
                        PRIMARY KEY (city, uf)
                    )
                    """
                )
                cur.execute("CREATE INDEX IF NOT EXISTS coibe_monitoring_map_cache_geom_idx ON coibe_monitoring_map_cache USING gist (geom)")
                cur.execute("CREATE INDEX IF NOT EXISTS coibe_monitoring_map_cache_updated_idx ON coibe_monitoring_map_cache (updated_at DESC)")
        POSTGIS_SCHEMA_READY = True
        return True
    except Exception as exc:
        print(f"PostGIS indisponivel para cache do mapa: {exc}")
        return False


def postgis_upsert_map_points(points: list[MonitoringMapPoint]) -> bool:
    if not points or not ensure_postgis_schema():
        return False
    try:
        import psycopg

        rows = [
            (
                point.city,
                point.uf,
                float(point.lng),
                float(point.lat),
                int(point.risk_score),
                str(point.total_value),
                int(point.alerts_count),
            )
            for point in points
        ]
        with psycopg.connect(POSTGIS_DATABASE_URL, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO coibe_monitoring_map_cache
                        (city, uf, geom, risk_score, total_value, alerts_count, updated_at)
                    VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, now())
                    ON CONFLICT (city, uf) DO UPDATE SET
                        geom = EXCLUDED.geom,
                        risk_score = EXCLUDED.risk_score,
                        total_value = EXCLUDED.total_value,
                        alerts_count = EXCLUDED.alerts_count,
                        updated_at = now()
                    """,
                    rows,
                )
        return True
    except Exception as exc:
        print(f"Falha ao gravar cache PostGIS do mapa: {exc}")
        return False


def postgis_read_map_points(
    q: str | None = None,
    uf: str | None = None,
    city: str | None = None,
    min_lat: float | None = None,
    max_lat: float | None = None,
    min_lng: float | None = None,
    max_lng: float | None = None,
) -> list[MonitoringMapPoint]:
    if not ensure_postgis_schema():
        return []
    try:
        import psycopg

        conditions = ["updated_at >= now() - (%s * interval '1 second')"]
        params: list[Any] = [MAP_CACHE_TTL_SECONDS]
        if uf:
            conditions.append("upper(uf) = %s")
            params.append(uf.upper())
        if None not in (min_lng, min_lat, max_lng, max_lat):
            conditions.append("geom && ST_MakeEnvelope(%s, %s, %s, %s, 4326)")
            params.extend([min_lng, min_lat, max_lng, max_lat])
        sql = f"""
            SELECT city, uf, ST_Y(geom), ST_X(geom), risk_score, total_value::text, alerts_count
            FROM coibe_monitoring_map_cache
            WHERE {' AND '.join(conditions)}
            ORDER BY risk_score DESC, total_value DESC, alerts_count DESC
            LIMIT 500
        """
        points: list[MonitoringMapPoint] = []
        with psycopg.connect(POSTGIS_DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for point_city, point_uf, lat, lng, risk_score, total_value, alerts_count in cur.fetchall():
                    points.append(
                        MonitoringMapPoint(
                            city=point_city,
                            uf=point_uf,
                            lat=float(lat),
                            lng=float(lng),
                            risk_score=int(risk_score or 0),
                            total_value=parse_decimal(total_value) or Decimal("0"),
                            alerts_count=int(alerts_count or 0),
                        )
                    )
        return filter_map_points(points, q=q, uf=uf, city=city, min_lat=min_lat, max_lat=max_lat, min_lng=min_lng, max_lng=max_lng)
    except Exception as exc:
        print(f"Falha ao ler cache PostGIS do mapa: {exc}")
        return []


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
    if str(object_text or "").strip():
        return requires_physical_presence(object_text)
    return requires_physical_presence(company_cnae_text(company))


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
    if not company:
        return None
    object_requires_presence, logistic_reason = logistic_presence_profile(object_text)
    if str(object_text or "").strip():
        if not object_requires_presence:
            return None
    elif not company_physical_activity(company, object_text):
        return None
    company_city = company.get("municipio")
    company_uf = company.get("uf")
    if not company_city or not company_uf or not item_uf:
        return None
    company_lat, company_lng = coords_for(str(company_city), str(company_uf))
    agency_lat, agency_lng = coords_for(item_city, item_uf)
    distance = haversine_distance_km(company_lat, company_lng, agency_lat, agency_lng)
    has_risk = (
        distance > LOGISTIC_DISTANCE_THRESHOLD_KM
        and str(company_uf).upper() != str(item_uf).upper()
        and value >= LOGISTIC_MIN_CONTRACT_VALUE
    )
    if not has_risk:
        return None
    return RedFlagResult(
        code="RF03",
        title="Inviabilidade Logistica Potencial",
        has_risk=True,
        risk_level="alto",
        message=f"Fornecedor em {company_city}/{company_uf} esta a aproximadamente {distance:.0f} km do orgao em {item_city or 'municipio nao informado'}/{item_uf}; o objeto indica execucao presencial fixa.",
        evidence={
            "supplier_location": f"{company_city}/{company_uf}",
            "agency_location": f"{item_city or ''}/{item_uf or ''}",
            "distance_km": round(distance, 2),
            "valor_global": str(value),
            "logistic_reason": logistic_reason,
            "cnae": company_cnae_text(company)[:240],
            "branch_registry_available": False,
        },
        criteria={"rule": f"objeto com execucao presencial fixa E distancia > {LOGISTIC_DISTANCE_THRESHOLD_KM} km E UF diferente E valor >= R$ 500.000; ignora transporte, entrega, frete e compra/fornecimento sem execucao local"},
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


async def fetch_current_senators() -> list[dict[str, Any]]:
    now = time.time()
    cached_items = SENATORS_PUBLIC_CACHE.get("items") or []
    if cached_items and now - float(SENATORS_PUBLIC_CACHE.get("loaded_at") or 0) < SENATORS_PUBLIC_CACHE_TTL_SECONDS:
        return list(cached_items)

    data = await get_json(SENADO_SENADORES_URL)
    senators = (
        data.get("ListaParlamentarEmExercicio", {})
        .get("Parlamentares", {})
        .get("Parlamentar", [])
    )
    if not isinstance(senators, list):
        senators = [senators] if senators else []
    SENATORS_PUBLIC_CACHE["loaded_at"] = now
    SENATORS_PUBLIC_CACHE["items"] = senators
    return list(senators)


async def search_senators(query: str) -> list[UniversalSearchResult]:
    try:
        senators = await fetch_current_senators()
    except HTTPException:
        return []

    normalized = query.strip().lower()
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


def political_attention_level(score: int) -> str:
    if score >= 70:
        return "alto"
    if score >= 35:
        return "médio"
    return "baixo"


def political_attention_rank(level: str | None) -> int:
    return {"alto": 3, "medio": 2, "baixo": 1}.get(normalize_text(level).lower(), 0)


def recompute_political_attention(item: PoliticalScanItem) -> PoliticalScanItem:
    score = 0
    has_medium_or_high_risk = False
    has_high_risk = False
    for risk in item.risks or []:
        level_rank = political_attention_rank(risk.level)
        if level_rank >= 3:
            score += 45
            has_high_risk = True
        elif level_rank >= 2:
            score += 25
            has_medium_or_high_risk = True
        elif level_rank == 1:
            score += 5

    total = item.total_public_money or Decimal("0")
    travel = item.travel_public_money or Decimal("0")
    records = int(item.records_count or 0)
    if total >= Decimal("1000000"):
        score += 35
    elif total >= Decimal("350000"):
        score += 20
    if travel >= Decimal("90000"):
        score += 15
    if records >= 300:
        score += 15
    elif records >= 100:
        score += 8

    detail_types = {
        normalize_text(detail.get("type")).lower()
        for detail in item.analysis_details
        if isinstance(detail, dict)
    }
    if detail_types.intersection({"contratos", "doacoes", "contas", "processos", "controle", "vinculos"}):
        score += 10

    computed = political_attention_level(min(score, 100))
    if has_high_risk:
        computed = "alto"
    elif has_medium_or_high_risk and political_attention_rank(computed) < 2:
        computed = "médio"
    if political_attention_rank(computed) > political_attention_rank(item.attention_level):
        item.attention_level = computed
    return item


def political_priority_for(name: str, role: str | None = None, party: str | None = None, item_type: str | None = None) -> tuple[int, str | None]:
    normalized_name = normalize_text(name)
    normalized_role = normalize_text(role)
    normalized_party = normalize_text(party)
    if any(term in normalized_role for term in ("PRESIDENTE DA REPUBLICA", "PRESIDENTE DO BRASIL")) and "VICE" not in normalized_role and "EX" not in normalized_role:
        return 100, "Presidencia da Republica atual"
    if "VICE PRESIDENTE" in normalized_role or "VICE-PRESIDENTE" in normalized_role:
        return 95, "Vice-presidencia da Republica atual"
    if "EX PRESIDENTE" in normalized_role or "EX-PRESIDENTE" in normalized_role:
        return 80, "Ex-presidente da Republica"

    for person in POLITICAL_RELATED_PEOPLE:
        names = [person.get("title"), *(person.get("aliases") or [])]
        if any(normalized_name == normalize_text(value) or normalize_text(value) in normalized_name for value in names if value):
            return int(person.get("priority_score") or 70), str(person.get("subtitle") or "Pessoa publica prioritaria")

    if normalized_party in POLITICAL_PRIORITY_PARTIES:
        party_priority = POLITICAL_PRIORITY_PARTIES[normalized_party]
        if item_type == "partido":
            return party_priority, f"Partido prioritario: {normalized_party}"
        return min(party_priority, 70), f"Partido prioritario no recorte politico: {normalized_party}"
    if "SENADOR" in normalized_role:
        return 65, "Senado Federal"
    if "DEPUTADO FEDERAL" in normalized_role and "EX" not in normalized_role:
        return 55, "Camara dos Deputados - mandato atual"
    if "EX DEPUTADO" in normalized_role:
        return 40, "Ex-mandato federal"
    if item_type == "partido":
        return POLITICAL_PRIORITY_PARTIES.get(normalized_party, 35), "Partido politico monitorado"
    return 25, None


def apply_political_priority(item: PoliticalScanItem) -> PoliticalScanItem:
    score, reason = political_priority_for(item.name, item.role, item.party, item.type)
    item.priority_score = max(item.priority_score or 0, score)
    if reason and not item.priority_reason:
        item.priority_reason = reason
    already_has_priority_detail = any(
        isinstance(detail, dict) and detail.get("type") == "prioridade"
        for detail in item.analysis_details
    )
    if item.priority_reason and not already_has_priority_detail:
        item.analysis_details.append(
            {
                "type": "prioridade",
                "title": "Prioridade de varredura",
                "description": (
                    "Este item aparece antes por prioridade institucional do monitor. "
                    "A ordem prioriza cargos maiores, Presidencia, Vice-Presidencia, partidos ligados ao Executivo federal e ex-presidentes, sem limitar a varredura a eles."
                ),
                "person": item.name,
                "party": item.party,
                "role": item.role,
                "priority_score": item.priority_score,
                "priority_reason": item.priority_reason,
            }
        )
        if "prioridade" not in item.analysis_types:
            item.analysis_types.append("prioridade")
    return recompute_political_attention(item)


def political_sort_key(item: PoliticalScanItem) -> tuple[int, int, Decimal, int, str]:
    risk_rank = {"alto": 3, "médio": 2, "medio": 2, "baixo": 1}
    priority = item.priority_score or political_priority_for(item.name, item.role, item.party, item.type)[0]
    return (
        priority,
        risk_rank.get(normalize_text(item.attention_level).lower(), 0),
        item.total_public_money,
        item.records_count,
        item.name,
    )


def expense_value(expense: dict[str, Any]) -> Decimal:
    return parse_decimal(expense.get("valorLiquido") or expense.get("valorDocumento") or expense.get("valorGlosa") or 0) or Decimal("0")


def expense_kind_text(expense: dict[str, Any]) -> str:
    return normalize_text(expense.get("tipoDespesa") or expense.get("descricao") or "")


def is_travel_expense(expense: dict[str, Any]) -> bool:
    text = expense_kind_text(expense)
    return any(term in text for term in ("PASSAGEM", "AEREA", "HOSPEDAGEM", "LOCOMOCAO", "TAXI", "VEICULO", "COMBUSTIVEL"))


def political_analysis_type_from_text(text: str) -> str:
    normalized = normalize_text(text)
    if any(term in normalized for term in ("CONTRATO", "LICITACAO", "COMPRA", "FORNECEDOR", "EMPENHO")):
        return "contratos"
    if any(term in normalized for term in ("PASSAGEM", "AEREA", "HOSPEDAGEM", "LOCOMOCAO", "TAXI", "VEICULO", "COMBUSTIVEL")):
        return "viagem"
    if any(term in normalized for term in ("DIVULGACAO", "PUBLICIDADE", "COMUNICACAO", "INTERNET", "TELEFONIA")):
        return "comunicacao"
    if any(term in normalized for term in ("CONSULTORIA", "ASSESSORIA", "SERVICO", "PESQUISA")):
        return "servicos"
    if any(term in normalized for term in ("ALUGUEL", "IMOVEL", "ESCRITORIO", "MANUTENCAO")):
        return "estrutura"
    return "outros"


def political_analysis_types_from_expenses(expenses: list[dict[str, Any]]) -> list[str]:
    types = {political_analysis_type_from_text(str(expense.get("tipoDespesa") or "")) for expense in expenses}
    if expenses:
        types.add("despesas")
    return sorted(types)


def expense_analysis_details(expenses: list[dict[str, Any]], limit: int = 8) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    sorted_expenses = sorted(expenses, key=lambda expense: expense_value(expense), reverse=True)
    for expense in sorted_expenses[:limit]:
        analysis_type = political_analysis_type_from_text(str(expense.get("tipoDespesa") or ""))
        details.append(
            {
                "type": analysis_type,
                "title": str(expense.get("tipoDespesa") or "Despesa pública"),
                "description": (
                    "Despesa ligada a viagem/deslocamento; a fonte informa tipo, data, fornecedor e documento fiscal. "
                    "Destino, motivo e dias exatos só aparecem se estiverem no documento oficial vinculado."
                    if analysis_type == "viagem"
                    else "Despesa pública lida por tipo, data, fornecedor, valor e documento fiscal na fonte oficial."
                ),
                "date": expense.get("dataDocumento"),
                "month": expense.get("mes"),
                "year": expense.get("ano"),
                "supplier": expense.get("nomeFornecedor"),
                "supplier_document": expense.get("cnpjCpfFornecedor"),
                "value": str(expense_value(expense)),
                "document_url": expense.get("urlDocumento"),
                "travel_location": "não informado pela API; verificar documento fiscal",
                "travel_reason": "não informado pela API; verificar documento fiscal",
                "travel_days": "estimado somente quando houver sequência de datas no documento",
            }
        )
    return details


def contract_crosscheck_detail(name: str, role: str | None = None, party: str | None = None) -> dict[str, Any]:
    target = party or name
    detail: dict[str, Any] = {
        "type": "contratos",
        "title": f"Busca de contratos para {target}",
        "description": (
            f"O COIBE pesquisou {target} em contratos, fornecedores, órgãos e pessoas relacionadas. "
            "Os itens abaixo mostram valor, fornecedor, órgão e termo que puxou a correspondência."
        ),
        "person": name,
        "role": role,
        "source": "Compras.gov.br, PNCP e Portal da Transparência",
    }
    if party:
        detail["party"] = party
    return detail


def political_related_terms(name: str, party: str | None = None, people: list[str] | None = None) -> list[str]:
    terms: list[str] = []
    for value in [name, party or "", *(people or [])]:
        clean = str(value or "").strip()
        if len(clean) >= 2:
            terms.append(clean)
    normalized_seen: set[str] = set()
    unique_terms: list[str] = []
    for term in terms:
        normalized = normalize_text(term)
        if normalized and normalized not in normalized_seen:
            normalized_seen.add(normalized)
            unique_terms.append(term)
    return unique_terms[:12]


def cached_local_monitoring_items_for_political() -> list[MonitoringItem]:
    now = time.monotonic()
    if now - float(POLITICAL_LOCAL_ITEMS_CACHE.get("loaded_at") or 0) < 90:
        cached_items = POLITICAL_LOCAL_ITEMS_CACHE.get("items") or []
        if isinstance(cached_items, list):
            return cached_items
    items = load_local_monitoring_items()
    POLITICAL_LOCAL_ITEMS_CACHE["loaded_at"] = now
    POLITICAL_LOCAL_ITEMS_CACHE["items"] = items
    return items


def political_term_matches_item(item: MonitoringItem, normalized_terms: list[str], digit_terms: list[str]) -> bool:
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
    for term in normalized_terms:
        tokens = [token for token in term.split() if len(token) >= 2]
        if term and (term in searchable_normalized or all(token in searchable_normalized for token in tokens[:6])):
            return True
    return any(digits and digits in searchable_digits for digits in digit_terms)


def local_contract_crosscheck_for_political(
    name: str,
    role: str | None = None,
    party: str | None = None,
    people: list[str] | None = None,
    limit: int = 6,
) -> tuple[list[dict[str, Any]], list[PoliticalRiskFactor], list[str]]:
    terms = political_related_terms(name, party, people)
    if not terms:
        return [], [], []

    matched: dict[str, MonitoringItem] = {}
    term_hits: dict[str, int] = {}
    normalized_terms = [normalize_text(term) for term in terms]
    digit_terms = ["".join(ch for ch in term if ch.isdigit()) for term in terms]
    for item in cached_local_monitoring_items_for_political():
        if not political_term_matches_item(item, normalized_terms, digit_terms):
            continue
        key = f"{item.id}:{item.date.isoformat()}"
        matched.setdefault(key, item)
        for term, normalized_term, digit_term in zip(terms, normalized_terms, digit_terms):
            if political_term_matches_item(item, [normalized_term], [digit_term]):
                term_hits[term] = min(term_hits.get(term, 0) + 1, limit)
        if len(matched) >= limit * 4:
            break

    items = sorted(
        matched.values(),
        key=lambda item: (item.risk_score, item.value, item.date.isoformat()),
        reverse=True,
    )[:limit]
    details: list[dict[str, Any]] = []
    linked_people: list[str] = []
    for item in items:
        linked_people.extend([value for value in [item.supplier_name, item.entity] if value])
        matched_terms = [term for term, count in term_hits.items() if count > 0][:6]
        supplier_text = item.supplier_name or "fornecedor não informado"
        entity_text = item.entity or "órgão não informado"
        terms_text = ", ".join(matched_terms) if matched_terms else name
        risk_text = f"atenção {item.risk_level}" if item.risk_level else "atenção não informada"
        details.append(
            {
                "type": "contratos",
                "title": f"{money(item.value)} em contrato com {supplier_text}",
                "description": (
                    f"Termo(s) que ligaram ao recorte: {terms_text}. Órgão: {entity_text}. "
                    f"Nível do registro: {risk_text}, score {item.risk_score}. Confira objeto, fornecedor, data e fonte oficial."
                ),
                "person": name,
                "role": role,
                "party": party,
                "supplier": item.supplier_name,
                "supplier_document": item.supplier_cnpj,
                "entity": item.entity,
                "value": str(item.value),
                "date": item.date.isoformat(),
                "risk_level": item.risk_level,
                "risk_score": item.risk_score,
                "contract_id": item.id,
                "document_url": item.report.official_sources[0].url if item.report.official_sources else None,
                "matched_terms": matched_terms,
            }
        )

    risks: list[PoliticalRiskFactor] = []
    total = sum((item.value for item in items), Decimal("0"))
    high_items = [item for item in items if item.value >= POLITICAL_HIGH_VALUE_CONTRACT_THRESHOLD or item.risk_level == "alto"]
    if items:
        risks.append(
            PoliticalRiskFactor(
                level="médio" if high_items or total >= POLITICAL_HIGH_VALUE_CONTRACT_THRESHOLD else "baixo",
                title=f"{len(items)} contrato(s) ligados ao recorte",
                message=(
                    f"Valor somado: {money(total)}. {len(high_items)} contrato(s) tem valor alto ou risco alto. "
                    "A ligação veio por termo textual; confira fornecedor, órgão, objeto e documento oficial."
                ),
                evidence={
                    "matches_count": len(items),
                    "total_related_value": str(total),
                    "high_value_or_high_risk_matches": len(high_items),
                    "terms_checked": terms,
                },
                source="Base COIBE.IA de contratos acumulados",
                url=items[0].report.official_sources[0].url if items and items[0].report.official_sources else COMPRAS_PUBLIC_PORTAL_URL,
            )
        )
    return details, risks, list(dict.fromkeys([person for person in linked_people if person]))[:8]


def electoral_donation_crosscheck_for_political(
    name: str,
    role: str | None = None,
    party: str | None = None,
    people: list[str] | None = None,
    limit: int = 5,
) -> tuple[list[dict[str, Any]], list[PoliticalRiskFactor]]:
    terms = political_related_terms(name, party, people)
    records = []
    for record in load_public_records():
        searchable_source = normalize_text(record.get("source"))
        searchable_type = normalize_text(record.get("record_type"))
        searchable_title = normalize_text(record.get("title"))
        if not any(term in f"{searchable_source} {searchable_type} {searchable_title}" for term in ("TSE", "DOACAO", "DOACOES", "ELEITORAL", "CAMPANHA", "CONTAS")):
            continue
        haystack = normalize_text(json.dumps(record, ensure_ascii=False))
        if any(normalize_text(term) in haystack for term in terms):
            records.append(record)

    details: list[dict[str, Any]] = [
        {
            "type": "doacoes",
            "title": f"{len(records)} registro(s) eleitoral(is) ligado(s) a {party or name}",
            "description": (
                f"Termos pesquisados: {', '.join(terms[:6])}. "
                "Confira doador, recebedor, valor, data e partido no TSE antes de concluir."
            ),
            "person": name,
            "role": role,
            "party": party,
            "source": "TSE Divulgação de Candidaturas e Contas",
            "document_url": TSE_CONTAS_ELEITORAIS_URL,
            "matched_records": len(records),
            "terms_checked": terms,
        }
    ]
    for record in records[:limit]:
        record_title = str(record.get("title") or "Registro eleitoral relacionado")
        record_source = str(record.get("source") or "fonte eleitoral pública")
        details.append(
            {
                "type": "doacoes",
                "title": record_title,
                "description": (
                    f"Fonte: {record_source}. Correspondência textual com {party or name}; "
                    "abra a fonte e confira nomes, CPF/CNPJ, valor e campanha."
                ),
                "person": name,
                "party": party,
                "source": record.get("source"),
                "document_url": record.get("url") or TSE_CONTAS_ELEITORAIS_URL,
                "record_type": record.get("record_type"),
            }
        )

    risks: list[PoliticalRiskFactor] = []
    if records:
        risks.append(
            PoliticalRiskFactor(
                level="médio",
                title=f"{len(records)} registro(s) eleitoral(is) para checar",
                message=(
                    f"Termos que puxaram a correspondência: {', '.join(terms[:6])}. "
                    "Valide no TSE se o registro é da mesma pessoa, partido, fornecedor ou campanha."
                ),
                evidence={"matching_records": len(records), "terms_checked": terms},
                source="TSE e base pública carregada no COIBE.IA",
                url=TSE_CONTAS_ELEITORAIS_URL,
            )
        )
    return details, risks


def proximity_money_flow_attention(
    name: str,
    total_by_person: dict[str, Decimal],
    role: str | None = None,
    party: str | None = None,
) -> tuple[list[dict[str, Any]], list[PoliticalRiskFactor], list[str]]:
    high_value_people = [
        (person, value)
        for person, value in sorted(total_by_person.items(), key=lambda row: row[1], reverse=True)
        if person and person != name and value >= POLITICAL_HIGH_VALUE_PERSON_THRESHOLD
    ]
    details = [
        {
            "type": "vinculos",
            "title": f"{len(high_value_people)} nome(s) com valor acima de {money(POLITICAL_HIGH_VALUE_PERSON_THRESHOLD)}",
            "description": (
                "O COIBE somou valores por nomes ligados ao recorte. "
                "Proximidade aqui é textual/operacional na base, não parentesco confirmado."
            ),
            "person": name,
            "role": role,
            "party": party,
            "threshold": str(POLITICAL_HIGH_VALUE_PERSON_THRESHOLD),
            "people_checked": len(total_by_person),
            "high_value_people": [{"name": person, "value": str(value)} for person, value in high_value_people[:6]],
        }
    ]
    risks: list[PoliticalRiskFactor] = []
    if high_value_people:
        total = sum((value for _, value in high_value_people), Decimal("0"))
        risks.append(
            PoliticalRiskFactor(
                level="médio",
                title=f"{money(total)} concentrados em nomes do recorte",
                message=(
                    f"{len(high_value_people)} nome(s) passaram de {money(POLITICAL_HIGH_VALUE_PERSON_THRESHOLD)}. "
                    "Confira documentos, fornecedor, pessoa/partido e repetição antes de concluir."
                ),
                evidence={
                    "total_high_value_people": len(high_value_people),
                    "total_high_value_money": str(total),
                    "threshold": str(POLITICAL_HIGH_VALUE_PERSON_THRESHOLD),
                    "examples": [{"name": person, "value": str(value)} for person, value in high_value_people[:4]],
                },
                source="COIBE.IA - despesas parlamentares e contratos locais",
                url=TSE_CONTAS_ELEITORAIS_URL,
            )
        )
    return details, risks, [person for person, _ in high_value_people[:8]]


def political_detail_money_and_records(details: list[dict[str, Any]], base_records: int = 0) -> tuple[Decimal, int]:
    total = Decimal("0")
    records = base_records
    for detail in details:
        if not isinstance(detail, dict):
            continue
        value = parse_decimal(detail.get("value"))
        if value:
            total += value
        matched_records = detail.get("matched_records")
        if isinstance(matched_records, int):
            records += max(matched_records, 0)
        elif str(matched_records or "").isdigit():
            records += int(str(matched_records))
        elif detail.get("type") in {"contratos", "doacoes", "processos", "controle", "contas", "vinculos"}:
            records += 1
    return total, records


async def fetch_deputy_expenses(deputy_id: Any, years: list[int] | None = None, limit_per_year: int = 80) -> list[dict[str, Any]]:
    years = years or [brasilia_today().year, brasilia_today().year - 1]
    expenses: list[dict[str, Any]] = []
    for year in years:
        try:
            data = await get_json(
                f"{CAMARA_DEPUTADOS_URL}/{deputy_id}/despesas",
                params={"ano": year, "itens": limit_per_year, "ordem": "DESC", "ordenarPor": "dataDocumento"},
            )
        except HTTPException:
            continue
        expenses.extend(response_items(data))
    return expenses


async def gather_limited(limit: int, coroutines: list[Any]) -> list[Any]:
    semaphore = asyncio.Semaphore(max(1, limit))

    async def run(coroutine: Any) -> Any:
        async with semaphore:
            return await coroutine

    return await asyncio.gather(*(run(coroutine) for coroutine in coroutines))


def expense_sources_for_deputy(deputy_id: Any) -> list[MonitoringSource]:
    return [
        MonitoringSource(
            label="Câmara dos Deputados - despesas parlamentares",
            url=f"{CAMARA_DEPUTADOS_URL}/{deputy_id}/despesas",
            kind="API oficial de despesas reembolsadas",
        ),
        MonitoringSource(
            label="Câmara dos Deputados - cadastro parlamentar",
            url=f"{CAMARA_DEPUTADOS_URL}/{deputy_id}",
            kind="API oficial de parlamentares",
        ),
    ]


def legal_public_sources_for_name(name: str) -> list[MonitoringSource]:
    clean = " ".join(str(name or "").split())
    encoded = urlencode({"termo": clean})
    return [
        MonitoringSource(
            label="STF - consulta processual pública",
            url=stf_process_link(clean),
            kind="Busca oficial por processos públicos; exige conferência humana do resultado",
        ),
        MonitoringSource(
            label="STF - jurisprudência",
            url=stf_jurisprudence_link(clean),
            kind="Busca oficial por decisões e acórdãos públicos",
        ),
        MonitoringSource(
            label="TCU - pesquisa de processos",
            url=f"{TCU_PROCESSOS_URL}?{encoded}",
            kind="Busca oficial de processos de controle externo",
        ),
        MonitoringSource(
            label="TSE - divulgação de candidaturas e contas eleitorais",
            url=TSE_CONTAS_ELEITORAIS_URL,
            kind="Portal oficial para conferir candidaturas, bens e contas eleitorais",
        ),
        MonitoringSource(
            label="Compras.gov.br Dados Abertos - contratos",
            url=COMPRAS_CONTRATOS_URL,
            kind="API oficial para cruzar contratos, fornecedores e órgãos públicos por termo",
        ),
        MonitoringSource(
            label="PNCP - contratos públicos",
            url=PNCP_CONTRATOS_URL,
            kind="API oficial nacional de contratações públicas",
        ),
    ]


def legal_attention_factor(name: str, role: str | None = None) -> PoliticalRiskFactor:
    return PoliticalRiskFactor(
        level="baixo",
        title="Checagens legais e de controle disponíveis",
        message=(
            "Há consultas oficiais para verificar processos públicos, decisões, contas eleitorais e controle externo. "
            "A plataforma lista as fontes; a conclusão depende da leitura do órgão competente."
        ),
        evidence={"person": name, "role": role, "legal_conclusion": "não inferida automaticamente"},
        source="STF, TCU e TSE",
        url=stf_process_link(name),
    )


def public_related_political_item(person: dict[str, Any]) -> PoliticalScanItem:
    title = str(person.get("title") or "Pessoa pública")
    role = str(person.get("role") or "Pessoa pública ou política relacionada")
    party = str(person.get("party") or "")
    related_people = [title, *[str(query) for query in person.get("related_queries", [])[:5]]]
    contract_details, contract_risks, contract_people = local_contract_crosscheck_for_political(
        title,
        role,
        party,
        related_people,
    )
    donation_details, donation_risks = electoral_donation_crosscheck_for_political(
        title,
        role,
        party,
        related_people,
    )
    sources = [
        *legal_public_sources_for_name(title),
        MonitoringSource(
            label="Fonte pública informativa relacionada",
            url=str(person.get("url") or "https://www.tse.jus.br/"),
            kind="Fonte oficial ou institucional para contexto",
        ),
    ]
    analysis_details = [
        {
            "type": "processos",
            "title": "Consultas legais oficiais",
            "description": "Links oficiais para conferencia humana de processos publicos, jurisprudencia, contas eleitorais e controle externo.",
            "person": title,
        },
        contract_crosscheck_detail(title, role, party),
        *contract_details,
        *donation_details,
    ]
    related_money, related_records = political_detail_money_and_records(analysis_details, base_records=len(sources))
    attention = political_attention_level(
        min(
            100,
            (45 if related_money >= POLITICAL_HIGH_VALUE_CONTRACT_THRESHOLD else 0)
            + (20 if contract_risks else 0)
            + (15 if donation_risks else 0),
        )
    )
    item = PoliticalScanItem(
        id=record_fingerprint([title, "related_public_person"]),
        type="politico",
        name=title,
        subtitle=str(person.get("subtitle") or "Pessoa pública relacionada"),
        party=party or None,
        role=role,
        total_public_money=related_money,
        travel_public_money=Decimal("0"),
        records_count=related_records,
        attention_level=attention,
        summary=(
            f"{money(related_money)} em contratos e pagamentos ligados a {title}; "
            f"{related_records} registros/fontes lidos, com foco em fornecedor, órgão, processo e conta eleitoral."
        ),
        people=list(dict.fromkeys([*related_people, *contract_people]))[:16],
        analysis_types=["processos", "contas", "controle", "contratos", "doacoes", "vinculos"],
        analysis_details=analysis_details,
        sources=sources,
        risks=[
            legal_attention_factor(title, role),
            *contract_risks,
            *donation_risks,
            PoliticalRiskFactor(
                level="baixo",
                title="Cruzamento por nomes relacionados",
                message="Termos relacionados são usados para buscar contratos, partidos, processos públicos e registros oficiais.",
                evidence={"related_queries": person.get("related_queries", [])},
                source="COIBE.IA - busca política relacionada",
                url=str(person.get("url") or "https://www.tse.jus.br/"),
            ),
        ],
    )
    return apply_political_priority(item)


def quick_public_related_political_item(person: dict[str, Any]) -> PoliticalScanItem:
    title = str(person.get("title") or "Pessoa pública")
    role = str(person.get("role") or "Pessoa pública ou política relacionada")
    party = str(person.get("party") or "")
    sources = [
        MonitoringSource(
            label="Fonte pública informativa relacionada",
            url=str(person.get("url") or "https://www.tse.jus.br/"),
            kind="Fonte oficial ou institucional para contexto inicial",
        ),
        *legal_public_sources_for_name(title),
    ]
    item = PoliticalScanItem(
        id=record_fingerprint([title, "quick_related_public_person"]),
        type="politico",
        name=title,
        subtitle=str(person.get("subtitle") or "Pessoa pública relacionada"),
        party=party or None,
        role=role,
        total_public_money=Decimal("0"),
        travel_public_money=Decimal("0"),
        records_count=len(sources),
        attention_level="baixo",
        summary=(
            f"Perfil público inicial de {title}. A análise detalhada de contratos, despesas, doações "
            "e vínculos fica em atualização no backend e será reaproveitada do cache."
        ),
        people=list(dict.fromkeys([title, *[str(query) for query in person.get("related_queries", [])[:5]]]))[:16],
        analysis_types=["processos", "controle", "contas", "contratos", "doacoes", "vinculos", "prioridade"],
        analysis_details=[
            {
                "type": "prioridade",
                "title": "Perfil carregado rapidamente",
                "description": "A busca retornou primeiro os dados públicos básicos para não travar a plataforma. O backend aprofunda a análise em segundo plano.",
                "person": title,
                "party": party,
            },
            {
                "type": "processos",
                "title": "Consultas legais oficiais",
                "description": "Links oficiais para conferência humana de processos públicos, jurisprudência, contas eleitorais e controle externo.",
                "person": title,
            },
        ],
        sources=sources,
        risks=[
            legal_attention_factor(title, role),
            PoliticalRiskFactor(
                level="baixo",
                title="Análise aprofundada em segundo plano",
                message="O resultado inicial evita espera longa; o monitor local completa cruzamentos e cacheia novas evidências.",
                evidence={"mode": "quick_public_profile", "party": party},
                source="COIBE.IA - busca política rápida",
                url=str(person.get("url") or "https://www.tse.jus.br/"),
            ),
        ],
    )
    return apply_political_priority(item)


def quick_political_item_from_deputy(deputy: dict[str, Any], current: bool = True) -> PoliticalScanItem:
    name = str(deputy.get("nome") or "Parlamentar")
    role = "Deputado federal" if current else "Ex-deputado federal"
    deputy_id = deputy.get("id")
    source_url = str(deputy.get("uri") or f"{CAMARA_DEPUTADOS_URL}/{deputy_id or ''}")
    sources = [
        MonitoringSource(
            label="Câmara dos Deputados - perfil público",
            url=source_url,
            kind="API oficial de parlamentares",
        ),
        *legal_public_sources_for_name(name),
    ]
    item = PoliticalScanItem(
        id=str(deputy_id or record_fingerprint([name, role])),
        type="politico",
        name=name,
        subtitle=f"{deputy.get('siglaPartido', '-')}/{deputy.get('siglaUf', '-')}",
        party=deputy.get("siglaPartido"),
        role=role,
        uf=deputy.get("siglaUf"),
        total_public_money=Decimal("0"),
        travel_public_money=Decimal("0"),
        records_count=len(sources),
        attention_level="baixo",
        summary=(
            f"Perfil público inicial de {name} ({role}). Despesas, contratos, doações e vínculos "
            "serão aprofundados pelo monitor em segundo plano e salvos no cache."
        ),
        people=[name, str(deputy.get("siglaPartido") or "")],
        analysis_types=["processos", "controle", "contas", "contratos", "doacoes", "vinculos", "prioridade"],
        analysis_details=[
            {
                "type": "prioridade",
                "title": "Perfil parlamentar carregado rapidamente",
                "description": "A busca usa a API pública pelo nome e retorna sem aguardar todas as despesas. A análise completa roda em segundo plano.",
                "person": name,
                "party": deputy.get("siglaPartido"),
            },
            {
                "type": "processos",
                "title": "Consultas legais oficiais",
                "description": "Fontes oficiais para conferência humana de processos, contas eleitorais e controle externo.",
                "person": name,
            },
        ],
        sources=sources,
        risks=[
            legal_attention_factor(name, role),
            PoliticalRiskFactor(
                level="baixo",
                title="Análise completa pendente no cache",
                message="O backend retornou o perfil rápido e continuará cruzando despesas, contratos, doações e vínculos.",
                evidence={"mode": "quick_deputy_profile", "party": deputy.get("siglaPartido"), "uf": deputy.get("siglaUf")},
                source="Câmara dos Deputados - Dados Abertos",
                url=source_url,
            ),
        ],
    )
    return apply_political_priority(item)


def quick_political_item_from_senator(senator: dict[str, Any]) -> PoliticalScanItem:
    ident = senator.get("IdentificacaoParlamentar", {})
    name = str(ident.get("NomeParlamentar") or ident.get("NomeCompletoParlamentar") or "Senador")
    party = ident.get("SiglaPartidoParlamentar")
    uf = ident.get("UfParlamentar") or senator.get("Mandato", {}).get("UfParlamentar")
    sources = [
        MonitoringSource(
            label="Senado Federal - parlamentares em exercício",
            url="https://legis.senado.leg.br/dadosabertos/senador/lista/atual",
            kind="API oficial de parlamentares",
        ),
        *legal_public_sources_for_name(name),
    ]
    item = PoliticalScanItem(
        id=str(ident.get("CodigoParlamentar") or record_fingerprint([name, party, uf])),
        type="politico",
        name=name,
        subtitle=f"{party or '-'}/{uf or '-'}",
        party=party,
        role="Senador",
        uf=uf,
        total_public_money=Decimal("0"),
        travel_public_money=Decimal("0"),
        records_count=len(sources),
        attention_level="baixo",
        summary=f"Perfil público inicial de {name}. O backend aprofunda contratos, despesas, doações e vínculos em segundo plano.",
        people=[name, str(party or "")],
        analysis_types=["processos", "controle", "contas", "contratos", "doacoes", "vinculos", "prioridade"],
        analysis_details=[
            {
                "type": "prioridade",
                "title": "Perfil do Senado carregado rapidamente",
                "description": "A busca retornou o cadastro público antes dos cruzamentos pesados para manter a plataforma responsiva.",
                "person": name,
                "party": party,
            },
        ],
        sources=sources,
        risks=[legal_attention_factor(name, "Senador")],
    )
    return apply_political_priority(item)


def political_item_from_deputy(deputy: dict[str, Any], expenses: list[dict[str, Any]], current: bool = True) -> PoliticalScanItem:
    total = sum((expense_value(expense) for expense in expenses), Decimal("0"))
    travel_total = sum((expense_value(expense) for expense in expenses if is_travel_expense(expense)), Decimal("0"))
    suppliers: dict[str, Decimal] = {}
    missing_docs = 0
    for expense in expenses:
        supplier = str(expense.get("nomeFornecedor") or "Fornecedor não informado")
        suppliers[supplier] = suppliers.get(supplier, Decimal("0")) + expense_value(expense)
        if not expense.get("urlDocumento"):
            missing_docs += 1

    top_supplier, top_supplier_value = max(suppliers.items(), key=lambda row: row[1], default=("", Decimal("0")))
    score = 0
    risks: list[PoliticalRiskFactor] = []

    if total >= Decimal("120000"):
        score += 35
        risks.append(
            PoliticalRiskFactor(
                level="médio",
                title="Gasto público alto no período",
                message="O valor reembolsado está alto para leitura humana e comparação com pares.",
                evidence={"total_public_money": str(total), "records_count": len(expenses)},
                source="Câmara dos Deputados - Dados Abertos",
                url=f"{CAMARA_DEPUTADOS_URL}/{deputy.get('id')}/despesas",
            )
        )
    if travel_total >= Decimal("40000"):
        score += 30
        risks.append(
            PoliticalRiskFactor(
                level="médio",
                title="Viagens e deslocamentos em destaque",
                message="Passagens, hospedagem, locomoção ou combustível aparecem com valor relevante.",
                evidence={"travel_public_money": str(travel_total)},
                source="Câmara dos Deputados - Dados Abertos",
                url=f"{CAMARA_DEPUTADOS_URL}/{deputy.get('id')}/despesas",
            )
        )
    if top_supplier_value >= Decimal("50000"):
        score += 25
        risks.append(
            PoliticalRiskFactor(
                level="médio",
                title="Pagamento concentrado em fornecedor",
                message="Um fornecedor aparece com parcela importante dos pagamentos públicos analisados.",
                evidence={"supplier": top_supplier, "supplier_total": str(top_supplier_value)},
                source="Câmara dos Deputados - Dados Abertos",
                url=f"{CAMARA_DEPUTADOS_URL}/{deputy.get('id')}/despesas",
            )
        )
    if missing_docs >= 8:
        score += 15
        risks.append(
            PoliticalRiskFactor(
                level="baixo",
                title="Documentos para conferir",
                message="Alguns registros não trouxeram link de documento na resposta consultada.",
                evidence={"records_without_document_url": missing_docs},
                source="Câmara dos Deputados - Dados Abertos",
                url=f"{CAMARA_DEPUTADOS_URL}/{deputy.get('id')}/despesas",
            )
        )

    risks.append(legal_attention_factor(str(deputy.get("nome") or "Parlamentar"), "Deputado federal" if current else "Ex-deputado federal"))

    if len(risks) == 1:
        risks.append(
            PoliticalRiskFactor(
                level="baixo",
                title="Sem atenção forte no recorte",
                message="Os registros públicos consultados não mostraram concentração forte neste recorte.",
                evidence={"records_count": len(expenses), "total_public_money": str(total)},
                source="Câmara dos Deputados - Dados Abertos",
                url=f"{CAMARA_DEPUTADOS_URL}/{deputy.get('id')}/despesas",
            )
        )

    level = political_attention_level(score)
    people = [str(deputy.get("nome") or "Parlamentar")]
    people.extend([name for name, _ in sorted(suppliers.items(), key=lambda row: row[1], reverse=True)[:4] if name])
    analysis_types = political_analysis_types_from_expenses(expenses)
    analysis_types.extend(["processos", "controle", "contas", "contratos", "doacoes", "vinculos"])
    analysis_details = expense_analysis_details(expenses)
    analysis_details.append(
        contract_crosscheck_detail(
            str(deputy.get("nome") or "Parlamentar"),
            "Deputado federal" if current else "Ex-deputado federal",
            str(deputy.get("siglaPartido") or ""),
        )
    )
    role = "Deputado federal" if current else "Ex-deputado federal"
    contract_details, contract_risks, contract_people = local_contract_crosscheck_for_political(
        str(deputy.get("nome") or "Parlamentar"),
        role,
        str(deputy.get("siglaPartido") or ""),
        people,
    )
    donation_details, donation_risks = electoral_donation_crosscheck_for_political(
        str(deputy.get("nome") or "Parlamentar"),
        role,
        str(deputy.get("siglaPartido") or ""),
        people,
    )
    proximity_details, proximity_risks, proximity_people = proximity_money_flow_attention(
        str(deputy.get("nome") or "Parlamentar"),
        suppliers,
        role,
        str(deputy.get("siglaPartido") or ""),
    )
    analysis_details.extend(contract_details)
    analysis_details.extend(donation_details)
    analysis_details.extend(proximity_details)
    risks.extend(contract_risks)
    risks.extend(donation_risks)
    risks.extend(proximity_risks)
    people.extend(contract_people)
    people.extend(proximity_people)
    if contract_risks or donation_risks or proximity_risks:
        risks = [risk for risk in risks if risk.title != "Sem atenção forte no recorte"]
        score += 15
        level = political_attention_level(score)

    return apply_political_priority(PoliticalScanItem(
        id=str(deputy.get("id")),
        type="politico",
        name=str(deputy.get("nome") or "Parlamentar"),
        subtitle=f"{deputy.get('siglaPartido', '-')}/{deputy.get('siglaUf', '-')}",
        party=deputy.get("siglaPartido"),
        role="Deputado federal" if current else "Ex-deputado federal",
        uf=deputy.get("siglaUf"),
        total_public_money=total,
        travel_public_money=travel_total,
        records_count=len(expenses),
        attention_level=level,
        summary=f"{money(total)} em despesas públicas no recorte; {money(travel_total)} ligados a viagens ou deslocamentos. {'Mandato atual.' if current else 'Fora do exercício atual neste recorte.'}",
        people=list(dict.fromkeys([person for person in people if person]))[:16],
        analysis_types=sorted(set(analysis_types)),
        analysis_details=analysis_details,
        sources=[*expense_sources_for_deputy(deputy.get("id")), *legal_public_sources_for_name(str(deputy.get("nome") or ""))],
        risks=risks,
    ))


async def fetch_current_deputies(limit: int = 24, party: str | None = None, legislature: int | None = None, page: int = 1) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"itens": min(max(limit, 1), 100), "ordem": "ASC", "ordenarPor": "nome"}
    params["pagina"] = max(int(page or 1), 1)
    if party:
        params["siglaPartido"] = party
    if legislature:
        params["idLegislatura"] = legislature
    try:
        data = await get_json(CAMARA_DEPUTADOS_URL, params=params)
    except HTTPException:
        return []
    return response_items(data)


async def fetch_deputies_by_name(
    name: str,
    limit: int = 24,
    party: str | None = None,
    legislature: int | None = None,
) -> list[dict[str, Any]]:
    clean_name = str(name or "").strip()
    if len(clean_name) < 2:
        return []
    params: dict[str, Any] = {
        "nome": clean_name,
        "itens": min(max(limit, 1), 100),
        "ordem": "ASC",
        "ordenarPor": "nome",
    }
    if party:
        params["siglaPartido"] = party
    if legislature:
        params["idLegislatura"] = legislature
    try:
        data = await get_json(CAMARA_DEPUTADOS_URL, params=params)
    except HTTPException:
        return []
    return response_items(data)


async def fetch_former_deputies(limit: int = 12, party: str | None = None, page_offset: int = 0) -> list[dict[str, Any]]:
    current_year = brasilia_today().year
    legislature_hints = [56, 55, 54, 53] if current_year >= 2023 else [55, 54, 53]
    collected: dict[str, dict[str, Any]] = {}
    page = max(int(page_offset or 0), 0) + 1
    for legislature in legislature_hints:
        for deputy in await fetch_current_deputies(limit=max(limit, 20), party=party, legislature=legislature, page=page):
            key = str(deputy.get("id") or deputy.get("uri") or deputy.get("nome"))
            if key:
                collected[key] = deputy
            if len(collected) >= limit:
                break
        if len(collected) >= limit:
            break
    return list(collected.values())[:limit]


async def political_people_scan(limit: int = 18, q: str | None = None, party: str | None = None, offset: int = 0) -> list[PoliticalScanItem]:
    current_target = max(4, int(limit * 0.6))
    former_target = max(3, limit - current_target)
    normalized_query = normalize_text(q)
    if normalized_query:
        deputies = await fetch_deputies_by_name(str(q), limit=max(current_target * 3, 20), party=party)
    else:
        page_size = max(current_target * 2, 20)
        page = max(int(offset or 0), 0) // page_size + 1
        deputies = await fetch_current_deputies(limit=page_size, party=party, page=page)
    if normalized_query:
        deputies = [
            deputy for deputy in deputies
            if normalized_query in normalize_text(" ".join(str(deputy.get(key) or "") for key in ("nome", "siglaPartido", "siglaUf")))
        ]
    deputies = deputies[:current_target]
    if normalized_query:
        former_by_key: dict[str, dict[str, Any]] = {}
        legislature_hints = [56, 55, 54, 53]
        for legislature in legislature_hints:
            for deputy in await fetch_deputies_by_name(str(q), limit=max(former_target * 2, 12), party=party, legislature=legislature):
                key = str(deputy.get("id") or deputy.get("uri") or deputy.get("nome"))
                if key:
                    former_by_key[key] = deputy
            if len(former_by_key) >= former_target * 2:
                break
        former_deputies = list(former_by_key.values())
    else:
        former_deputies = await fetch_former_deputies(
            limit=former_target * 2,
            party=party,
            page_offset=max(int(offset or 0), 0) // max(former_target * 2, 20),
        )
    if normalized_query:
        former_deputies = [
            deputy for deputy in former_deputies
            if normalized_query in normalize_text(" ".join(str(deputy.get(key) or "") for key in ("nome", "siglaPartido", "siglaUf")))
        ]
    current_ids = {str(deputy.get("id")) for deputy in deputies}
    former_deputies = [deputy for deputy in former_deputies if str(deputy.get("id")) not in current_ids][:former_target]
    expense_batches = await gather_limited(4, [fetch_deputy_expenses(deputy.get("id")) for deputy in deputies])
    former_expense_batches = await gather_limited(3, [fetch_deputy_expenses(deputy.get("id")) for deputy in former_deputies])
    items = [political_item_from_deputy(deputy, expenses, current=True) for deputy, expenses in zip(deputies, expense_batches)]
    items.extend(political_item_from_deputy(deputy, expenses, current=False) for deputy, expenses in zip(former_deputies, former_expense_batches))

    try:
        senators = await fetch_current_senators()
    except HTTPException:
        senators = []
    for senator in senators[: max(0, min(6, limit - len(items)))]:
        ident = senator.get("IdentificacaoParlamentar", {})
        name = ident.get("NomeParlamentar") or ident.get("NomeCompletoParlamentar") or "Senador"
        senator_party = ident.get("SiglaPartidoParlamentar")
        uf = ident.get("UfParlamentar") or senator.get("Mandato", {}).get("UfParlamentar")
        if normalized_query and normalized_query not in normalize_text(f"{name} {senator_party} {uf}"):
            continue
        if party and normalize_text(party) != normalize_text(senator_party):
            continue
        senator_people = [str(name), str(senator_party or "")]
        contract_details, contract_risks, contract_people = local_contract_crosscheck_for_political(
            str(name),
            "Senador",
            str(senator_party or ""),
            senator_people,
        )
        donation_details, donation_risks = electoral_donation_crosscheck_for_political(
            str(name),
            "Senador",
            str(senator_party or ""),
            senator_people,
        )
        senator_analysis_details = [
            {
                "type": "processos",
                "title": "Consultas legais oficiais",
                "description": "Cadastro de parlamentar e links oficiais para conferencia humana de processos publicos, jurisprudencia e contas eleitorais.",
                "person": name,
            },
            contract_crosscheck_detail(str(name), "Senador", str(senator_party or "")),
            *contract_details,
            *donation_details,
        ]
        senator_money, senator_records = political_detail_money_and_records(
            senator_analysis_details,
            base_records=1 + len(legal_public_sources_for_name(str(name))),
        )
        senator_attention = political_attention_level(
            min(100, (35 if senator_money >= POLITICAL_HIGH_VALUE_CONTRACT_THRESHOLD else 0) + (20 if contract_risks else 0) + (15 if donation_risks else 0))
        )
        items.append(
            apply_political_priority(PoliticalScanItem(
                id=str(ident.get("CodigoParlamentar") or name),
                type="politico",
                name=str(name),
                subtitle=f"{senator_party or '-'}/{uf or '-'}",
                party=senator_party,
                role="Senador",
                uf=uf,
                total_public_money=senator_money,
                travel_public_money=Decimal("0"),
                records_count=senator_records,
                attention_level=senator_attention,
                summary=(
                    f"{money(senator_money)} em contratos e pagamentos ligados a {name}; "
                    f"{senator_records} registros/fontes lidos, com foco em fornecedor, órgão, processo e conta eleitoral."
                ),
                people=list(dict.fromkeys([str(name), *contract_people]))[:16],
                analysis_types=["processos", "controle", "contas", "contratos", "doacoes", "vinculos"],
                analysis_details=senator_analysis_details,
                sources=[
                    MonitoringSource(
                        label="Senado Federal - parlamentares em exercício",
                        url="https://legis.senado.leg.br/dadosabertos/senador/lista/atual",
                        kind="API oficial de parlamentares",
                    ),
                    *legal_public_sources_for_name(str(name)),
                ],
                risks=[
                    legal_attention_factor(str(name), "Senador"),
                    *contract_risks,
                    *donation_risks,
                    PoliticalRiskFactor(
                        level="baixo",
                        title="Cadastro público monitorado",
                        message="Registro mantido para cruzamento com contratos, pessoas, partidos e fontes externas.",
                        evidence={"party": senator_party, "uf": uf},
                        source="Senado Federal Dados Abertos",
                        url="https://legis.senado.leg.br/dadosabertos/senador/lista/atual",
                    )
                ],
            ))
        )

    if not party:
        related_people = POLITICAL_RELATED_PEOPLE if normalized_query or max(int(offset or 0), 0) == 0 else []
        for person in related_people:
            if normalized_query and normalized_query not in normalize_text(f"{person.get('title')} {' '.join(person.get('aliases', []))}"):
                continue
            items.append(public_related_political_item(person))

    items.sort(key=political_sort_key, reverse=True)
    return items[:limit]


async def political_people_quick_scan(limit: int = 18, q: str | None = None, party: str | None = None, offset: int = 0) -> list[PoliticalScanItem]:
    normalized_query = normalize_text(q)
    current_limit = min(max(limit, 1), 12)
    if normalized_query:
        deputies = await fetch_deputies_by_name(str(q), limit=current_limit, party=party)
    else:
        page_size = max(current_limit, 12)
        page = max(int(offset or 0), 0) // page_size + 1
        deputies = await fetch_current_deputies(limit=page_size, party=party, page=page)

    items = [quick_political_item_from_deputy(deputy, current=True) for deputy in deputies[:current_limit]]

    if not normalized_query or not items:
        try:
            senators = await fetch_current_senators()
        except HTTPException:
            senators = []
        for senator in senators:
            if len(items) >= limit:
                break
            ident = senator.get("IdentificacaoParlamentar", {})
            name = ident.get("NomeParlamentar") or ident.get("NomeCompletoParlamentar") or ""
            senator_party = ident.get("SiglaPartidoParlamentar")
            uf = ident.get("UfParlamentar") or senator.get("Mandato", {}).get("UfParlamentar")
            searchable = normalize_text(f"{name} {senator_party} {uf}")
            if normalized_query and normalized_query not in searchable:
                continue
            if party and normalize_text(party) != normalize_text(senator_party):
                continue
            items.append(quick_political_item_from_senator(senator))

    if not party:
        for person in POLITICAL_RELATED_PEOPLE:
            if len(items) >= limit:
                break
            searchable = normalize_text(f"{person.get('title')} {' '.join(person.get('aliases', []))}")
            if normalized_query and normalized_query not in searchable:
                continue
            items.append(quick_public_related_political_item(person))

    by_key: dict[str, PoliticalScanItem] = {}
    for item in items:
        key = f"{item.type}:{item.id or normalize_text(item.name)}"
        by_key[key] = item
    ordered = list(by_key.values())
    ordered.sort(key=political_sort_key, reverse=True)
    return ordered[:limit]


async def refresh_political_people_cache_background(q: str | None, party: str | None, limit: int, offset: int) -> None:
    try:
        live_items = await political_people_scan(limit=limit, q=q, party=party, offset=offset)
        save_political_scan_public_records("political_people", q, live_items)
    except Exception:
        return


def schedule_political_people_cache_refresh(q: str | None, party: str | None, limit: int, offset: int) -> None:
    key = record_fingerprint(["political_people_refresh", q, party, limit, offset])
    if key in POLITICAL_BACKGROUND_REFRESHES:
        return

    async def runner() -> None:
        try:
            await refresh_political_people_cache_background(q, party, limit, offset)
        finally:
            POLITICAL_BACKGROUND_REFRESHES.discard(key)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    POLITICAL_BACKGROUND_REFRESHES.add(key)
    loop.create_task(runner())


def provisional_political_item(q: str, party: str | None = None) -> PoliticalScanItem:
    name = str(q or "").strip()
    safe_name = name if name else "Pessoa pública pesquisada"
    generated_at = brasilia_now()
    return apply_political_priority(PoliticalScanItem(
        id=f"pending:{record_fingerprint(['political_people_pending', safe_name, party])}",
        type="politico",
        name=safe_name,
        subtitle="Busca pública em atualização",
        party=party,
        role="Pessoa pública pesquisada",
        attention_level="indeterminado",
        summary=(
            f"A plataforma iniciou a atualização pública para {safe_name}. "
            "O resultado inicial evita travar a aba enquanto o backend cruza Câmara, Senado, despesas, contratos e fontes oficiais."
        ),
        people=[safe_name],
        analysis_types=["prioridade", "contratos", "despesas", "processos"],
        analysis_details=[
            {
                "type": "prioridade",
                "title": "Atualização em segundo plano",
                "description": "Cache não encontrado; o backend está aprofundando a análise pública sem bloquear a plataforma.",
                "person": safe_name,
                "party": party,
                "source": "COIBE.IA",
            }
        ],
        analyzed_at=generated_at,
        sources=[
            MonitoringSource(
                label="COIBE.IA - fila de atualização política",
                url="/api/political/politicians",
                kind="Análise local em segundo plano",
            )
        ],
        risks=[
            PoliticalRiskFactor(
                level="baixo",
                title="Análise pendente",
                message="Registro temporário de atualização; aguarde o cache completo antes de concluir qualquer leitura.",
                evidence={"query": safe_name, "party": party},
                source="COIBE.IA",
            )
        ],
    ))


async def political_parties_scan(limit: int = 16, q: str | None = None) -> list[PoliticalScanItem]:
    try:
        parties_data = await get_json(CAMARA_PARTIDOS_URL, params={"itens": 40, "ordem": "ASC", "ordenarPor": "sigla"})
    except HTTPException:
        return []
    normalized_query = normalize_text(q)
    parties = response_items(parties_data)
    if normalized_query:
        parties = [
            party for party in parties
            if normalized_query in normalize_text(f"{party.get('sigla')} {party.get('nome')}")
        ]
    parties.sort(
        key=lambda party: (
            POLITICAL_PRIORITY_PARTIES.get(normalize_text(party.get("sigla")), 35),
            normalize_text(party.get("sigla")),
        ),
        reverse=True,
    )
    parties = parties[:limit]

    async def build_party_item(party: dict[str, Any]) -> PoliticalScanItem:
        sigla = str(party.get("sigla") or "")
        members = await fetch_current_deputies(limit=10, party=sigla)
        expense_batches = await gather_limited(3, [fetch_deputy_expenses(member.get("id"), limit_per_year=35) for member in members[:8]])
        member_items = [political_item_from_deputy(member, expenses) for member, expenses in zip(members[:8], expense_batches)]
        total = sum((item.total_public_money for item in member_items), Decimal("0"))
        travel_total = sum((item.travel_public_money for item in member_items), Decimal("0"))
        records_count = sum(item.records_count for item in member_items)
        high_or_medium = [item for item in member_items if item.attention_level in {"alto", "médio"}]
        score = min(100, len(high_or_medium) * 18 + (35 if total >= Decimal("350000") else 0) + (25 if travel_total >= Decimal("90000") else 0))
        level = political_attention_level(score)
        risks = [
            PoliticalRiskFactor(
                level=level,
                title="Leitura agregada de despesas do partido",
                message="Soma de despesas públicas dos parlamentares encontrados no recorte automático.",
                evidence={
                    "members_checked": len(member_items),
                    "attention_members": len(high_or_medium),
                    "total_public_money": str(total),
                    "travel_public_money": str(travel_total),
                    "records_count": records_count,
                },
                source="Câmara dos Deputados - Dados Abertos",
                url=f"{CAMARA_PARTIDOS_URL}/{party.get('id')}/membros" if party.get("id") else CAMARA_PARTIDOS_URL,
            )
        ]
        people = [item.name for item in member_items[:6]]
        for item in member_items[:3]:
            people.extend(item.people[1:3])
        contract_details, contract_risks, contract_people = local_contract_crosscheck_for_political(
            sigla,
            "Partido político",
            sigla,
            people,
        )
        donation_details, donation_risks = electoral_donation_crosscheck_for_political(
            sigla,
            "Partido político",
            sigla,
            people,
        )
        member_money: dict[str, Decimal] = {}
        for item in member_items:
            for person in item.people[:8]:
                member_money[person] = max(member_money.get(person, Decimal("0")), item.total_public_money)
        proximity_details, proximity_risks, proximity_people = proximity_money_flow_attention(
            sigla,
            member_money,
            "Partido político",
            sigla,
        )
        risks.extend(contract_risks)
        risks.extend(donation_risks)
        risks.extend(proximity_risks)
        if contract_risks or donation_risks or proximity_risks:
            score = min(100, score + 15)
            level = political_attention_level(score)
            risks[0].level = level
        people.extend(contract_people)
        people.extend(proximity_people)
        analysis_types = sorted(set(["partido", "despesas", "viagem", "processos", "contas", "controle", "contratos", "doacoes", "vinculos"] + [analysis_type for item in member_items for analysis_type in item.analysis_types]))
        analysis_details = [
            {
                **contract_crosscheck_detail(sigla, "Partido político", sigla),
                "title": f"Contratos e despesas agregados do {sigla}",
                "description": (
                    f"O COIBE juntou parlamentares, fornecedores citados e contratos ligados ao {sigla}. "
                    "Veja abaixo valores, fornecedores, órgãos e termos que geraram cada correspondência."
                ),
            }
        ]
        for item in member_items[:4]:
            for detail in item.analysis_details[:2]:
                analysis_details.append({**detail, "person": item.name, "party": sigla})
        analysis_details.extend(contract_details)
        analysis_details.extend(donation_details)
        analysis_details.extend(proximity_details)
        return apply_political_priority(PoliticalScanItem(
            id=str(party.get("id") or sigla),
            type="partido",
            name=f"{sigla} - {party.get('nome')}".strip(" -"),
            subtitle=f"{len(member_items)} parlamentar(es) analisado(s) no recorte",
            party=sigla,
            role="Partido político",
            total_public_money=total,
            travel_public_money=travel_total,
            records_count=records_count,
            attention_level=level,
            summary=f"{money(total)} em despesas dos parlamentares analisados; {money(travel_total)} em viagens/deslocamentos.",
            people=list(dict.fromkeys([person for person in people if person]))[:16],
            analysis_types=analysis_types,
            analysis_details=analysis_details[:30],
            sources=[
                MonitoringSource(
                    label="Câmara dos Deputados - partidos e membros",
                    url=f"{CAMARA_PARTIDOS_URL}/{party.get('id')}/membros" if party.get("id") else CAMARA_PARTIDOS_URL,
                    kind="API oficial de partidos",
                ),
                MonitoringSource(
                    label="TSE - partidos políticos",
                    url=TSE_PARTIDOS_URL,
                    kind="Portal oficial eleitoral",
                ),
                MonitoringSource(
                    label="TSE - divulgação de contas eleitorais",
                    url=TSE_CONTAS_ELEITORAIS_URL,
                    kind="Portal oficial de contas eleitorais",
                ),
                MonitoringSource(
                    label="TCU - pesquisa de processos por partido ou pessoa",
                    url=f"{TCU_PROCESSOS_URL}?{urlencode({'termo': sigla})}",
                    kind="Consulta oficial de controle externo",
                ),
            ],
            risks=risks,
        ))

    items = await gather_limited(3, [build_party_item(party) for party in parties])
    items.sort(key=political_sort_key, reverse=True)
    return list(items)[:limit]


POLITICAL_PARTY_SOURCES = [
    "Câmara dos Deputados Dados Abertos",
    "TSE - Partidos Políticos",
    "TSE Divulgação de Candidaturas e Contas",
    "TCU Pesquisa de Processos",
    "COIBE.IA - cruzamento preventivo de despesas públicas",
]

POLITICAL_PEOPLE_SOURCES = [
    "Câmara dos Deputados Dados Abertos",
    "Senado Federal Dados Abertos",
    "STF Consulta Processual/Jurisprudência",
    "TCU Pesquisa de Processos",
    "TSE Divulgação de Candidaturas e Contas",
    "COIBE.IA - cruzamento preventivo de despesas públicas",
]


def political_item_cache_quality(item: PoliticalScanItem) -> tuple[int, Decimal, int, int]:
    detail_count = len(item.analysis_details or [])
    people_count = len(item.people or [])
    return (
        int(item.records_count or 0),
        item.total_public_money or Decimal("0"),
        detail_count,
        people_count,
    )


def normalize_cached_political_detail(detail: dict[str, Any], fallback_party: str | None = None, fallback_person: str | None = None) -> dict[str, Any]:
    if not isinstance(detail, dict):
        return detail
    output = dict(detail)
    detail_type = str(output.get("type") or "")
    title = str(output.get("title") or "")
    person = output.get("person") or fallback_person or "recorte"
    party = output.get("party") or fallback_party
    if detail_type == "contratos":
        value = parse_decimal(output.get("value"))
        supplier = output.get("supplier") or "fornecedor não informado"
        entity = output.get("entity") or "órgão não informado"
        matched_terms = output.get("matched_terms") if isinstance(output.get("matched_terms"), list) else []
        terms_text = ", ".join(str(term) for term in matched_terms[:4] if term) or str(party or person)
        if title in {
            "Contrato relacionado encontrado na base local",
            "Cruzamento com contratos e compras públicas",
            "Cruzamento agregado do partido com contratos e compras públicas",
        }:
            output["title"] = (
                f"{money(value)} em contrato com {supplier}"
                if value and value > 0
                else f"Busca de contratos para {party or person}"
            )
        output["description"] = (
            f"Termo(s) que ligaram ao recorte: {terms_text}. Órgão: {entity}. "
            f"Fornecedor: {supplier}. Confira objeto, valor, data e fonte oficial."
        )
    elif detail_type == "doacoes":
        matched_records = int(output.get("matched_records") or 0)
        if title in {"Doações e contas eleitorais a conferir", "Registro eleitoral relacionado"}:
            output["title"] = f"{matched_records} registro(s) eleitoral(is) ligado(s) a {party or person}"
        output["description"] = (
            f"Confira no TSE se o registro pertence a {party or person}; valide doador, recebedor, valor, data e campanha."
        )
    elif detail_type == "vinculos" and title == "Movimento de alto valor em pessoas ou fornecedores próximos ao recorte":
        output["title"] = "Valores concentrados em nomes do recorte"
        output["description"] = "O COIBE somou valores por nomes ligados ao recorte; proximidade é textual/operacional, não parentesco confirmado."
    return output


def normalize_cached_political_risk(risk: PoliticalRiskFactor) -> PoliticalRiskFactor:
    if risk.title == "Contratos relacionados para conferência":
        matches = risk.evidence.get("matches_count")
        total = parse_decimal(risk.evidence.get("total_related_value"))
        high = risk.evidence.get("high_value_or_high_risk_matches")
        risk.title = f"{matches or 0} contrato(s) ligados ao recorte"
        risk.message = (
            f"Valor somado: {money(total)}. {high or 0} contrato(s) tem valor alto ou risco alto. "
            "Confira fornecedor, órgão, objeto e documento oficial."
        )
    elif risk.title == "Possível relação eleitoral para conferência":
        matches = risk.evidence.get("matching_records")
        terms = risk.evidence.get("terms_checked") if isinstance(risk.evidence.get("terms_checked"), list) else []
        risk.title = f"{matches or 0} registro(s) eleitoral(is) para checar"
        risk.message = (
            f"Termos que puxaram a correspondência: {', '.join(str(term) for term in terms[:6])}. "
            "Valide no TSE se é a mesma pessoa, partido, fornecedor ou campanha."
        )
    elif risk.title == "Alto valor concentrado em pessoas/fornecedores do recorte":
        total = parse_decimal(risk.evidence.get("total_high_value_money"))
        count = risk.evidence.get("total_high_value_people")
        risk.title = f"{money(total)} concentrados em nomes do recorte"
        risk.message = f"{count or 0} nome(s) passaram do limite de alto valor. Confira documentos e repetição."
    return risk


def political_detail_key(detail: dict[str, Any]) -> str:
    if detail.get("document_url"):
        return f"doc:{detail.get('document_url')}"
    return "|".join(
        normalize_text(detail.get(key))
        for key in ("type", "title", "date", "supplier", "person", "party", "value", "source")
    )


def political_source_key(source: MonitoringSource) -> str:
    return f"{source.label}|{source.url}|{source.kind}"


def political_risk_key(risk: PoliticalRiskFactor) -> str:
    return f"{risk.title}|{risk.source}|{risk.url}|{json.dumps(risk.evidence, sort_keys=True, default=str)}"


def merge_cached_political_item(existing: PoliticalScanItem, incoming: PoliticalScanItem) -> PoliticalScanItem:
    richer = incoming if political_item_cache_quality(incoming) > political_item_cache_quality(existing) else existing
    existing.name = richer.name or existing.name
    existing.subtitle = richer.subtitle or existing.subtitle
    existing.party = richer.party or existing.party
    existing.role = richer.role or existing.role
    existing.uf = richer.uf or existing.uf

    existing.total_public_money = max(existing.total_public_money or Decimal("0"), incoming.total_public_money or Decimal("0"))
    existing.travel_public_money = max(existing.travel_public_money or Decimal("0"), incoming.travel_public_money or Decimal("0"))

    detail_by_key: dict[str, dict[str, Any]] = {}
    for detail in [*(existing.analysis_details or []), *(incoming.analysis_details or [])]:
        if not isinstance(detail, dict):
            continue
        key = political_detail_key(detail)
        if key and key not in detail_by_key:
            detail_by_key[key] = detail
    existing.analysis_details = list(detail_by_key.values())[:60]

    existing.records_count = max(
        int(existing.records_count or 0),
        int(incoming.records_count or 0),
        len(existing.analysis_details or []),
    )
    existing.people = list(dict.fromkeys([*existing.people, *incoming.people]))[:24]
    existing.analysis_types = sorted(set([*existing.analysis_types, *incoming.analysis_types]))

    source_by_key: dict[str, MonitoringSource] = {}
    for source in [*existing.sources, *incoming.sources]:
        key = political_source_key(source)
        if key not in source_by_key:
            source_by_key[key] = source
    existing.sources = list(source_by_key.values())[:16]

    risk_by_key: dict[str, PoliticalRiskFactor] = {}
    for risk in [*existing.risks, *incoming.risks]:
        key = political_risk_key(risk)
        if key not in risk_by_key:
            risk_by_key[key] = risk
    existing.risks = list(risk_by_key.values())[:16]

    rank = {"alto": 3, "médio": 2, "medio": 2, "baixo": 1}
    existing_level = rank.get(normalize_text(existing.attention_level).lower(), 0)
    incoming_level = rank.get(normalize_text(incoming.attention_level).lower(), 0)
    if incoming_level > existing_level:
        existing.attention_level = incoming.attention_level

    if existing.type == "partido":
        existing.summary = (
            f"{money(existing.total_public_money)} em despesas dos parlamentares analisados; "
            f"{money(existing.travel_public_money)} em viagens/deslocamentos. "
            f"{existing.records_count} registro(s) preservado(s) no acumulado."
        )
    return existing


def cached_parliamentary_people_for_party(records: list[dict[str, Any]], party: str | None) -> list[PoliticalScanItem]:
    normalized_party = normalize_text(party)
    if not normalized_party:
        return []
    people_by_key: dict[str, PoliticalScanItem] = {}
    for record in records:
        if record.get("record_type") != "political_people" or not isinstance(record.get("payload"), dict):
            continue
        try:
            person = PoliticalScanItem.model_validate(record["payload"])
        except Exception:
            continue
        if normalize_text(person.party) != normalized_party:
            continue
        role_text = normalize_text(f"{person.role} {person.subtitle}")
        if not any(term in role_text for term in ("DEPUTADO", "SENADOR", "PARLAMENTAR")):
            continue
        key = f"{person.type}:{person.id or normalize_text(person.name)}"
        existing = people_by_key.get(key)
        if existing is None:
            people_by_key[key] = person
        else:
            people_by_key[key] = merge_cached_political_item(existing, person)
    return list(people_by_key.values())


def enrich_party_from_cached_people(item: PoliticalScanItem, records: list[dict[str, Any]]) -> PoliticalScanItem:
    if item.type != "partido":
        return item
    people_items = cached_parliamentary_people_for_party(records, item.party)
    if not people_items:
        return item

    people_total = sum((person.total_public_money or Decimal("0") for person in people_items), Decimal("0"))
    travel_total = sum((person.travel_public_money or Decimal("0") for person in people_items), Decimal("0"))
    people_records = sum(int(person.records_count or 0) for person in people_items)

    if people_total > (item.total_public_money or Decimal("0")):
        item.total_public_money = people_total
    if travel_total > (item.travel_public_money or Decimal("0")):
        item.travel_public_money = travel_total
    if people_records > int(item.records_count or 0):
        item.records_count = people_records

    item.people = list(dict.fromkeys([*item.people, *[person.name for person in people_items if person.name]]))[:24]
    item.analysis_types = sorted(set([*item.analysis_types, *[kind for person in people_items for kind in person.analysis_types]]))

    detail_by_key = {
        political_detail_key(detail): detail
        for detail in item.analysis_details
        if isinstance(detail, dict)
    }
    for person in people_items:
        for detail in (person.analysis_details or [])[:6]:
            if not isinstance(detail, dict):
                continue
            enriched_detail = {**detail, "person": detail.get("person") or person.name, "party": item.party}
            key = political_detail_key(enriched_detail)
            if key and key not in detail_by_key:
                detail_by_key[key] = enriched_detail
    item.analysis_details = list(detail_by_key.values())[:60]

    attention_members = sum(1 for person in people_items if political_attention_rank(person.attention_level) >= 2)
    aggregate_level = "médio" if item.total_public_money >= Decimal("350000") or attention_members > 0 else "baixo"
    for risk in item.risks:
        if risk.title == "Leitura agregada de despesas do partido":
            risk.level = aggregate_level
            risk.evidence = {
                **risk.evidence,
                "members_checked": len(people_items),
                "attention_members": attention_members,
                "total_public_money": str(item.total_public_money),
                "travel_public_money": str(item.travel_public_money),
                "records_count": item.records_count,
            }
            risk.message = "Soma acumulada das despesas e sinais dos parlamentares encontrados no cache e nas coletas recentes."
            break

    item.summary = (
        f"{money(item.total_public_money)} em despesas dos parlamentares analisados; "
        f"{money(item.travel_public_money)} em viagens/deslocamentos. "
        f"{item.records_count} registro(s) no acumulado."
    )
    return item


def cached_political_items(
    record_type: str,
    q: str | None = None,
    party: str | None = None,
    risk_level: str | None = None,
    analysis_type: str | None = None,
    size_order: str | None = None,
    limit: int = 24,
) -> tuple[list[PoliticalScanItem], datetime | None]:
    all_records = load_public_records()
    records = sorted(
        (
            record
            for record in all_records
            if record.get("record_type") == record_type and isinstance(record.get("payload"), dict)
        ),
        key=lambda record: str(record.get("collected_at") or ""),
        reverse=True,
    )
    normalized_query = normalize_text(q)
    normalized_party = normalize_text(party)
    normalized_risk = normalize_text(risk_level).lower()
    normalized_analysis_type = normalize_text(analysis_type).lower()
    by_key: dict[str, PoliticalScanItem] = {}
    latest_collected_at: datetime | None = None

    for record in records:
        try:
            item = PoliticalScanItem.model_validate(record["payload"])
        except Exception:
            continue
        item.analysis_details = [
            normalize_cached_political_detail(detail, item.party, item.name)
            for detail in item.analysis_details
            if isinstance(detail, dict)
        ]
        item.risks = [normalize_cached_political_risk(risk) for risk in item.risks]

        if normalized_party and normalize_text(item.party) != normalized_party:
            continue

        if normalized_risk and normalized_risk != "todos" and normalize_text(item.attention_level) != normalized_risk:
            continue

        if normalized_analysis_type and normalized_analysis_type != "todos":
            priority_score, _ = political_priority_for(item.name, item.role, item.party, item.type)
            item_types = {normalize_text(value).lower() for value in item.analysis_types}
            detail_types = {
                normalize_text(detail.get("type")).lower()
                for detail in item.analysis_details
                if isinstance(detail, dict)
            }
            if normalized_analysis_type == "prioridade" and priority_score > 0:
                pass
            elif normalized_analysis_type not in item_types and normalized_analysis_type not in detail_types:
                continue

        detail_text = " ".join(
            " ".join(
                str(detail.get(key) or "")
                for key in ("title", "description", "supplier", "person", "party", "type", "entity", "value", "source")
            )
            for detail in item.analysis_details
            if isinstance(detail, dict)
        )
        searchable = normalize_text(
            " ".join([
                item.id,
                item.type,
                item.name,
                item.subtitle or "",
                item.party or "",
                item.role or "",
                item.summary,
                detail_text,
                " ".join(item.people),
                " ".join(item.analysis_types),
            ])
        )
        if normalized_query and normalized_query not in searchable:
            continue

        parsed_collected_at = parse_iso_datetime(str(record.get("collected_at") or ""))
        if parsed_collected_at and (latest_collected_at is None or parsed_collected_at > latest_collected_at):
            latest_collected_at = parsed_collected_at
        if parsed_collected_at and item.analyzed_at is None:
            item.analyzed_at = parsed_collected_at

        key = f"{item.type}:{item.id or normalize_text(item.name)}"
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = item
        else:
            by_key[key] = merge_cached_political_item(existing, item)

    if record_type == "political_people":
        for person in POLITICAL_RELATED_PEOPLE:
            candidate = public_related_political_item(person)
            if normalized_party and normalize_text(candidate.party) != normalized_party:
                continue
            candidate_text = normalize_text(
                " ".join(
                    [
                        candidate.name,
                        candidate.subtitle or "",
                        candidate.party or "",
                        candidate.role or "",
                        candidate.summary,
                        " ".join(candidate.people),
                    ]
                )
            )
            if normalized_query and normalized_query not in candidate_text:
                continue
            if normalized_risk and normalized_risk != "todos" and normalize_text(candidate.attention_level).lower() != normalized_risk:
                continue
            if normalized_analysis_type and normalized_analysis_type != "todos":
                candidate_types = {normalize_text(value).lower() for value in candidate.analysis_types}
                if normalized_analysis_type not in candidate_types and normalized_analysis_type != "prioridade":
                    continue
            candidate_key = f"{candidate.type}:{candidate.id}"
            if candidate_key not in by_key or candidate.priority_score > (by_key[candidate_key].priority_score or 0):
                by_key[candidate_key] = candidate

    risk_rank = {"alto": 3, "médio": 2, "medio": 2, "baixo": 1}
    items = list(by_key.values())
    if record_type == "political_parties":
        items = [enrich_party_from_cached_people(item, all_records) for item in items]
    for item in items:
        apply_political_priority(item)

    if size_order == "risco":
        items.sort(key=lambda item: (risk_rank.get(normalize_text(item.attention_level).lower(), 0), item.total_public_money, item.records_count, item.name), reverse=True)
    elif size_order == "viagens":
        items.sort(key=lambda item: (item.travel_public_money, item.total_public_money, item.records_count, item.name), reverse=True)
    elif size_order == "registros":
        items.sort(key=lambda item: (item.records_count, item.total_public_money, item.name), reverse=True)
    elif size_order == "valor":
        items.sort(key=lambda item: (item.total_public_money, risk_rank.get(normalize_text(item.attention_level).lower(), 0), item.records_count, item.name), reverse=True)
    else:
        items.sort(key=political_sort_key, reverse=True)
    return items[:limit], latest_collected_at


async def get_state_name_map() -> dict[str, str]:
    try:
        states = await get_json(IBGE_STATES_URL)
    except HTTPException:
        return {}
    return {state.get("sigla"): state.get("nome") for state in states if state.get("sigla")}


async def build_state_risks(page_size: int = 80) -> list[StateRiskPoint]:
    return await build_state_risks_from_source(page_size=page_size, use_local=True)


def filter_state_risks(states: list[StateRiskPoint], q: str | None = None, uf: str | None = None) -> list[StateRiskPoint]:
    normalized_query = normalize_text(q if isinstance(q, str) else None)
    uf_filter = (uf if isinstance(uf, str) else "").strip().upper()
    filtered: list[StateRiskPoint] = []
    for state in states:
        if uf_filter and state.uf.upper() != uf_filter:
            continue
        if normalized_query:
            text = normalize_text(f"{state.uf} {state.state_name or ''}")
            if normalized_query not in text:
                continue
        filtered.append(state)
    filtered.sort(key=lambda point: (point.risk_score, point.total_value, point.alerts_count), reverse=True)
    return filtered


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


@app.get("/backend", response_class=HTMLResponse, include_in_schema=False)
async def backend_ui(request: Request) -> HTMLResponse:
    require_local_request(request)
    return HTMLResponse(
        """
<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>COIBE.IA Backend</title>
  <style>
    :root{color-scheme:dark;--bg:#08090b;--panel:#111318;--panel2:#171a21;--line:#262b35;--text:#f5f7fb;--muted:#9aa3b2;--red:#ef4444;--green:#22c55e}
    *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--text);font-family:Inter,ui-sans-serif,system-ui,sans-serif;font-size:13px}
    main{width:min(1240px,calc(100% - 28px));margin:0 auto;padding:18px 0 26px}
    h1{font-size:22px;line-height:1;margin:0 0 5px;letter-spacing:0} h2{margin:18px 0 8px;font-size:12px;text-transform:uppercase;color:#cbd5e1;letter-spacing:.08em}.muted{color:var(--muted)}
    .grid{display:grid;gap:10px;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));margin-top:14px}
    .card,form{border:1px solid var(--line);background:linear-gradient(180deg,var(--panel2),var(--panel));border-radius:8px;box-shadow:0 12px 34px rgba(0,0,0,.28)}
    .card{min-height:86px;padding:12px} strong{display:block;font-size:19px;line-height:1.1;margin-top:6px;word-break:break-word}.red{color:var(--red)}.ok{color:var(--green)}
    form{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:8px;padding:12px}
    label{display:block;min-width:0;margin:0;color:#d9dee8;font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.04em}
    input,select{width:100%;margin-top:5px;border:1px solid #303642;background:#090b0f;color:#fff;border-radius:6px;padding:8px 9px;outline:none}
    input:focus,select:focus{border-color:#ef4444;box-shadow:0 0 0 2px rgba(239,68,68,.18)}
    input[type=checkbox]{width:auto;margin:0 7px 0 0;vertical-align:middle} label:has(input[type=checkbox]){display:flex;align-items:center;min-height:34px;border:1px solid #303642;background:#090b0f;border-radius:6px;padding:8px 9px}
    button{border:1px solid var(--red);background:var(--red);color:white;border-radius:6px;padding:9px 12px;font-weight:900;cursor:pointer;margin:3px 5px 3px 0}
    form button[type=submit]{align-self:end}
    button.secondary{background:#171a21;border-color:#475569} button.danger{background:#991b1b;border-color:#dc2626}
    pre{white-space:pre-wrap;word-break:break-word;background:#090b0f;border:1px solid var(--line);border-radius:8px;padding:12px;max-height:280px;overflow:auto;font-size:12px}
    a{color:#f87171} small{color:var(--muted);line-height:1.5}
  </style>
</head>
<body>
  <main>
    <h1>COIBE.IA Backend</h1>
    <p class="muted">Monitoramento local de dados públicos, modelo salvo em Models e configuração de GPU.</p>
    <section class="grid" id="cards"></section>
    <h2>Configuração local do treinamento</h2>
    <form id="configForm">
      <label>Modelo de IA selecionado<select name="selected_model_id" id="selected_model_id"></select></label>
      <label><input type="checkbox" name="deep_learning_enabled" /> Usar deep learning local no treino</label>
      <label>Modo de quantização<select name="quantization_mode"><option value="dynamic-int8">dynamic-int8</option><option value="static-int8">static-int8</option><option value="none">sem quantização</option></select></label>
      <label>Formato do modelo<input name="model_format" type="text" /></label>
      <label><input type="checkbox" name="internet_sweep_enabled" /> Fazer varredura HTML pública sem depender só de APIs</label>
      <label><input type="checkbox" name="web_fallback_enabled" /> Usar varredura web como fallback quando API/feed falhar</label>
      <label>Páginas públicas por ciclo<input name="internet_sweep_pages_per_cycle" type="number" min="0" max="50" /></label>
      <label>Timeout por página pública (segundos)<input name="internet_sweep_timeout_seconds" type="number" min="3" max="120" /></label>
      <label><input type="checkbox" name="use_gpu" /> Usar GPU quando disponível</label>
      <label>Limite de memória da GPU (MB)<input name="gpu_memory_limit_mb" type="number" min="256" max="49152" /></label>
      <label><input type="checkbox" name="use_shared_memory" /> Permitir memória compartilhada/RAM do PC quando GPU não bastar</label>
      <label>Limite de memória compartilhada/RAM (MB)<input name="shared_memory_limit_mb" type="number" min="512" max="131072" /></label>
      <label>Perfil de treinamento/varredura<select name="scan_profile"><option value="conservative">conservador</option><option value="balanced">equilibrado</option><option value="heavy">pesado</option><option value="no-delay-heavy">pesado sem delay</option></select></label>
      <label>Concorrência máxima de fontes/API<input name="max_concurrent_requests" type="number" min="1" max="24" /></label>
      <label>Concorrência em APIs públicas<input name="public_api_concurrency" type="number" min="1" max="8" /></label>
      <label>Modo das APIs públicas<select name="public_api_source_mode"><option value="hybrid">híbrido</option><option value="cache-first">cache primeiro</option><option value="live">sempre live</option></select></label>
      <label>Concorrência da varredura HTML<input name="internet_sweep_concurrency" type="number" min="1" max="24" /></label>
      <label>Uso da GPU no analisador<select name="gpu_acceleration_level"><option value="off">desligado</option><option value="balanced">equilibrado</option><option value="aggressive">agressivo</option></select></label>
      <label>Amostra máxima de treino por ciclo<input name="training_sample_limit" type="number" min="100" max="10000" /></label>
      <label>Lote de análise<input name="analysis_batch_size" type="number" min="16" max="5000" /></label>
      <label>Lote de aprendizagem<input name="learning_batch_size" type="number" min="16" max="10000" /></label>
      <label>Tempo máximo de pesquisa por ciclo (segundos)<input name="research_timeout_seconds" type="number" min="15" max="900" /></label>
      <label>Rodadas/páginas por ciclo<input name="research_rounds" type="number" min="1" max="100" /></label>
      <label>Itens por rodada<input name="feed_page_size" type="number" min="10" max="100" /></label>
      <label>Partidos por varredura<input name="political_party_scan_limit" type="number" min="1" max="24" /></label>
      <label>Políticos por varredura<input name="political_people_scan_limit" type="number" min="1" max="36" /></label>
      <label>Termos aprendidos por ciclo<input name="learned_terms_per_cycle" type="number" min="1" max="100" /></label>
      <label>Recortes de alto risco por ciclo<input name="priority_feed_queries_per_cycle" type="number" min="0" max="20" /></label>
      <label>Buscas universais por ciclo<input name="search_terms_per_cycle" type="number" min="0" max="60" /></label>
      <label>Espera entre buscas universais (segundos)<input name="search_delay_seconds" type="number" min="0" max="60" step="0.5" /></label>
      <button type="submit">Salvar configuração local</button>
      <span id="saveStatus" class="muted"></span>
    </form>
    <h2>Controle local</h2>
    <section class="card">
      <button id="startBackend" type="button">Iniciar backend visível</button>
      <button id="startTraining" type="button">Iniciar treinamento</button>
      <button id="stopTraining" class="secondary" type="button">Parar treinamento</button>
      <button id="restartBackend" class="danger" type="button">Reiniciar backend</button>
      <p class="muted" id="controlStatus">Os comandos funcionam apenas neste backend local.</p>
    </section>
    <h2>Modelo de monitoramento</h2>
    <pre id="model">Carregando...</pre>
    <h2>Monitor</h2>
    <pre id="monitor">Carregando...</pre>
    <p><small>Monitoramento público: a plataforma prioriza fontes oficiais e conferência humana, sem concluir crime, culpa, parentesco ou irregularidade.</small></p>
  </main>
  <script>
    async function getJson(path){ const r = await fetch(path, {cache:'no-store'}); if(!r.ok) throw new Error(path+' HTTP '+r.status); return r.json(); }
    async function postJson(path, payload){ const r = await fetch(path, {method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(payload)}); if(!r.ok) throw new Error(path+' HTTP '+r.status); return r.json(); }
    function card(label,value,note,cls=''){ return `<article class="card"><span class="muted">${label}</span><strong class="${cls}">${value}</strong><small>${note||''}</small></article>`; }
    function fillConfig(config){
      const form = document.getElementById('configForm');
      for(const [key,value] of Object.entries(config||{})){
        const field = form.elements[key];
        if(!field) continue;
        if(field.type === 'checkbox') field.checked = Boolean(value);
        else field.value = value;
      }
    }
    function fillModelOptions(models, selected){
      const field = document.getElementById('selected_model_id');
      field.innerHTML = (models||[]).map(model => `<option value="${model.id}">${model.name || model.id}${model.available === false ? ' (preparando)' : ''}</option>`).join('');
      if(selected) field.value = selected;
    }
    function readConfig(){
      const form = document.getElementById('configForm');
      const data = {};
      for(const field of form.elements){
        if(!field.name) continue;
        if(field.type === 'checkbox') data[field.name] = field.checked;
        else if(field.type === 'number') data[field.name] = Number(field.value);
        else data[field.name] = field.value;
      }
      return data;
    }
    async function load(){
      const [model, monitor] = await Promise.all([getJson('/api/models/status'), getJson('/api/monitoring/status')]);
      fillModelOptions(model.available_models, model.config?.selected_model_id || model.selected_model?.id);
      fillConfig(model.config);
      const quality = model.model_quality || {};
      const qualityValue = quality.accuracy ?? quality.macro_f1 ?? (quality.sample_size ? 'novo treino' : 'n/d');
      const qualityNote = quality.sample_size
        ? `amostra ${quality.sample_size} • classes ${quality.class_count || 'n/d'} • ${quality.metric_source || quality.status || 'sem metrica'}`
        : 'aguardando primeiro treino com metricas';
      document.getElementById('cards').innerHTML = [
        card('Modelo', model.selected_model?.name || model.version || 'n/d', model.updated_at || 'aguardando treino'),
        card(
          'Aprendizados',
          `${model.learned_accumulated_total_count || model.learned_total_count || 0} itens`,
          `${model.learned_terms_count || 0} termos ativos • ${model.learned_checks_count || 0} verificacoes • ${model.learned_targets_count || 0} alvos • ${model.evolution_memory_summary?.events || 0} ciclos`
        ),
        card('IA/Quantização', model.model_artifacts?.deep_model_trained ? 'Deep pronta' : 'Manifesto pronto', model.model_artifacts?.quantization_mode || model.config?.quantization_mode || 'dynamic-int8'),
        card('Qualidade ML', qualityValue, qualityNote),
        card('Memória', `${model.evolution_memory_events || 0} ciclos`, model.evolution_memory_path || 'aguardando próximo ciclo'),
        card('Perfil', model.config?.scan_profile || 'balanced', `concorrência ${model.config?.max_concurrent_requests || 4} • API pública ${model.config?.public_api_source_mode || 'hybrid'}/${model.config?.public_api_concurrency || 3} • lotes ${model.config?.analysis_batch_size || 256}/${model.config?.learning_batch_size || 512}`),
        card('Agentes', `${monitor.autonomous_agents?.summary?.insights_count || 0} insights`, `${monitor.autonomous_agents?.summary?.item_flags_count || 0} flags em itens`),
        card('Internet', monitor.collector_state?.internet_items_collected || model.cache_profile?.internet_pages_seen || 0, model.config?.internet_sweep_enabled ? `HTML ativa • fallback ${model.config?.web_fallback_enabled ? 'ativo' : 'off'} • usados ${monitor.collector_state?.web_fallback_items_collected || 0}` : 'desativada'),
        card('GPU', model.gpu.available ? model.gpu.name : 'Não detectada', model.gpu.enabled ? 'ativada na configuração' : 'desativada na configuração', model.gpu.available ? 'ok' : ''),
        card('Treinamento', model.training_process?.running ? 'Rodando' : 'Parado', model.training_process?.pid ? 'PID '+model.training_process.pid : 'sem processo'),
        card('Itens analisados', monitor.items_analyzed || 0, monitor.generated_at || 'sem ciclo'),
      ].join('');
      document.getElementById('model').textContent = JSON.stringify(model, null, 2);
      document.getElementById('monitor').textContent = JSON.stringify(monitor, null, 2);
    }
    document.getElementById('configForm').addEventListener('submit', async (event) => {
      event.preventDefault();
      document.getElementById('saveStatus').textContent = ' Salvando...';
      const saved = await postJson('/api/models/config', readConfig());
      document.getElementById('saveStatus').textContent = ' Salvo. Reinicie o monitor para aplicar em processo já aberto.';
      fillConfig(saved);
      await load();
    });
    document.getElementById('startTraining').addEventListener('click', async () => {
      document.getElementById('controlStatus').textContent = 'Iniciando treinamento...';
      const result = await postJson('/api/models/training/start', {});
      document.getElementById('controlStatus').textContent = result.running ? `Treinamento rodando em terminal visível. PID ${result.pid}` : 'Treinamento não iniciou.';
      await load();
    });
    document.getElementById('stopTraining').addEventListener('click', async () => {
      document.getElementById('controlStatus').textContent = 'Parando treinamento...';
      await postJson('/api/models/training/stop', {});
      document.getElementById('controlStatus').textContent = 'Treinamento parado. Backend continua ativo.';
      await load();
    });
    document.getElementById('startBackend').addEventListener('click', async () => {
      if(!confirm('Abrir o backend em um terminal visível? O script remove processos duplicados na porta 8000.')) return;
      document.getElementById('controlStatus').textContent = 'Abrindo backend em terminal visível...';
      await postJson('/api/backend/start', {});
    });
    document.getElementById('restartBackend').addEventListener('click', async () => {
      if(!confirm('Reiniciar o backend local em um novo terminal visível?')) return;
      document.getElementById('controlStatus').textContent = 'Reiniciando backend em novo terminal...';
      await postJson('/api/backend/restart', {});
    });
    load().catch(err => { document.getElementById('model').textContent = String(err); });
  </script>
</body>
</html>
        """.strip()
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/models/status")
async def models_status(request: Request) -> dict[str, Any]:
    require_local_request(request)
    return monitor_model_status()


@app.get("/api/models/config", response_model=MonitorModelConfig)
async def models_config(request: Request) -> MonitorModelConfig:
    require_local_request(request)
    return read_monitor_model_config()


@app.post("/api/models/config", response_model=MonitorModelConfig)
async def save_models_config(payload: MonitorModelConfig, request: Request) -> MonitorModelConfig:
    require_local_request(request)
    return write_monitor_model_config(payload)


@app.post("/api/models/training/start")
async def start_models_training(request: Request) -> dict[str, Any]:
    require_local_request(request)
    return start_monitor_training_process()


@app.post("/api/models/training/stop")
async def stop_models_training(request: Request) -> dict[str, Any]:
    require_local_request(request)
    return stop_monitor_training_process()


@app.post("/api/backend/restart")
async def restart_backend(request: Request, background_tasks: BackgroundTasks) -> dict[str, Any]:
    require_local_request(request)
    background_tasks.add_task(restart_backend_visible_terminal)
    return {
        "status": "restarting",
        "message": "Backend será reiniciado em um novo terminal visível. Fechar o terminal encerra o backend.",
    }


@app.post("/api/backend/start")
async def start_backend_visible(request: Request, background_tasks: BackgroundTasks) -> dict[str, Any]:
    require_local_request(request)
    background_tasks.add_task(restart_backend_visible_terminal)
    return {
        "status": "starting",
        "message": "Backend será aberto em um terminal visível. O script remove duplicações na porta 8000.",
    }


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
        model_status = public_monitor_model_summary()
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
            model_status=model_status,
            **platform_metrics,
            message="A análise contínua ainda não gerou o primeiro ciclo.",
        )

    try:
        data = read_data_json(LOCAL_MONITOR_LATEST_PATH, {})
        latest_data = data
    except Exception:
        library_status = read_library_status()
        model_status = public_monitor_model_summary()
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
            model_status=model_status,
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
    model_status = public_monitor_model_summary()
    platform_metrics = merge_metric_max(platform_metrics, raw_analysis_metrics(latest_data))
    collector_state = data.get("collector_state") if isinstance(data.get("collector_state"), dict) else {}
    knowledge_items_count = max(
        int(data.get("items_analyzed") or 0),
        database_items_count,
    ) + public_records_count
    collector_state = {
        **collector_state,
        "knowledge_items_count": knowledge_items_count,
        "contracts_items_count": database_items_count,
        "public_records_count": public_records_count,
    }
    autonomous_agents = data.get("agents") if isinstance(data.get("agents"), dict) else {}
    return LocalMonitorStatus(
        running=running,
        latest_file_exists=True,
        latest_analysis_path=str(LOCAL_MONITOR_LATEST_PATH),
        database_path=str(LOCAL_MONITOR_DB_PATH),
        public_records_path=str(LOCAL_PUBLIC_RECORDS_PATH),
        generated_at=generated_at,
        items_analyzed=knowledge_items_count,
        alerts_count=int(data.get("alerts_count") or 0),
        database_items_count=database_items_count,
        public_records_count=public_records_count,
        library_records_count=library_status.records_count,
        library_records_added=library_status.records_added_last_cycle,
        public_codes_count=library_status.public_codes_count,
        collector_state=collector_state,
        model_status=model_status,
        autonomous_agents=autonomous_agents,
        **platform_metrics,
        message="Análise contínua atualizada recentemente." if running else "Análise contínua aguardando nova atualização.",
    )


@app.get("/api/monitoring/status", response_model=LocalMonitorStatus)
async def monitoring_status() -> LocalMonitorStatus:
    return await cached_api_value("monitoring-status", 8, local_monitor_status)


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
async def universal_search(
    request: Request,
    q: str = Query(..., min_length=2, max_length=120),
    refresh: bool = Query(False, description="Ignora cache e consulta as APIs publicas novamente."),
    x_coibe_admin_token: str | None = Header(default=None),
) -> UniversalSearchResponse:
    if refresh:
        allow_local_or_admin_live_scan(request, x_coibe_admin_token)
    cached = load_cached_universal_search(q)
    if cached and not refresh and cached.public_api_checked:
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
        from_cache=False,
        cache_status="refreshed" if refresh else ("miss" if cached is None else "revalidated"),
        public_api_checked=True,
    )
    save_cached_universal_search(q, response)
    save_universal_search_public_records(q, response)
    return response


@app.get("/api/search/local", response_model=LocalSearchResponse)
async def local_fuzzy_search(q: str = Query(..., min_length=2, max_length=120), limit: int = Query(10, ge=1, le=30)) -> LocalSearchResponse:
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
async def search_autocomplete(q: str = Query(..., min_length=2, max_length=120), limit: int = Query(10, ge=1, le=20)) -> LocalSearchResponse:
    return await local_fuzzy_search(q=q, limit=limit)


@app.get("/api/search/index", response_model=LocalSearchResponse)
async def indexed_search(q: str = Query(..., min_length=2, max_length=120), limit: int = Query(10, ge=1, le=30)) -> LocalSearchResponse:
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
    path: str = Query(..., min_length=1, max_length=180, description="Caminho da API, ex: contratos ou contratos/{id}"),
    _admin: None = Depends(require_admin_token),
) -> Any:
    return await get_portal_transparencia_json(path)


@app.get("/api/monitoring/feed", response_model=MonitoringFeedResponse)
async def monitoring_feed(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=10, le=100),
    q: str | None = Query(None, min_length=2, max_length=120),
    uf: str | None = Query(None, min_length=2, max_length=2),
    city: str | None = Query(None, min_length=2, max_length=100),
    risk_level: str | None = Query(None),
    size_order: str | None = Query(None, pattern="^(data|data_asc|asc|desc)$"),
    date_from: date | None = Query(None, description="Data inicial do conteúdo/contrato, não da análise."),
    date_to: date | None = Query(None, description="Data final do conteúdo/contrato, não da análise."),
    source: str = Query("auto", pattern="^(auto|local|live)$"),
) -> MonitoringFeedResponse:
    if source != "live":
        local_items = load_local_monitoring_items(q=q, uf=uf, city=city, date_from=date_from, date_to=date_to)
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
        if city:
            city_filter = normalize_text(city)
            filtered = [item for item in filtered if city_filter in normalize_text(item.city or item.location)]
        filtered = filter_and_order_monitoring_items(filtered, risk_level, size_order)
        start = (page - 1) * page_size
        items = filtered[start : start + page_size]
        has_more = start + page_size < len(filtered)
    else:
        filtered_live_feed = bool(risk_level or size_order or date_from or date_to or city)
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
        if city:
            city_filter = normalize_text(city)
            enriched = [item for item in enriched if city_filter in normalize_text(item.city or item.location)]
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
    q: str = Query(..., min_length=2, max_length=120),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=10, le=50),
) -> MonitoringFeedResponse:
    return await monitoring_feed(page=page, page_size=page_size, q=q, uf=None)


@app.get("/api/political/parties", response_model=PoliticalScanResponse)
async def political_parties(
    request: Request,
    q: str | None = Query(None, min_length=2, max_length=120),
    limit: int = Query(12, ge=1, le=24),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=10, le=50),
    risk_level: str | None = Query(None),
    analysis_type: str | None = Query(None),
    size_order: str | None = Query(None, pattern="^(prioridade|valor|viagens|registros|risco)$"),
    source: str = Query("auto", pattern="^(auto|local|live)$"),
    x_coibe_admin_token: str | None = Header(default=None),
) -> PoliticalScanResponse:
    if source != "live":
        cached_limit = max(limit, page * page_size + 1)
        cached_items, generated_at = cached_political_items(
            "political_parties",
            q=q,
            risk_level=risk_level,
            analysis_type=analysis_type,
            size_order=size_order,
            limit=cached_limit,
        )
        if not cached_items and source != "local" and q:
            live_items = await political_parties_scan(limit=limit, q=q)
            save_political_scan_public_records("political_parties", q, live_items)
            cached_items, generated_at = cached_political_items(
                "political_parties",
                q=q,
                risk_level=risk_level,
                analysis_type=analysis_type,
                size_order=size_order,
                limit=cached_limit,
            )
        start = (page - 1) * page_size
        items = cached_items[start : start + page_size]
        return PoliticalScanResponse(
            generated_at=generated_at or brasilia_now(),
            kind="partidos",
            sources=["Base COIBE.IA autoatualizável", *POLITICAL_PARTY_SOURCES],
            items=items,
            page=page,
            page_size=page_size,
            has_more=start + page_size < len(cached_items),
            total_returned=len(items),
        )

    allow_local_or_admin_live_scan(request, x_coibe_admin_token)
    live_items = await political_parties_scan(limit=limit, q=q)
    save_political_scan_public_records("political_parties", q, live_items)
    return PoliticalScanResponse(
        generated_at=brasilia_now(),
        kind="partidos",
        sources=POLITICAL_PARTY_SOURCES,
        items=live_items,
        page=1,
        page_size=limit,
        has_more=False,
        total_returned=len(live_items),
    )


@app.get("/api/political/politicians", response_model=PoliticalScanResponse)
async def political_politicians(
    request: Request,
    q: str | None = Query(None, min_length=2, max_length=120),
    party: str | None = Query(None, min_length=2, max_length=12),
    limit: int = Query(18, ge=1, le=36),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=10, le=50),
    risk_level: str | None = Query(None),
    analysis_type: str | None = Query(None),
    size_order: str | None = Query(None, pattern="^(prioridade|valor|viagens|registros|risco)$"),
    source: str = Query("auto", pattern="^(auto|local|live)$"),
    offset: int = Query(0, ge=0, le=1000),
    x_coibe_admin_token: str | None = Header(default=None),
) -> PoliticalScanResponse:
    if source != "live":
        cached_limit = max(limit, page * page_size + 1)
        cached_items, generated_at = cached_political_items(
            "political_people",
            q=q,
            party=party,
            risk_level=risk_level,
            analysis_type=analysis_type,
            size_order=size_order,
            limit=cached_limit,
        )
        if not cached_items and source != "local" and q:
            refresh_limit = min(max(limit, 1), 12)
            schedule_political_people_cache_refresh(q, party, refresh_limit, offset)
            live_items: list[PoliticalScanItem] = []
            try:
                live_items = await asyncio.wait_for(
                    political_people_quick_scan(limit=refresh_limit, q=q, party=party, offset=offset),
                    timeout=4.0,
                )
            except (TimeoutError, asyncio.TimeoutError):
                live_items = []
            if live_items:
                save_political_scan_public_records("political_people", q, live_items)
                cached_items, generated_at = cached_political_items(
                    "political_people",
                    q=q,
                    party=party,
                    risk_level=risk_level,
                    analysis_type=analysis_type,
                    size_order=size_order,
                    limit=cached_limit,
                )
                if not cached_items:
                    cached_items = live_items
                    generated_at = brasilia_now()
            else:
                cached_items = [provisional_political_item(q, party)]
                generated_at = brasilia_now()
        start = (page - 1) * page_size
        items = cached_items[start : start + page_size]
        return PoliticalScanResponse(
            generated_at=generated_at or brasilia_now(),
            kind="politicos",
            sources=["Base COIBE.IA autoatualizável", *POLITICAL_PEOPLE_SOURCES],
            items=items,
            page=page,
            page_size=page_size,
            has_more=start + page_size < len(cached_items),
            total_returned=len(items),
        )

    allow_local_or_admin_live_scan(request, x_coibe_admin_token)
    live_items = await political_people_scan(limit=limit, q=q, party=party, offset=offset)
    save_political_scan_public_records("political_people", q, live_items)
    return PoliticalScanResponse(
        generated_at=brasilia_now(),
        kind="politicos",
        sources=POLITICAL_PEOPLE_SOURCES,
        items=live_items,
        page=1,
        page_size=limit,
        has_more=False,
        total_returned=len(live_items),
    )


@app.post("/api/monitoring/priority-scan", response_model=PriorityScanResponse)
async def monitoring_priority_scan(
    q: str | None = Query(None, min_length=2, max_length=120),
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
    page_size: int = Query(50, ge=10, le=500),
    q: str | None = Query(None, min_length=2, max_length=120),
    uf: str | None = Query(None, min_length=2, max_length=2),
    city: str | None = Query(None, min_length=2, max_length=100),
    source: str = Query("auto", pattern="^(auto|local|live)$"),
    min_lat: float | None = Query(None, ge=-90, le=90),
    max_lat: float | None = Query(None, ge=-90, le=90),
    min_lng: float | None = Query(None, ge=-180, le=180),
    max_lng: float | None = Query(None, ge=-180, le=180),
) -> MonitoringMapResponse:
    cache_key = f"monitoring-map:v{MAP_POINTS_CACHE_VERSION}:{page_size}:{q}:{uf}:{city}:{source}:{min_lat}:{max_lat}:{min_lng}:{max_lng}"

    async def build_response() -> MonitoringMapResponse:
        cache_status = "live"
        if source != "live":
            file_points = read_map_points_cache()
            if file_points:
                filtered = filter_map_points(file_points, q=q, uf=uf, city=city, min_lat=min_lat, max_lat=max_lat, min_lng=min_lng, max_lng=max_lng)
                if filtered:
                    if POSTGIS_ENABLED:
                        await asyncio.to_thread(postgis_upsert_map_points, file_points)
                    return MonitoringMapResponse(
                        sources=["Cache local do mapa", "Base COIBE.IA autoatualizavel", "IBGE UASG/municipios"],
                        points=filtered[:page_size],
                        generated_at=brasilia_now(),
                        cache_status="file-hit",
                        postgis_enabled=POSTGIS_ENABLED,
                        total_returned=min(len(filtered), page_size),
                    )

        local_items = load_local_monitoring_items() if source != "live" else []
        if local_items:
            points = await build_monitoring_map_points_from_items(local_items)
            cache_status = "local-refresh"
        else:
            contracts = await fetch_public_contracts(page=1, page_size=min(max(page_size, 10), 100), query=q or city)
            estimates = estimate_variations_with_ml(contracts)
            items = await asyncio.gather(*[
                contract_to_monitoring_item(
                    contract,
                    include_diario=False,
                    ml_estimate=estimates.get(contract_stable_key(contract)),
                )
                for contract in contracts
            ])
            if uf:
                items = [item for item in items if (item.uf or "").upper() == uf.upper()]
            if city:
                city_filter = normalize_text(city)
                items = [item for item in items if city_filter in normalize_text(item.city or item.location)]
            points = await build_monitoring_map_points_from_items(items)
            cache_status = "live-refresh"

        all_points = points
        points = filter_map_points(all_points, q=q, uf=uf, city=city, min_lat=min_lat, max_lat=max_lat, min_lng=min_lng, max_lng=max_lng)
        write_map_points_cache(all_points)
        await asyncio.to_thread(postgis_upsert_map_points, all_points)
        return MonitoringMapResponse(
            sources=["Base COIBE.IA autoatualizavel", "Compras.gov.br Dados Abertos", "PNCP", "IBGE UASG/municipios", "PostGIS opcional"],
            points=points[:page_size],
            generated_at=brasilia_now(),
            cache_status=cache_status,
            postgis_enabled=POSTGIS_ENABLED,
            total_returned=min(len(points), page_size),
        )

    return await cached_api_value(cache_key, MAP_CACHE_TTL_SECONDS, build_response)


@app.get("/api/monitoring/state-map", response_model=StateMapResponse)
async def monitoring_state_map(
    page_size: int = Query(80, ge=10, le=100),
    q: str | None = Query(None, min_length=2, max_length=120),
    uf: str | None = Query(None, min_length=2, max_length=2),
    source: str = Query("auto", pattern="^(auto|local|live)$"),
) -> StateMapResponse:
    cache_key = f"monitoring-state-map:{page_size}:{q}:{uf}:{source}"

    async def build_response() -> StateMapResponse:
        cached = read_ttl_json(MONITORING_STATE_MAP_CACHE_PATH, MAP_CACHE_TTL_SECONDS, None) if source != "live" else None
        if isinstance(cached, dict) and isinstance(cached.get("states"), list):
            states = []
            for row in cached.get("states") or []:
                try:
                    states.append(StateRiskPoint.model_validate(row))
                except Exception:
                    continue
            states = filter_state_risks(states, q=q, uf=uf)
            if states:
                return StateMapResponse(
                    sources=["Cache local do mapa", "Base COIBE.IA autoatualizavel", "IBGE Malhas Territoriais"],
                    states=states[:page_size],
                    generated_at=brasilia_now(),
                    cache_status="file-hit",
                )

        all_states = await build_state_risks_from_source(page_size, use_local=source != "live")
        write_ttl_json(
            MONITORING_STATE_MAP_CACHE_PATH,
            {
                "generated_at": brasilia_now().isoformat(),
                "states": [state.model_dump(mode="json") for state in all_states],
            },
        )
        states = filter_state_risks(all_states, q=q, uf=uf)
        return StateMapResponse(
            sources=["Base COIBE.IA autoatualizavel", "Compras.gov.br Dados Abertos", "PNCP", "IBGE UASG/municipios", "IBGE Malhas Territoriais"],
            states=states[:page_size],
            generated_at=brasilia_now(),
            cache_status="local-refresh" if source != "live" else "live-refresh",
        )

    return await cached_api_value(cache_key, MAP_CACHE_TTL_SECONDS, build_response)


@app.get("/api/public-data/ibge/states-geojson")
async def ibge_states_geojson() -> dict[str, Any]:
    cached = read_ttl_json(IBGE_STATES_GEOJSON_CACHE_PATH, 7 * 24 * 60 * 60, None)
    if isinstance(cached, dict):
        cached["coibe_cache_status"] = "hit"
        return cached
    try:
        data = await get_json(
            IBGE_STATES_GEOJSON_URL,
            params={
                "formato": "application/vnd.geo+json",
                "qualidade": "intermediaria",
                "intrarregiao": "UF",
            },
        )
        write_json(IBGE_STATES_GEOJSON_CACHE_PATH, data)
        return data
    except HTTPException:
        cached = read_data_json(IBGE_STATES_GEOJSON_CACHE_PATH, None)
        if isinstance(cached, dict):
            cached["coibe_cache_fallback"] = True
            return cached
        raise


@app.get("/api/public-data/cnpj/{cnpj}")
async def public_cnpj_data(cnpj: str) -> dict[str, Any]:
    normalized_cnpj = normalize_cnpj(cnpj)
    data = await fetch_cnpj_details_cached(normalized_cnpj)
    if data is None:
        raise HTTPException(status_code=404, detail="CNPJ nao encontrado na Brasil API.")
    return data


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
    q: str | None = Query(None, min_length=2, max_length=120),
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
    q: str | None = Query(None, min_length=2, max_length=120),
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
        pipeline_order=[
            "connectors_and_scrapers",
            "internet_html_sweep",
            "raw_database_merge",
            "risk_rules_and_ml",
            "autonomous_agents",
            "ai_artifact_training",
        ],
        implemented_now=[
            "coleta sem salvar arquivos baixados",
            "varredura HTML publica independente de APIs",
            "normalizacao de texto e valores",
            "hash MD5 CNPJ+data+valor para deduplicacao",
            "busca fuzzy sobre índice da plataforma para autocomplete",
            "z-score sobre base acumulada",
            "calculo espacial por Haversine para alerta logistico",
            "deep learning local com manifesto ONNX e compatibilidade de quantizacao",
            "diagnostico de dataset e metricas de treino/validacao do classificador local",
            "memoria evolutiva JSONL append-only para aprendizado ciclo a ciclo",
            "monitor local como orquestrador de coleta, analise, treino e memoria",
            "agentes autonomos deterministicos para grafo relacional, lacunas de evidencia, fontes e qualidade de dados",
        ],
        production_targets=[
            "PostgreSQL com particionamento por ano/UF",
            "embeddings persistidos em PostgreSQL com pgvector",
            "distancia geografica no PostGIS com ST_DistanceSphere",
            "OpenSearch/Elasticsearch para indice invertido em producao",
            "worker Celery/RabbitMQ para sincronizacao incremental",
            "agentes especializados para coleta, triagem, verificacao documental e explicacao",
            "avaliacao offline com conjunto rotulado e comparacao de modelos antes de promover artefatos",
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
