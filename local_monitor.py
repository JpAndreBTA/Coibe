import argparse
import asyncio
import hashlib
import json
import os
import re
import site
import subprocess
import unicodedata
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from statistics import mean, pstdev
from typing import Any
from zoneinfo import ZoneInfo

import httpx

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


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
MONITOR_MODEL_CONFIG_PATH = MODELS_DIR / "monitor_config.json"
ML_USE_GPU = os.getenv("COIBE_ML_USE_GPU", "false").lower() in {"1", "true", "yes"}
GPU_MEMORY_LIMIT_MB = int(os.getenv("COIBE_GPU_MEMORY_LIMIT_MB", "2048"))
USE_SHARED_MEMORY = os.getenv("COIBE_ML_USE_SHARED_MEMORY", "true").lower() in {"1", "true", "yes"}
SHARED_MEMORY_LIMIT_MB = int(os.getenv("COIBE_SHARED_MEMORY_LIMIT_MB", "4096"))
RESEARCH_TIMEOUT_SECONDS = int(os.getenv("COIBE_RESEARCH_TIMEOUT_SECONDS", "90"))
MAX_LEARNED_TERMS_PER_CYCLE = int(os.getenv("COIBE_MODEL_LEARNED_TERMS_PER_CYCLE", "12"))
MONITOR_SEARCH_TERMS_PER_CYCLE = int(os.getenv("COIBE_MONITOR_SEARCH_TERMS_PER_CYCLE", "8"))
MONITOR_SEARCH_DELAY_SECONDS = float(os.getenv("COIBE_MONITOR_SEARCH_DELAY_SECONDS", "2.0"))
MONITOR_PRIORITY_FEED_QUERIES_PER_CYCLE = int(os.getenv("COIBE_MONITOR_PRIORITY_FEED_QUERIES_PER_CYCLE", "4"))
POLITICAL_PARTY_SCAN_LIMIT = int(os.getenv("COIBE_POLITICAL_PARTY_SCAN_LIMIT", "12"))
POLITICAL_PEOPLE_SCAN_LIMIT = int(os.getenv("COIBE_POLITICAL_PEOPLE_SCAN_LIMIT", "24"))
CUDA_DLL_HANDLES: list[Any] = []
LOW_PRIORITY_REPEATED_TERMS = {"TIRIRICA"}

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
        "research_timeout_seconds": RESEARCH_TIMEOUT_SECONDS,
        "research_rounds": None,
        "feed_page_size": None,
        "political_party_scan_limit": POLITICAL_PARTY_SCAN_LIMIT,
        "political_people_scan_limit": POLITICAL_PEOPLE_SCAN_LIMIT,
        "learned_terms_per_cycle": MAX_LEARNED_TERMS_PER_CYCLE,
        "search_terms_per_cycle": MONITOR_SEARCH_TERMS_PER_CYCLE,
        "search_delay_seconds": MONITOR_SEARCH_DELAY_SECONDS,
        "priority_feed_queries_per_cycle": MONITOR_PRIORITY_FEED_QUERIES_PER_CYCLE,
    }
    if MONITOR_MODEL_CONFIG_PATH.exists():
        try:
            loaded = json.loads(MONITOR_MODEL_CONFIG_PATH.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                config.update({key: value for key, value in loaded.items() if key in config and value is not None})
        except Exception:
            pass
    return config


def gpu_runtime_status(config: dict[str, Any] | None = None) -> dict[str, Any]:
    config = config or load_monitor_config()
    prepare_cuda_dll_paths()
    status = {
        "enabled_by_env": ML_USE_GPU,
        "enabled_by_config": bool(config.get("use_gpu")),
        "enabled": bool(ML_USE_GPU and config.get("use_gpu")),
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
    if bool(ML_USE_GPU and config.get("use_gpu")) and gpu_limit > 0:
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
            loaded = json.loads(MONITOR_MODEL_STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            loaded = empty_state
        state = loaded if isinstance(loaded, dict) else empty_state

    learned_terms = state.get("learned_terms") if isinstance(state.get("learned_terms"), list) else []
    if learned_terms:
        return state

    latest_path = Path("data/processed/latest_analysis.json")
    if latest_path.exists():
        try:
            latest = json.loads(latest_path.read_text(encoding="utf-8"))
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


def update_monitor_model_state(analysis: dict[str, Any], snapshot: dict[str, Any], search_terms: list[str], gpu: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    state = load_monitor_model_state()
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
    for alert in analysis.get("alerts", [])[:80]:
        if not isinstance(alert, dict):
            continue
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

    for record in analysis.get("public_records", [])[:120]:
        if not isinstance(record, dict):
            continue
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

    for item in [*(analysis.get("items") or []), *(analysis.get("model_training_items") or [])][:500]:
        if not isinstance(item, dict):
            continue
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

    for normalized, candidate in selected_candidates:
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

    learned_terms = sorted(learned_by_key.values(), key=lambda item: (float(item.get("score") or 0), int(item.get("hits") or 0)), reverse=True)[:200]
    learned_checks = sorted(learned_checks_by_id.values(), key=lambda item: (float(item.get("score") or 0), int(item.get("hits") or 0)), reverse=True)[:80]
    learned_targets = sorted(learned_targets_by_id.values(), key=lambda item: (float(item.get("score") or 0), int(item.get("hits") or 0)), reverse=True)[:120]
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
        "cache_profile": {
            "date_window": date_window,
            "public_record_types": record_types,
            "public_records_seen": len(analysis.get("public_records", []) or []),
            "alerts_seen": len(analysis.get("alerts", []) or []),
            "items_seen": len(analysis.get("items", []) or []),
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
            "items_analyzed": analysis.get("items_analyzed"),
            "alerts_count": analysis.get("alerts_count"),
            "learned_terms_added": len(selected_candidates),
            "learned_checks_added": min(len(check_candidates), max(learned_limit, 8)),
            "learned_targets_added": min(len(target_candidates), max(learned_limit * 2, 16)),
            "method_research_applied": True,
            "method_research_sources": [
                "Open Contracting Partnership",
                "World Bank",
                "PNCP",
                "TCU",
                "CGU",
            ],
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
        snapshot["connectors"].append(
            {
                "name": name,
                "kind": kind,
                "path": path,
                "status": "rate_limited" if retry_after else "error",
                "record_count": 0,
                "error": str(exc),
                "retry_after_seconds": retry_after,
                "started_at": started_at.isoformat(),
                "finished_at": brasilia_now().isoformat(),
            }
        )
        snapshot["errors"].append({"connector": name, "error": str(exc)})
        if retry_after:
            monitor_print(f"LIMITE {name} | tempo={elapsed:.2f}s | aguardando {retry_after:.0f}s antes de novas buscas | detalhe={exc}")
        else:
            monitor_print(f"ERRO {name} | tempo={elapsed:.2f}s | detalhe={exc}")
        return None


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
) -> dict[str, Any]:
    timeout_value = max(15, int(timeout_seconds))
    timeout = httpx.Timeout(float(timeout_value), connect=min(10.0, float(timeout_value)))
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
            "/api/monitoring/state-map?page_size=80&source=live",
            "public_api_aggregation",
        )
        if state_map is not None:
            snapshot["state_map"] = state_map

        political_parties = await collect_connector(
            client,
            snapshot,
            "political_parties_scan",
            f"/api/political/parties?limit={political_party_limit}&source=live",
            "public_political_risk_scan",
        )
        if political_parties is not None:
            snapshot["political_parties"] = political_parties

        political_people = await collect_connector(
            client,
            snapshot,
            "political_people_scan",
            f"/api/political/politicians?limit={political_people_limit}&source=live&offset={max(int(political_people_offset or 0), 0)}",
            "public_political_risk_scan",
        )
        if political_people is not None:
            snapshot["political_people"] = political_people

        priority_queries = priority_feed_queries(search_terms, priority_feed_queries_per_cycle)
        snapshot["priority_feed_queries"] = priority_queries
        for index, query in enumerate(priority_queries):
            if index > 0:
                await asyncio.sleep(0.5)
            encoded = httpx.QueryParams(
                {
                    "page": 1,
                    "page_size": min(page_size, 50),
                    "source": "live",
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

        search_limit = max(0, int(search_terms_per_cycle))
        search_delay = max(0.0, float(search_delay_seconds))
        rotated_search_terms = rotate_list(search_terms, search_term_offset)
        limited_search_terms = rotated_search_terms[:search_limit] if search_limit else []
        if len(search_terms) > len(limited_search_terms):
            monitor_print(
                f"Busca universal limitada neste ciclo | termos_usados={len(limited_search_terms)} "
                f"de {len(search_terms)} | offset={search_term_offset} | ajuste COIBE_MONITOR_SEARCH_TERMS_PER_CYCLE se precisar"
            )
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

    for bucket_name, source_name in [
        ("political_parties", "COIBE.IA/Camara/TSE - Partidos"),
        ("political_people", "COIBE.IA/Camara/Senado - Politicos"),
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
        monitor_config = load_monitor_config()
        if bool(ML_USE_GPU and monitor_config.get("use_gpu")):
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
    normalized = dict(record)
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
    cycle_started = asyncio.get_running_loop().time()
    stamp = now_slug()
    monitor_config = load_monitor_config()
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
    search_delay_seconds = float(monitor_config.get("search_delay_seconds") or MONITOR_SEARCH_DELAY_SECONDS)
    priority_feed_queries_per_cycle = int(monitor_config.get("priority_feed_queries_per_cycle") or MONITOR_PRIORITY_FEED_QUERIES_PER_CYCLE)
    start_page = max(int(collection_state.get("next_feed_page") or 1), 1)
    political_people_offset = max(int(collection_state.get("political_people_offset") or 0), 0)
    search_term_offset = max(int(collection_state.get("search_term_offset") or 0), 0)
    monitor_print(
        "Iniciando ciclo de monitoramento | "
        f"paginas={pages} | itens_por_pagina={page_size} | pagina_inicial={start_page} | "
        f"partidos={political_party_limit} | politicos={political_people_limit} | offset_politicos={political_people_offset} | "
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
    )
    raw_path = paths["raw"] / f"snapshot-{stamp}.json"
    write_json(raw_path, snapshot)

    existing_items = load_monitoring_database(paths)
    existing_keys = {item_key(item) for item in existing_items}
    feed_items = flatten_feed(snapshot)
    new_feed_items = [item for item in feed_items if item_key(item) not in existing_keys]
    snapshot_public_records = flatten_public_records(snapshot)
    cached_public_records = load_public_records_database(paths, limit=2000)
    connector_records = merge_public_record_rows([*cached_public_records, *snapshot_public_records], limit=5000)

    analysis = analyze_items(snapshot, new_feed_items, connector_records)
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
    analysis["alerts"] = cached_alerts
    analysis["alerts_count"] = len(cached_alerts)
    library_records = build_library_records(snapshot, new_feed_items, analysis["public_records"])
    library_status = append_platform_library(paths, library_records)
    feed_items_collected = sum(
        len(page.get("items") or [])
        for page in snapshot.get("feed_pages", [])
        if isinstance(page, dict)
    )
    priority_feed_items_collected = sum(
        int(connector.get("record_count") or 0)
        for connector in snapshot.get("connectors", [])
        if connector.get("kind") == "priority_high_risk_feed"
    )
    feed_pages_failed = sum(
        1
        for connector in snapshot.get("connectors", [])
        if "_feed_page_" in str(connector.get("name") or "")
        and connector.get("status") != "ok"
    )
    next_feed_page = start_page + pages
    reset_reason = None
    if feed_items_collected == 0:
        if feed_pages_failed > 0:
            next_feed_page = start_page
            reset_reason = "feed publico falhou neste ciclo; mantendo pagina para nova tentativa"
        else:
            next_feed_page = start_page + pages
            reset_reason = "feed publico sem itens neste ciclo; avancando janela para nao ficar preso na pagina 1"
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
        "priority_feed_items_collected": priority_feed_items_collected,
        "priority_feed_queries": snapshot.get("priority_feed_queries", []),
        "new_items_analyzed": len(new_feed_items),
        "items_cached": len(existing_items),
        "feed_pages_failed": feed_pages_failed,
        "reset_reason": reset_reason,
        "political_people_offset": political_people_offset,
        "next_political_people_offset": next_political_people_offset,
        "search_term_offset": search_term_offset,
        "next_search_term_offset": next_search_term_offset,
    }
    model_training_analysis = {**analysis, "model_training_items": accumulated_items[:500]}
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
        f"feed_items={feed_items_collected} new_items={len(new_feed_items)} cached={len(existing_items)} "
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
        f"itens_feed={feed_items_collected} | novos={len(new_feed_items)} | "
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
