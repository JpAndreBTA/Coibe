import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  Activity,
  AlertCircle,
  AlertTriangle,
  ArrowDownWideNarrow,
  ChevronRight,
  Download,
  ExternalLink,
  FileText,
  Filter,
  Github,
  Loader2,
  MapPin,
  RefreshCw,
  Search,
  ShieldCheck,
  Target,
  User,
  ZoomIn,
  ZoomOut
} from 'lucide-react';

const configuredApiBases = [
  import.meta.env.VITE_API_BASE_URL || '',
  import.meta.env.VITE_API_BASES || '',
  typeof window !== 'undefined' ? window.localStorage?.getItem('coibeApiBaseUrl') || '' : ''
].join(',')
  .split(',')
  .map((base) => base.trim().replace(/\/$/, ''))
  .filter(Boolean);

const PUBLIC_API_BASE = 'https://api.coibe.com.br';
const isLocalFrontend = typeof window !== 'undefined'
  && ['localhost', '127.0.0.1', '0.0.0.0'].includes(window.location.hostname);
const fallbackApiBases = isLocalFrontend
  ? ['', 'http://127.0.0.1:8000', 'http://127.0.0.1:8001', PUBLIC_API_BASE]
  : [PUBLIC_API_BASE, '', 'http://127.0.0.1:8000', 'http://127.0.0.1:8001'];
const API_BASES = configuredApiBases.length
  ? configuredApiBases
  : fallbackApiBases;
const API_REQUEST_TIMEOUT_MS = 10000;
const API_CACHE_PREFIX = 'coibe-api-cache:';
const preferredApiBaseKey = 'coibePreferredApiBase';
const apiMemoryCache = new Map();
const apiPendingGets = new Map();
let preferredApiBase = typeof window !== 'undefined' ? window.sessionStorage?.getItem(preferredApiBaseKey) || '' : '';
const COMPRAS_CONTRATOS_URL = 'https://dadosabertos.compras.gov.br/modulo-contratos/1_consultarContratos';
const COMPRAS_PUBLIC_PORTAL_URL = 'https://www.gov.br/compras/pt-br';
const BRASILIA_TIME_ZONE = 'America/Sao_Paulo';

const IBGE_CODE_TO_UF = {
  11: 'RO', 12: 'AC', 13: 'AM', 14: 'RR', 15: 'PA', 16: 'AP', 17: 'TO',
  21: 'MA', 22: 'PI', 23: 'CE', 24: 'RN', 25: 'PB', 26: 'PE', 27: 'AL', 28: 'SE', 29: 'BA',
  31: 'MG', 32: 'ES', 33: 'RJ', 35: 'SP',
  41: 'PR', 42: 'SC', 43: 'RS',
  50: 'MS', 51: 'MT', 52: 'GO', 53: 'DF'
};

const STATE_NAME_TO_UF = {
  acre: 'AC', alagoas: 'AL', amapa: 'AP', amazonas: 'AM', bahia: 'BA', ceara: 'CE',
  'distrito federal': 'DF', 'espirito santo': 'ES', goias: 'GO', maranhao: 'MA',
  'mato grosso': 'MT', 'mato grosso do sul': 'MS', 'minas gerais': 'MG', para: 'PA',
  paraiba: 'PB', parana: 'PR', pernambuco: 'PE', piaui: 'PI', 'rio de janeiro': 'RJ',
  'rio grande do norte': 'RN', 'rio grande do sul': 'RS', rondonia: 'RO', roraima: 'RR',
  'santa catarina': 'SC', 'sao paulo': 'SP', sergipe: 'SE', tocantins: 'TO',
  ac: 'AC', al: 'AL', ap: 'AP', am: 'AM', ba: 'BA', ce: 'CE', df: 'DF', es: 'ES',
  go: 'GO', ma: 'MA', mt: 'MT', ms: 'MS', mg: 'MG', pa: 'PA', pb: 'PB', pr: 'PR',
  pe: 'PE', pi: 'PI', rj: 'RJ', rn: 'RN', rs: 'RS', ro: 'RO', rr: 'RR', sc: 'SC',
  sp: 'SP', se: 'SE', to: 'TO'
};

const riskCopy = {
  alto: { label: 'Atenção Alta', color: 'text-red-300 border-red-500 bg-red-500/10', panel: 'bg-red-950/40' },
  médio: { label: 'Atenção Média', color: 'text-amber-300 border-amber-500 bg-amber-500/10', panel: 'bg-amber-950/40' },
  baixo: { label: 'Atenção Baixa', color: 'text-neutral-300 border-neutral-700 bg-neutral-800', panel: 'bg-neutral-800/70' },
  indeterminado: { label: 'Análise Pendente', color: 'text-neutral-300 border-neutral-700 bg-neutral-800', panel: 'bg-neutral-800/70' }
};

riskCopy.medio = Object.values(riskCopy).find((entry) => entry.color.includes('amber')) || riskCopy.baixo;

const POLITICAL_TYPE_LABELS = {
  todos: 'Todos',
  viagem: 'Viagem/deslocamento',
  despesas: 'Despesas',
  comunicacao: 'Comunicação/conteúdo',
  servicos: 'Serviços/consultoria',
  estrutura: 'Estrutura/gabinete',
  processos: 'Processos legais',
  contas: 'Contas eleitorais',
  controle: 'Controle externo',
  contratos: 'Contratos/compras',
  doacoes: 'Doações eleitorais',
  vinculos: 'Vínculos próximos',
  prioridade: 'Prioridade pública',
  partido: 'Partido',
  outros: 'Outros'
};

const POLITICAL_DETAIL_PAGE_SIZE = 6;

function politicalTypeLabel(value) {
  return POLITICAL_TYPE_LABELS[value] || String(value || 'Outros');
}

const SEARCH_TYPE_ORDER = {
  politico_relacionado: 10,
  politico_deputado: 11,
  politico_senador: 12,
  partido_politico: 13,
  estado: 20,
  municipio: 21,
  cnpj: 40,
  risco_superfaturamento: 55,
  contrato: 60,
  monitoring_item: 61,
  universal_search_result: 70,
  stf_processo: 80,
  stf_jurisprudencia: 81
};

function searchResultRank(result) {
  return SEARCH_TYPE_ORDER[result?.type] || 90;
}

function mergeSearchResults(...lists) {
  const seen = new Set();
  return lists.flat().filter((result) => {
    const key = `${result.type}:${result.title}:${result.subtitle || ''}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  }).sort((left, right) => {
    const order = searchResultRank(left) - searchResultRank(right);
    if (order !== 0) return order;
    return String(left.title || '').localeCompare(String(right.title || ''), 'pt-BR');
  });
}

async function fetchWithTimeout(url, options = {}) {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), API_REQUEST_TIMEOUT_MS);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    window.clearTimeout(timeout);
  }
}

function orderedApiBases() {
  const bases = preferredApiBase
    ? [preferredApiBase, ...API_BASES.filter((base) => base !== preferredApiBase)]
    : API_BASES;
  return [...new Set(bases)];
}

function rememberApiBase(base) {
  preferredApiBase = base;
  try {
    window.sessionStorage?.setItem(preferredApiBaseKey, base);
  } catch {
    // Session storage can be blocked; the active tab still keeps the base in memory.
  }
}

function apiCacheTtl(path) {
  if (path.startsWith('/api/public-data/ibge/states-geojson')) return 24 * 60 * 60 * 1000;
  if (path.startsWith('/api/monitoring/state-map') || path.startsWith('/api/monitoring/map')) return 60 * 1000;
  if (path.startsWith('/api/monitoring/status')) return 10 * 1000;
  if (path.startsWith('/api/monitoring/feed')) return 12 * 1000;
  if (path.startsWith('/api/political/')) return 45 * 1000;
  if (path.startsWith('/api/search/index') || path.startsWith('/api/search/autocomplete')) return 90 * 1000;
  if (path.startsWith('/api/search')) return 5 * 60 * 1000;
  return 15 * 1000;
}

function readCachedApi(path, allowStale = false) {
  const now = Date.now();
  const memoryEntry = apiMemoryCache.get(path);
  if (memoryEntry && (allowStale || memoryEntry.expiresAt > now)) return memoryEntry.data;

  try {
    const raw = window.sessionStorage?.getItem(`${API_CACHE_PREFIX}${path}`);
    if (!raw) return null;
    const entry = JSON.parse(raw);
    if (entry && (allowStale || Number(entry.expiresAt || 0) > now)) {
      apiMemoryCache.set(path, entry);
      return entry.data;
    }
  } catch {
    return null;
  }
  return null;
}

function writeCachedApi(path, data, ttlMs) {
  const entry = {
    data,
    expiresAt: Date.now() + ttlMs,
    cachedAt: Date.now()
  };
  apiMemoryCache.set(path, entry);
  try {
    window.sessionStorage?.setItem(`${API_CACHE_PREFIX}${path}`, JSON.stringify(entry));
  } catch {
    // Large responses can exceed storage quota; memory cache still helps this session.
  }
}

async function apiGet(path, options = {}) {
  const ttlMs = options.cacheTtlMs ?? apiCacheTtl(path);
  if (!options.force) {
    const cached = readCachedApi(path);
    if (cached) return cached;
    const pending = apiPendingGets.get(path);
    if (pending) return pending;
  }

  const request = (async () => {
    let lastError;
    for (const base of orderedApiBases()) {
      try {
        const response = await fetchWithTimeout(`${base}${path}`, { cache: 'no-store' });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const data = await response.json();
        rememberApiBase(base);
        writeCachedApi(path, data, ttlMs);
        return data;
      } catch (error) {
        lastError = error;
      }
    }
    const stale = readCachedApi(path, true);
    if (stale) return { ...stale, coibe_stale_cache: true };
    throw lastError || new Error('Backend indisponivel');
  })();

  apiPendingGets.set(path, request);
  try {
    return await request;
  } finally {
    apiPendingGets.delete(path);
  }
}

async function apiPost(path) {
  let requestError;
  for (const base of orderedApiBases()) {
    try {
      const response = await fetchWithTimeout(`${base}${path}`, { method: 'POST' });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      rememberApiBase(base);
      return response.json();
    } catch (error) {
      requestError = error;
    }
  }
  throw requestError || new Error('Backend indisponivel');
}

function normalizeSearchText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
    .toLowerCase();
}

function clampNumber(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function ufFromSearchText(value) {
  return STATE_NAME_TO_UF[normalizeSearchText(value)] || '';
}

function formatDate(value) {
  if (!value) return 'DATA NÃO INFORMADA';
  const rawValue = String(value);
  const parsedDate = /^\d{4}-\d{2}-\d{2}$/.test(rawValue)
    ? new Date(`${rawValue}T00:00:00Z`)
    : new Date(rawValue);
  if (Number.isNaN(parsedDate.getTime())) return 'DATA NÃO INFORMADA';
  return parsedDate.toLocaleDateString('pt-BR', {
    timeZone: BRASILIA_TIME_ZONE,
    day: '2-digit',
    month: 'short',
    year: 'numeric'
  }).replace('.', '').toUpperCase();
}

const mapBounds = { minLng: -74, maxLng: -34, minLat: -34, maxLat: 6, width: 760, height: 720 };

function projectPoint([lng, lat]) {
  const x = ((lng - mapBounds.minLng) / (mapBounds.maxLng - mapBounds.minLng)) * mapBounds.width;
  const y = (1 - (lat - mapBounds.minLat) / (mapBounds.maxLat - mapBounds.minLat)) * mapBounds.height;
  return [x, y];
}

function clampMapPan(pan, zoom) {
  const maxX = zoom <= 1 ? 0 : (mapBounds.width * (zoom - 1)) / 2 + 24;
  const maxY = zoom <= 1 ? 0 : (mapBounds.height * (zoom - 1)) / 2 + 24;
  return {
    x: clampNumber(pan.x, -maxX, maxX),
    y: clampNumber(pan.y, -maxY, maxY)
  };
}

function mapClientPoint(clientX, clientY, node) {
  if (!node) return { x: mapBounds.width / 2, y: mapBounds.height / 2 };
  const rect = node.getBoundingClientRect();
  if (!rect.width || !rect.height) return { x: mapBounds.width / 2, y: mapBounds.height / 2 };
  return {
    x: ((clientX - rect.left) / rect.width) * mapBounds.width,
    y: ((clientY - rect.top) / rect.height) * mapBounds.height
  };
}

function ringToPath(ring) {
  return ring.map((coord, index) => {
    const [x, y] = projectPoint(coord);
    return `${index === 0 ? 'M' : 'L'}${x.toFixed(2)},${y.toFixed(2)}`;
  }).join(' ') + ' Z';
}

function geometryToPath(geometry) {
  if (!geometry) return '';
  if (geometry.type === 'Polygon') return geometry.coordinates.map(ringToPath).join(' ');
  if (geometry.type === 'MultiPolygon') return geometry.coordinates.flatMap((polygon) => polygon.map(ringToPath)).join(' ');
  return '';
}

function ringAreaAndCentroid(ring) {
  let area = 0;
  let cx = 0;
  let cy = 0;
  for (let index = 0; index < ring.length; index += 1) {
    const [x1, y1] = projectPoint(ring[index]);
    const [x2, y2] = projectPoint(ring[(index + 1) % ring.length]);
    const cross = x1 * y2 - x2 * y1;
    area += cross;
    cx += (x1 + x2) * cross;
    cy += (y1 + y2) * cross;
  }
  area /= 2;
  if (!area) return null;
  return { area, x: cx / (6 * area), y: cy / (6 * area) };
}

function geometryLabelPoint(geometry) {
  if (!geometry) return null;
  const rings = geometry.type === 'Polygon'
    ? geometry.coordinates
    : geometry.type === 'MultiPolygon'
      ? geometry.coordinates.flat()
      : [];

  let best = null;
  for (const ring of rings) {
    const centroid = ringAreaAndCentroid(ring);
    if (!centroid) continue;
    if (!best || Math.abs(centroid.area) > Math.abs(best.area)) {
      best = centroid;
    }
  }
  return best ? { x: best.x, y: best.y } : null;
}

function geometryProjectedBounds(geometry) {
  if (!geometry) return null;
  const rings = geometry.type === 'Polygon'
    ? geometry.coordinates
    : geometry.type === 'MultiPolygon'
      ? geometry.coordinates.flat()
      : [];
  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;
  for (const ring of rings) {
    for (const coord of ring) {
      const [x, y] = projectPoint(coord);
      minX = Math.min(minX, x);
      minY = Math.min(minY, y);
      maxX = Math.max(maxX, x);
      maxY = Math.max(maxY, y);
    }
  }
  if (![minX, minY, maxX, maxY].every(Number.isFinite)) return null;
  return { minX, minY, maxX, maxY };
}

function offsetOverlappingMapPoints(points, boundsByUf) {
  const prepared = points.map((point, index) => {
    const lng = Number(point.lng);
    const lat = Number(point.lat);
    const [baseX, baseY] = projectPoint([lng, lat]);
    return {
      ...point,
      displayKey: `${point.uf || 'BR'}:${baseX.toFixed(1)}:${baseY.toFixed(1)}`,
      originalIndex: index,
      displayX: baseX,
      displayY: baseY
    };
  });

  const groups = new Map();
  for (const point of prepared) {
    const group = groups.get(point.displayKey) || [];
    group.push(point);
    groups.set(point.displayKey, group);
  }

  for (const group of groups.values()) {
    if (group.length <= 1) continue;
    const total = group.length;
    group.forEach((point, index) => {
      const bounds = boundsByUf[point.uf] || {
        minX: 0,
        minY: 0,
        maxX: mapBounds.width,
        maxY: mapBounds.height
      };
      const safeWidth = Math.max(bounds.maxX - bounds.minX, 1);
      const safeHeight = Math.max(bounds.maxY - bounds.minY, 1);
      const maxRadius = Math.max(10, Math.min(95, safeWidth * 0.42, safeHeight * 0.42));
      const radius = Math.min(maxRadius, 7 + Math.sqrt(index + 1) * (total > 18 ? 8 : 6));
      const angle = index * 2.399963229728653;
      point.displayX = clampNumber(point.displayX + Math.cos(angle) * radius, bounds.minX + 5, bounds.maxX - 5);
      point.displayY = clampNumber(point.displayY + Math.sin(angle) * radius, bounds.minY + 5, bounds.maxY - 5);
    });
  }

  return prepared.sort((left, right) => left.originalIndex - right.originalIndex);
}

function stateFill(score, selected) {
  if (selected) return '#ef4444';
  if (score >= 40) return '#b91c1c';
  if (score >= 20) return '#ea580c';
  if (score > 0) return '#ca8a04';
  return '#262626';
}

function downloadReport(alert) {
  if (!alert) return;
  const normalizedAlert = {
    ...alert,
    report: {
      ...alert.report,
      official_sources: officialSourcesForAlert(alert)
    }
  };
  const blob = new Blob([JSON.stringify(normalizedAlert.report, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = `${normalizedAlert.report.id}.json`;
  anchor.click();
  URL.revokeObjectURL(url);
}

function comprasUrlForAlert(alert) {
  const params = new URLSearchParams({ pagina: '1', tamanhoPagina: '10' });
  if (alert?.id) params.set('idCompra', String(alert.id));
  const supplierDigits = String(alert?.supplier_cnpj || '').replace(/\D/g, '');
  if (supplierDigits) params.set('niFornecedor', supplierDigits);
  if (alert?.date) {
    params.set('dataVigenciaInicialMin', alert.date);
    params.set('dataVigenciaInicialMax', alert.date);
  }
  return `${COMPRAS_CONTRATOS_URL}?${params}`;
}

function normalizeOfficialSource(alert, source, index = 0) {
  const url = String(source?.url || '');
  const label = String(source?.label || '');
  const isComprasSource = url.includes('dadosabertos.compras.gov.br') || label.toLowerCase().includes('compras.gov');
  if (!isComprasSource) return source;
  return {
    ...source,
    label: `Compras.gov.br Dados Abertos - contrato ${alert?.id || ''}`.trim(),
    url: COMPRAS_PUBLIC_PORTAL_URL,
    api_url: comprasUrlForAlert(alert),
    kind: 'Portal oficial federal; a API de dados abertos pode oscilar no navegador'
  };
}

function officialSourcesForAlert(alert) {
  return (alert?.report?.official_sources || [])
    .map((source, index) => normalizeOfficialSource(alert, source, index))
    .sort((left, right) => {
      const leftUrl = String(left?.url || '').toLowerCase();
      const rightUrl = String(right?.url || '').toLowerCase();
      const leftPriority = leftUrl.includes('pncp.gov.br') ? 0 : leftUrl.includes('gov.br') ? 1 : 2;
      const rightPriority = rightUrl.includes('pncp.gov.br') ? 0 : rightUrl.includes('gov.br') ? 1 : 2;
      return leftPriority - rightPriority;
    });
}

const DETAIL_LABELS = {
  baseline: 'Média dos contratos comparáveis',
  standard_deviation: 'Variação normal da amostra',
  z_score: 'Distância estatística',
  percent_above_baseline: 'Acima da média',
  sample_size: 'Contratos comparados',
  category: 'Grupo comparado',
  supplier_cnpj: 'CNPJ do fornecedor',
  valor_global: 'Valor do contrato',
  company_opening_date: 'Abertura do CNPJ',
  contract_date: 'Data do contrato',
  cnpj_age_days: 'Idade da empresa',
  capital_social: 'Capital social',
  ratio_contract_to_capital: 'Contrato / capital social',
  supplier_location: 'Sede do fornecedor',
  agency_location: 'Local do órgão',
  distance_km: 'Distância aproximada',
  logistic_reason: 'Motivo logístico',
  cnae: 'Atividade econômica',
  related_contracts: 'Contratos relacionados',
  total_window_value: 'Valor somado no período',
  reference_limit: 'Limite de referência',
  window_days: 'Janela analisada',
  matching_records: 'Registros públicos relacionados',
  qsa_terms_checked: 'Sócios/dados cruzados',
  supplier_contracts: 'Contratos do fornecedor',
  supplier_total_value: 'Valor total do fornecedor',
  supplier_total_z_score: 'Concentração estatística',
  contracts_in_window: 'Contratos no período',
  window_total_value: 'Valor no período',
  entity: 'Órgão',
  supplier: 'Fornecedor',
  uf: 'UF',
  family: 'Grupo textual',
  modality: 'Modalidade'
};

const INTERNAL_DETAIL_KEYS = new Set([
  'model',
  'score',
  'estimated_variation',
  'rule',
  'branch_registry_available',
  'z_score',
  'supplier_total_z_score',
  'standard_deviation',
  'is_anomaly',
  'is_uncertainty_floor'
]);

const CURRENCY_KEYS = new Set([
  'baseline',
  'standard_deviation',
  'estimated_variation',
  'valor_global',
  'capital_social',
  'total_window_value',
  'reference_limit',
  'supplier_total_value',
  'window_total_value',
  'value'
]);

const PERCENT_KEYS = new Set(['percent_above_baseline']);

function readableKey(key) {
  return DETAIL_LABELS[key] || String(key || '').replaceAll('_', ' ');
}

function compactValue(value, key = '') {
  if (value === null || value === undefined || value === '') return 'n/d';
  const numericValue = Number(value);
  if (CURRENCY_KEYS.has(key) && Number.isFinite(numericValue)) {
    return numericValue.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 });
  }
  if (PERCENT_KEYS.has(key) && Number.isFinite(numericValue)) {
    return `${numericValue.toLocaleString('pt-BR', { maximumFractionDigits: 1 })}%`;
  }
  if (key === 'distance_km' && Number.isFinite(numericValue)) {
    return `${numericValue.toLocaleString('pt-BR', { maximumFractionDigits: 0 })} km`;
  }
  if (key === 'cnpj_age_days' && Number.isFinite(numericValue)) {
    return `${numericValue.toLocaleString('pt-BR', { maximumFractionDigits: 0 })} dias`;
  }
  if (key === 'window_days' && Number.isFinite(numericValue)) {
    return `${numericValue.toLocaleString('pt-BR', { maximumFractionDigits: 0 })} dias`;
  }
  if (key === 'ratio_contract_to_capital' && Number.isFinite(numericValue)) {
    return `${numericValue.toLocaleString('pt-BR', { maximumFractionDigits: 1 })}x`;
  }
  if (typeof value === 'number') return Number.isInteger(value) ? String(value) : value.toLocaleString('pt-BR', { maximumFractionDigits: 2 });
  return String(value);
}

function compactText(text = '', limit = 180) {
  const cleaned = String(text || '').replace(/\s+/g, ' ').trim();
  if (cleaned.length <= limit) return cleaned;
  return `${cleaned.slice(0, limit).trimEnd()}...`;
}

function numericValue(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

function isGenericPreventiveFlag(flag = {}) {
  const text = normalizeSearchText(`${flag.code || ''} ${flag.title || ''} ${flag.message || ''}`);
  return flag.code === 'RFBASE'
    || flag.code === 'SEARCH_CONTEXT'
    || text.includes('monitoramento preventivo')
    || text.includes('sem fator forte nos dados comparaveis');
}

function contextualAttentionFlag(flag = {}, context = {}) {
  const payload = context.payload || {};
  const evidence = flag.evidence || {};
  const normalizedFlag = {
    title: flag.title || 'Fator de atenção',
    message: compactText(flag.message || 'Conferir este ponto na fonte oficial.', 160),
    risk_level: flag.risk_level || flag.level || context.risk_level || 'baixo',
    level: flag.risk_level || flag.level || context.risk_level || 'baixo',
    evidence,
    code: flag.code || ''
  };

  if (!isGenericPreventiveFlag(flag)) return normalizedFlag;

  const value = numericValue(
    evidence.valor_global
    || evidence.value
    || payload.value
    || payload.valorGlobal
    || payload.valorInicial
    || context.value
  );
  const formattedValue = value > 0 ? compactValue(value, 'value') : '';
  const supplier = evidence.supplier
    || evidence.supplier_name
    || payload.supplier_name
    || payload.nomeRazaoSocialFornecedor
    || payload.nomeFornecedor
    || context.supplier_name
    || context.supplier
    || '';
  const entity = evidence.entity
    || payload.entity
    || payload.nomeUnidadeGestora
    || payload.nomeOrgao
    || context.entity
    || context.orgao
    || '';
  const objectText = evidence.objeto
    || payload.objeto
    || payload.objetoContrato
    || context.object
    || context.title
    || '';
  const date = context.date || payload.date || payload.dataAssinatura || payload.dataVigenciaInicial || '';
  const pieces = [];
  if (formattedValue) pieces.push(`valor ${formattedValue}`);
  if (supplier) pieces.push(`fornecedor ${supplier}`);
  if (entity) pieces.push(`órgão ${entity}`);
  if (date) pieces.push(`data ${formatDate(date)}`);

  return {
    ...normalizedFlag,
    title: 'Conferência preventiva',
    message: pieces.length > 0
      ? compactText(`Sem alerta forte. Conferir fonte oficial porque envolve ${pieces.join(', ')}. Compare objeto, valor, fornecedor e repetição.`, 170)
      : 'Sem alerta forte. Conferir fonte oficial, valor, objeto, fornecedor e repetição.',
    evidence: {
      ...evidence,
      ...(formattedValue ? { valor_global: String(value) } : {}),
      ...(supplier ? { supplier } : {}),
      ...(entity ? { entity } : {}),
      ...(objectText ? { objeto: String(objectText).slice(0, 240) } : {})
    }
  };
}

function contextualAttentionFlags(flags = [], context = {}) {
  const validFlags = Array.isArray(flags) ? flags.filter(Boolean) : [];
  const specificFlags = validFlags.filter((flag) => !isGenericPreventiveFlag(flag));
  const selectedFlags = specificFlags.length > 0
    ? specificFlags
    : validFlags.length > 0
      ? validFlags
      : [{
          code: 'SEARCH_CONTEXT',
          title: 'Conferência preventiva do registro',
          has_risk: false,
          risk_level: context.risk_level || 'baixo',
          message: 'Resultado encontrado para conferencia na fonte oficial.',
          evidence: {}
        }];
  return selectedFlags.map((flag) => contextualAttentionFlag(flag, context));
}

function politicalSuperpricingRisk(detail = {}) {
  const level = String(detail.risk_level || '').toLowerCase();
  const score = numericValue(detail.risk_score);
  const value = numericValue(detail.value);
  const isContract = detail.type === 'contratos';
  const isMovement = ['vinculos', 'doacoes'].includes(detail.type);
  const hasSupplier = Boolean(detail.supplier || detail.supplier_document);
  let label = 'Baixo';
  let color = riskCopy.baixo.color;
  let message = 'Sem sinal forte de superfaturamento neste registro.';

  if (level === 'alto' || score >= 70) {
    label = 'Alto';
    color = riskCopy.alto.color;
    message = 'Registro relacionado a contrato ou movimentação com risco alto na base analisada.';
  } else if (level === 'médio' || level === 'medio' || score >= 40 || (isContract && value >= 1000000) || (isMovement && value >= 250000)) {
    label = 'Médio';
    color = riskCopy.médio.color;
    message = 'Registro merece conferência por valor, tipo, fornecedor ou score de risco.';
  } else if (isContract || hasSupplier) {
    message = 'Contrato, fornecedor ou pagamento relacionado para conferência preventiva.';
  }

  return { label, color, message, score, value };
}

function politicalDetailReview(detail = {}) {
  const type = String(detail.type || 'outros');
  const value = numericValue(detail.value);
  const formattedValue = value > 0 ? compactValue(value, 'value') : '';
  const who = detail.person || detail.party || 'este recorte';
  const supplier = detail.supplier || detail.supplier_document || '';
  const entity = detail.entity || '';
  const matchedTerms = Array.isArray(detail.matched_terms) ? detail.matched_terms.filter(Boolean).slice(0, 4) : [];
  const readableDate = detail.date ? formatDate(detail.date) : detail.month && detail.year ? `${String(detail.month).padStart(2, '0')}/${detail.year}` : '';
  const risk = politicalSuperpricingRisk(detail);
  const valueText = formattedValue ? ` no valor de ${formattedValue}` : '';
  const supplierText = supplier ? ` envolvendo ${supplier}` : '';
  const entityText = entity ? ` no órgão ${entity}` : '';
  const dateText = readableDate ? ` em ${readableDate}` : '';
  const checksByType = {
    contratos: `Contrato ligado a ${who}${supplierText}${entityText}${valueText}${dateText}. ${matchedTerms.length ? `Termos encontrados: ${matchedTerms.join(', ')}. ` : ''}Confira objeto, preço, fornecedor e repetição.`,
    despesas: `Gasto público ligado a ${who}${supplierText}${valueText}${dateText}. Veja quem recebeu, motivo, data e documento.`,
    servicos: `Serviço/consultoria ligado a ${who}${supplierText}${valueText}. Compare entrega e preço com serviços parecidos.`,
    comunicacao: `Comunicação/divulgação ligada a ${who}${supplierText}${valueText}. Confira material entregue, fornecedor e justificativa.`,
    estrutura: `Estrutura ou apoio para ${who}${supplierText}${valueText}. Ver nota, necessidade e pagamentos repetidos.`,
    viagem: `Viagem ligada a ${who}${supplierText}${valueText}${dateText}. Confira destino, motivo, datas e documento fiscal.`,
    processos: `Processo ou fonte jurídica relacionada a ${who}. Abra a fonte e confira homônimo, classe e situação.`,
    contas: `Conta eleitoral ligada a ${who}. Veja origem, destino, fornecedor, campanha/partido e documento no TSE.`,
    doacoes: `Registro eleitoral relacionado a ${who}${supplierText}${valueText}. Veja doador, recebedor, data, valor e vínculos com contratos.`,
    vinculos: `Sinal de proximidade textual/operacional em ${who}${supplierText}${valueText}. Não prova irregularidade; guia a checagem.`,
    controle: `Fonte de controle externo ligada a ${who}. Confira órgão, processo/acórdão, data, escopo e decisão.`,
    prioridade: `${who} entrou antes na leitura por prioridade pública. Prioridade não é acusação.`
  };
  const hiddenSignals = [];
  if (value >= 1000000) hiddenSignals.push(`Valor alto: ${formattedValue}. Compare com contratos parecidos.`);
  if (supplier) hiddenSignals.push(`Fornecedor/CNPJ: confira repetição em pagamentos e contratos.`);
  if (entity) hiddenSignals.push(`Órgão relacionado: ${entity}. Compare com outros contratos do mesmo fornecedor.`);
  if (matchedTerms.length) hiddenSignals.push(`Termos que puxaram o vínculo: ${matchedTerms.join(', ')}.`);
  if (detail.matched_records) hiddenSignals.push(`${Number(detail.matched_records || 0).toLocaleString('pt-BR')} registro(s) relacionado(s) encontrados na base.`);
  if (detail.risk_score) hiddenSignals.push(`Score ${Number(detail.risk_score || 0).toLocaleString('pt-BR')}: prioridade de leitura, não prova.`);
  if (risk.label !== 'Baixo') hiddenSignals.push(`Superfaturamento: ${risk.label}. ${risk.message}`);
  if (detail.document_url) hiddenSignals.push('Documento oficial disponível.');

  return {
    title: checksByType[type] || 'Conferir fonte oficial, valor, data, pessoa/partido, fornecedor e recorrência no conjunto analisado.',
    hiddenSignals
  };
}

function searchResultReview(result = {}) {
  const payload = result.payload || {};
  const rawFlags = Array.isArray(payload.coibe_red_flags)
    ? payload.coibe_red_flags
    : Array.isArray(payload.red_flags)
      ? payload.red_flags
      : [];
  const riskFlags = rawFlags.filter((flag) => flag && !isGenericPreventiveFlag(flag));
  const preventiveFlags = rawFlags.filter((flag) => flag && isGenericPreventiveFlag(flag));
  const visibleFlags = riskFlags.length > 0 ? riskFlags : preventiveFlags;
  const value = numericValue(payload.value || payload.valorGlobal || payload.valorInicial || payload.estimated_variation);
  const formattedValue = value > 0 ? compactValue(value, 'value') : '';
  const supplier = payload.supplier_name || payload.nomeRazaoSocialFornecedor || payload.nomeFornecedor || payload.niFornecedor || payload.cnpj || '';
  const entity = payload.entity || payload.nomeUnidadeGestora || payload.nomeOrgao || payload.orgao || payload.uf || '';
  const type = String(result.type || '').replaceAll('_', ' ');
  const source = result.source || 'Fonte pública';
  const base = {
    title: 'Resultado encontrado',
    summary: `Fonte: ${source}. Confira valor, data, órgão/pessoa e repetições antes de concluir.`,
    checks: ['Abrir a fonte oficial.', 'Comparar nome, data, valor e órgão/pessoa.'],
    metrics: [],
    legacyFlags: visibleFlags.map((flag) => ({
      title: flag.title || 'Fator de atenção',
      message: flag.message || 'Ponto identificado para conferência na fonte oficial.',
      level: flag.risk_level || 'baixo',
      evidence: flag.evidence || {},
      code: flag.code || ''
    })),
    flags: contextualAttentionFlags(visibleFlags, { ...result, ...payload, payload })
  };
  if (formattedValue) base.metrics.push(['Valor citado', formattedValue]);
  if (supplier) base.metrics.push(['Fornecedor/CNPJ', supplier]);
  if (entity) base.metrics.push(['Órgão/UF', entity]);
  if (payload.coibe_risk_score) base.metrics.push(['Score COIBE', Number(payload.coibe_risk_score).toLocaleString('pt-BR')]);

  if (result.type === 'risco_superfaturamento') {
    return {
      ...base,
      title: 'Possível valor acima da média',
      summary: 'O COIBE encontrou valor, fornecedor ou padrão fora do comum. Compare com contratos parecidos.',
      checks: [
        formattedValue ? `Valor: ${formattedValue}.` : 'Ver item e valor.',
        supplier ? `Fornecedor/CNPJ: ${supplier}.` : 'Ver fornecedor/CNPJ.',
        'Abrir documento oficial.'
      ]
    };
  }
  if (result.type === 'contrato') {
    return {
      ...base,
      title: 'Contrato público encontrado',
      summary: `Contrato${formattedValue ? ` de ${formattedValue}` : ''}${supplier ? ` com ${supplier}` : ''}. Veja objeto, pagador, fornecedor e preço.`,
      checks: [
        `Objeto: ${payload.objeto || payload.objetoContrato || result.title}.`,
        entity ? `Órgão/gestor: ${entity}.` : 'Ver órgão contratante.',
        supplier ? `Fornecedor: ${supplier}.` : 'Ver fornecedor e CNPJ.',
        'Comparar com contratos semelhantes.'
      ]
    };
  }
  if (result.type?.startsWith('politico')) {
    return {
      ...base,
      title: 'Pessoa pública encontrada',
      summary: `${result.title}. Use para ver despesas, contratos, processos e fontes oficiais ligados ao nome.`,
      checks: ['Confirmar homônimos.', 'Ver partido/cargo e período.', 'Cruzar contratos, doações e despesas.']
    };
  }
  if (result.type === 'partido_politico') {
    return {
      ...base,
      title: 'Partido político encontrado',
      summary: `${result.title}. Veja valores agregados, parlamentares, contratos e contas eleitorais.`,
      checks: ['Conferir sigla e nome oficial.', 'Abrir aba de partido.', 'Comparar contratos, despesas e doações.']
    };
  }
  if (result.type === 'cnpj') {
    return {
      ...base,
      title: 'CNPJ encontrado',
      summary: 'Empresa ou entidade encontrada. Confira atividade, abertura, capital social e contratos públicos.',
      checks: ['Confirmar razão social e UF.', 'Comparar capital social e contratos.', 'Procurar contratos recentes do CNPJ.']
    };
  }
  if (result.type === 'stf_processo' || result.type === 'stf_jurisprudencia') {
    return {
      ...base,
      title: 'Consulta oficial do STF',
      summary: 'Atalho para consulta oficial do STF. O mérito deve ser lido na fonte.',
      checks: ['Abrir o portal oficial.', 'Conferir número, classe, partes e data.', 'Checar homônimos.']
    };
  }
  if (result.type === 'estado' || result.type === 'municipio') {
    return {
      ...base,
      title: 'Localidade encontrada',
      summary: 'Use para filtrar contratos, alertas e riscos da UF ou município.',
      checks: ['Aplicar filtro no feed.', 'Ver maiores contratos.', 'Abrir mapa da região.']
    };
  }
  return { ...base, title: `Resultado de busca: ${type || 'registro público'}` };
}

function evidenceNumber(evidence, key) {
  const value = evidence?.[key];
  if (value === undefined || value === null || value === '') return null;
  const numericValue = Number(value);
  return Number.isFinite(numericValue) ? numericValue : null;
}

function friendlyComparison(flag) {
  const evidence = flag.evidence || {};
  const comparisons = [];
  const percentAbove = evidenceNumber(evidence, 'percent_above_baseline');
  const sampleSize = evidenceNumber(evidence, 'sample_size');
  if (percentAbove !== null) {
    comparisons.push(`Valor ${compactValue(percentAbove, 'percent_above_baseline')} acima da média de ${sampleSize || 'vários'} contratos comparáveis.`);
  }
  const ageDays = evidenceNumber(evidence, 'cnpj_age_days');
  if (ageDays !== null) {
    comparisons.push(`Empresa aberta há ${compactValue(ageDays, 'cnpj_age_days')} na data analisada.`);
  }
  const capitalRatio = evidenceNumber(evidence, 'ratio_contract_to_capital');
  if (capitalRatio !== null) {
    comparisons.push(`Contrato equivale a ${compactValue(capitalRatio, 'ratio_contract_to_capital')} o capital social declarado.`);
  }
  const distanceKm = evidenceNumber(evidence, 'distance_km');
  if (distanceKm !== null) {
    comparisons.push(`Fornecedor a cerca de ${compactValue(distanceKm, 'distance_km')} do órgão contratante.`);
  }
  const contracts = evidenceNumber(evidence, 'related_contracts') ?? evidenceNumber(evidence, 'contracts_in_window');
  const windowValue = evidenceNumber(evidence, 'total_window_value') ?? evidenceNumber(evidence, 'window_total_value');
  if (contracts !== null && windowValue !== null) {
    comparisons.push(`${contracts.toLocaleString('pt-BR', { maximumFractionDigits: 0 })} contratos próximos somam ${compactValue(windowValue, 'total_window_value')}.`);
  }
  const matchingRecords = evidenceNumber(evidence, 'matching_records');
  if (matchingRecords !== null) {
    comparisons.push(`${matchingRecords.toLocaleString('pt-BR', { maximumFractionDigits: 0 })} registro(s) público(s) relacionado(s) a pessoa, sócio ou empresa.`);
  }
  const supplierContracts = evidenceNumber(evidence, 'supplier_contracts');
  const supplierTotal = evidenceNumber(evidence, 'supplier_total_value');
  if (supplierContracts !== null && supplierTotal !== null) {
    comparisons.push(`Histórico do fornecedor: ${supplierContracts.toLocaleString('pt-BR', { maximumFractionDigits: 0 })} contratos somando ${compactValue(supplierTotal, 'supplier_total_value')}.`);
  }
  return comparisons.slice(0, 2);
}

function alertBaselineValue(alert) {
  const sources = [];
  for (const flag of alert?.report?.red_flags || []) {
    sources.push(flag?.evidence || {});
  }
  if (alert?.ml_analysis) sources.push(alert.ml_analysis);

  for (const evidence of sources) {
    const baseline = evidenceNumber(evidence, 'baseline');
    if (baseline !== null && baseline > 0) return baseline;
  }

  const paidValue = Number(alert?.value);
  const estimatedVariation = Number(alert?.estimated_variation);
  if (Number.isFinite(paidValue) && Number.isFinite(estimatedVariation) && estimatedVariation > 0 && paidValue > estimatedVariation) {
    return paidValue - estimatedVariation;
  }

  return null;
}

function alertComparisonMetrics(alert) {
  if (!alert) return [];
  const paidValue = numericValue(alert.value);
  const variation = numericValue(alert.estimated_variation);
  const baseline = alertBaselineValue(alert);
  const percent = baseline && baseline > 0 && paidValue > 0
    ? ((paidValue - baseline) / baseline) * 100
    : null;
  const metrics = [];

  metrics.push(['Pago/contratado', paidValue > 0 ? compactValue(paidValue, 'value') : alert.formatted_value || 'n/d']);
  metrics.push(['Média comparável', baseline !== null ? compactValue(baseline, 'baseline') : 'Sem média confiável']);
  metrics.push(['Acima da média', variation > 0 ? compactValue(variation, 'estimated_variation') : alert.formatted_variation || 'n/d']);
  if (percent !== null && Number.isFinite(percent)) metrics.push(['Percentual acima', compactValue(percent, 'percent_above_baseline')]);

  const firstEvidence = (alert.report?.red_flags || []).map((flag) => flag?.evidence || {}).find((evidence) => evidence.sample_size || evidence.category);
  if (firstEvidence?.sample_size) metrics.push(['Contratos comparados', compactValue(firstEvidence.sample_size, 'sample_size')]);
  if (firstEvidence?.category) metrics.push(['Grupo comparado', compactText(firstEvidence.category, 80)]);

  return metrics;
}

function alertAnalyticDescription(alert, flags = []) {
  if (!alert) return '';
  const paidValue = numericValue(alert.value);
  const variation = numericValue(alert.estimated_variation);
  const baseline = alertBaselineValue(alert);
  const objectText = alert.object || alert.title || 'objeto nao informado na base carregada';
  const entity = alert.entity || alert.report?.entity || 'orgao nao informado';
  const supplier = alert.supplier_name || alert.supplier || alert.report?.supplier || '';
  const location = alert.location || [alert.city, alert.uf].filter(Boolean).join(' - ');
  const readableDate = alert.date ? formatDate(alert.date) : '';
  const firstFlag = flags[0];
  const sentences = [
    `O registro analisado trata de ${compactText(objectText, 260)}.`,
    `Orgao/unidade responsavel: ${entity}.`
  ];

  if (supplier) sentences.push(`Empresa, fornecedor ou pessoa relacionada: ${supplier}.`);
  if (paidValue > 0) sentences.push(`Valor pago ou contratado: ${compactValue(paidValue, 'value')}.`);
  if (baseline !== null) sentences.push(`Media comparavel encontrada pela plataforma: ${compactValue(baseline, 'baseline')}.`);
  if (variation > 0) sentences.push(`Possivel valor acima da media: ${compactValue(variation, 'estimated_variation')}.`);
  if (location) sentences.push(`Localidade ligada ao registro: ${location}.`);
  if (readableDate) sentences.push(`Data do conteudo: ${readableDate}.`);
  if (firstFlag?.message) sentences.push(`Motivo da atencao: ${firstFlag.message}`);

  return sentences.join(' ');
}

function searchAnalyticDescription(result, review) {
  const payload = result?.payload || {};
  const value = numericValue(payload.value || payload.valorGlobal || payload.valorInicial || payload.estimated_variation);
  const supplier = payload.supplier_name || payload.nomeRazaoSocialFornecedor || payload.nomeFornecedor || payload.niFornecedor || payload.cnpj || '';
  const entity = payload.entity || payload.nomeUnidadeGestora || payload.nomeOrgao || payload.orgao || payload.uf || '';
  const objectText = payload.objeto || payload.objetoContrato || result?.title || '';
  const person = payload.nomeCivil || payload.nome || payload.name || '';
  const pieces = [review?.summary || 'Resultado encontrado para conferencia em fonte publica.'];

  if (objectText) pieces.push(`Conteudo/objeto: ${compactText(objectText, 220)}.`);
  if (person && person !== result?.title) pieces.push(`Pessoa relacionada: ${person}.`);
  if (entity) pieces.push(`Orgao/local responsavel: ${entity}.`);
  if (supplier) pieces.push(`Empresa, fornecedor ou CNPJ: ${supplier}.`);
  if (value > 0) pieces.push(`Valor identificado: ${compactValue(value, 'value')}.`);
  if (review?.flags?.[0]?.message) pieces.push(`Motivo da atencao: ${review.flags[0].message}`);

  return pieces.join(' ');
}

function flagDetails(flag) {
  const evidence = Object.entries(flag.evidence || {});
  return evidence
    .filter(([key, value]) => !INTERNAL_DETAIL_KEYS.has(key) && value !== undefined && value !== null && value !== '')
    .slice(0, 6);
}

function mergePoliticalItems(currentItems, nextItems) {
  const seen = new Set();
  return [...currentItems, ...nextItems].filter((item) => {
    const key = `${item.type || 'item'}:${item.id || item.name}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

export default function CoibeApp() {
  const [searchTerm, setSearchTerm] = useState('');
  const [feedRiskFilter, setFeedRiskFilter] = useState('todos');
  const [feedSizeOrder, setFeedSizeOrder] = useState('data');
  const [feedDateFrom, setFeedDateFrom] = useState('');
  const [feedDateTo, setFeedDateTo] = useState('');
  const [activeTab, setActiveTab] = useState('feed');
  const [feedQuery, setFeedQuery] = useState('');
  const [feedCityFilter, setFeedCityFilter] = useState('');
  const [feedCityDraft, setFeedCityDraft] = useState('');
  const [activeSearchFilter, setActiveSearchFilter] = useState(null);
  const [items, setItems] = useState([]);
  const [mapPoints, setMapPoints] = useState([]);
  const [mapQuery, setMapQuery] = useState('');
  const [mapZoom, setMapZoom] = useState(1);
  const [mapPan, setMapPan] = useState({ x: 0, y: 0 });
  const [mapCacheStatus, setMapCacheStatus] = useState('');
  const [stateRisks, setStateRisks] = useState({});
  const [politicalParties, setPoliticalParties] = useState([]);
  const [politicalPeople, setPoliticalPeople] = useState([]);
  const [politicalSearch, setPoliticalSearch] = useState('');
  const [politicalRiskFilter, setPoliticalRiskFilter] = useState('todos');
  const [politicalSizeOrder, setPoliticalSizeOrder] = useState('prioridade');
  const [politicalTypeFilter, setPoliticalTypeFilter] = useState('todos');
  const [loadingPolitical, setLoadingPolitical] = useState(false);
  const [politicalPagination, setPoliticalPagination] = useState({
    parties: { page: 1, hasMore: false },
    politicians: { page: 1, hasMore: false }
  });
  const [selectedPoliticalItem, setSelectedPoliticalItem] = useState(null);
  const [politicalDetailPage, setPoliticalDetailPage] = useState(1);
  const [selectedPoliticalDetailIndex, setSelectedPoliticalDetailIndex] = useState(0);
  const [politicalDetailRiskFilter, setPoliticalDetailRiskFilter] = useState('todos');
  const [politicalDetailSearch, setPoliticalDetailSearch] = useState('');
  const [geoJson, setGeoJson] = useState(null);
  const [selectedState, setSelectedState] = useState(null);
  const [selectedUf, setSelectedUf] = useState('');
  const [selectedAlert, setSelectedAlert] = useState(null);
  const [showFullAlertTitle, setShowFullAlertTitle] = useState(false);
  const [selectedSearchResult, setSelectedSearchResult] = useState(null);
  const [page, setPage] = useState(1);
  const [hasMore, setHasMore] = useState(false);
  const [loadingFeed, setLoadingFeed] = useState(false);
  const [loadingMap, setLoadingMap] = useState(false);
  const [loadingSearch, setLoadingSearch] = useState(false);
  const [searchResults, setSearchResults] = useState([]);
  const [monitorStatus, setMonitorStatus] = useState(null);
  const [error, setError] = useState('');
  const loadMoreRef = useRef(null);
  const loadMorePoliticalRef = useRef(null);
  const suppressSearchEffectRef = useRef(false);
  const searchRequestIdRef = useRef(0);
  const mapPointerRef = useRef(null);
  const mapViewportRef = useRef(null);
  const mapSvgRef = useRef(null);
  const mapClickSuppressRef = useRef(false);
  const loadedPoliticalTabsRef = useRef({ parties: false, politicians: false });
  const politicalDataStampRef = useRef({ parties: '', politicians: '' });

  const analyzedCount = Math.max(
    Number(monitorStatus?.items_analyzed || 0),
    Number(monitorStatus?.database_items_count || 0) + Number(monitorStatus?.public_records_count || 0)
  );
  const libraryCount = Number(monitorStatus?.library_records_count || 0);
  const collectorState = monitorStatus?.collector_state || {};
  const feedItemsCollected = Number(collectorState.feed_items_collected || 0);
  const newItemsAnalyzed = Number(collectorState.new_items_analyzed || 0);
  const feedPagesFailed = Number(collectorState.feed_pages_failed || 0);
  const statusUpdatedAt = monitorStatus?.generated_at
    ? new Date(monitorStatus.generated_at).toLocaleString('pt-BR', { timeZone: BRASILIA_TIME_ZONE })
    : null;
  const analyzedCountNote = feedPagesFailed > 0 && feedItemsCollected === 0
    ? 'Fonte pública instável no último ciclo'
    : `Novos no último ciclo: ${newItemsAnalyzed.toLocaleString('pt-BR')}`;

  const politicalCurrentItems = activeTab === 'parties' ? politicalParties : politicalPeople;
  const currentPoliticalPagination = politicalPagination[activeTab] || { page: 1, hasMore: false };
  const filteredPoliticalItems = useMemo(() => {
    const query = politicalSearch.trim().toLowerCase();
    const riskOrder = { alto: 3, 'médio': 2, medio: 2, baixo: 1 };
    return [...politicalCurrentItems]
      .filter((item) => {
        const level = String(item.attention_level || '').toLowerCase();
        if (politicalRiskFilter !== 'todos' && level !== politicalRiskFilter) return false;
        if (
          politicalTypeFilter !== 'todos'
          && !(item.analysis_types || []).includes(politicalTypeFilter)
          && !(item.analysis_details || []).some((detail) => detail.type === politicalTypeFilter)
        ) return false;
        if (!query) return true;
        const detailText = (item.analysis_details || []).map((detail) => [detail.title, detail.description, detail.supplier, detail.person].join(' ')).join(' ');
        const text = [item.name, item.subtitle, item.party, item.role, item.summary, detailText, ...(item.people || [])].join(' ').toLowerCase();
        return text.includes(query);
      })
      .sort((left, right) => {
        if (politicalSizeOrder === 'prioridade') {
          return (Number(right.priority_score || 0) - Number(left.priority_score || 0))
            || ((riskOrder[String(right.attention_level || '').toLowerCase()] || 0) - (riskOrder[String(left.attention_level || '').toLowerCase()] || 0))
            || (Number(right.total_public_money || 0) - Number(left.total_public_money || 0));
        }
        if (politicalSizeOrder === 'risco') {
          return (riskOrder[String(right.attention_level || '').toLowerCase()] || 0) - (riskOrder[String(left.attention_level || '').toLowerCase()] || 0);
        }
        if (politicalSizeOrder === 'viagens') return Number(right.travel_public_money || 0) - Number(left.travel_public_money || 0);
        if (politicalSizeOrder === 'registros') {
          return (Number(right.priority_score || 0) - Number(left.priority_score || 0))
            || (Number(right.records_count || 0) - Number(left.records_count || 0));
        }
        return Number(right.total_public_money || 0) - Number(left.total_public_money || 0);
      });
  }, [politicalCurrentItems, politicalRiskFilter, politicalSearch, politicalSizeOrder, politicalTypeFilter]);

  const stats = useMemo(() => {
    const feedTotal = items.reduce((sum, item) => sum + Number(item.value || 0), 0);
    const feedVariation = items.reduce((sum, item) => sum + Number(item.estimated_variation || 0), 0);
    const feedHigh = items.filter((item) => item.risk_level === 'alto').length;
    const feedEntities = new Set(items.map((item) => item.entity).filter(Boolean)).size;
    const total = Math.max(Number(monitorStatus?.total_value || 0), feedTotal);
    const variation = Math.max(Number(monitorStatus?.estimated_variation_total || 0), feedVariation);
    const high = Math.max(Number(monitorStatus?.high_alerts_count || 0), feedHigh);
    const entities = Math.max(Number(monitorStatus?.monitored_entities_count || 0), feedEntities);
    const alerts = Math.max(Number(monitorStatus?.alerts_count || 0), items.length);

    return [
      {
        label: 'Volume Total Analisado',
        value: total.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 }),
        note: 'COIBE.IA - fontes públicas integradas',
        icon: ShieldCheck,
        emphasis: 'text-white'
      },
      {
        label: 'Risco de Superfaturamento',
        value: variation.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 }),
        note: 'Modelo COIBE + revisão humana',
        icon: AlertTriangle,
        emphasis: 'text-red-500',
        accent: true
      },
      {
        label: 'Alertas da Plataforma',
        value: String(alerts),
        note: `${high} alertas altos`,
        icon: Activity,
        emphasis: 'text-white'
      },
      {
        label: 'Entidades Monitoradas',
        value: String(entities),
        note: 'Órgãos e unidades gestoras',
        icon: MapPin,
        emphasis: 'text-white'
      }
    ];
  }, [items, monitorStatus]);

  const selectedPoliticalDetails = selectedPoliticalItem?.analysis_details || [];
  const filteredSelectedPoliticalDetails = useMemo(() => {
    const query = normalizeSearchText(politicalDetailSearch);
    return selectedPoliticalDetails.filter((detail) => {
      const risk = normalizeSearchText(politicalSuperpricingRisk(detail).label);
      if (politicalDetailRiskFilter !== 'todos' && risk !== normalizeSearchText(politicalDetailRiskFilter)) return false;
      if (!query) return true;
      const text = normalizeSearchText([
        detail.title,
        detail.description,
        detail.type,
        detail.person,
        detail.party,
        detail.supplier,
        detail.supplier_document,
        detail.entity,
        detail.source,
        detail.value
      ].join(' '));
      return text.includes(query);
    });
  }, [selectedPoliticalDetails, politicalDetailRiskFilter, politicalDetailSearch]);
  const selectedPoliticalDetailPages = Math.max(1, Math.ceil(filteredSelectedPoliticalDetails.length / POLITICAL_DETAIL_PAGE_SIZE));
  const selectedPoliticalDetailPage = Math.min(politicalDetailPage, selectedPoliticalDetailPages);
  const pagedPoliticalDetails = filteredSelectedPoliticalDetails.slice(
    (selectedPoliticalDetailPage - 1) * POLITICAL_DETAIL_PAGE_SIZE,
    selectedPoliticalDetailPage * POLITICAL_DETAIL_PAGE_SIZE
  );
  const selectedPoliticalDetail = filteredSelectedPoliticalDetails[selectedPoliticalDetailIndex] || filteredSelectedPoliticalDetails[0] || null;
  const selectedPoliticalDetailRisk = selectedPoliticalDetail ? politicalSuperpricingRisk(selectedPoliticalDetail) : null;
  const selectedPoliticalDetailReview = selectedPoliticalDetail ? politicalDetailReview(selectedPoliticalDetail) : null;
  const selectedPoliticalMetrics = useMemo(() => {
    if (!selectedPoliticalItem) return null;
    const details = selectedPoliticalItem.analysis_details || [];
    const risks = selectedPoliticalItem.risks || [];
    const totalDetailValue = details.reduce((sum, detail) => sum + numericValue(detail.value), 0);
    const contractDetails = details.filter((detail) => detail.type === 'contratos');
    const paymentDetails = details.filter((detail) => ['despesas', 'servicos', 'estrutura', 'comunicacao', 'outros'].includes(detail.type));
    const travelDetails = details.filter((detail) => detail.type === 'viagem');
    const riskMovementDetails = details.filter((detail) => ['vinculos', 'doacoes', 'contratos'].includes(detail.type));
    const superpricingDetails = details
      .map((detail) => ({ detail, risk: politicalSuperpricingRisk(detail) }))
      .filter(({ risk, detail }) => risk.label !== 'Baixo' || detail.type === 'contratos');
    const highRiskCount = risks.filter((risk) => ['alto', 'médio', 'medio'].includes(String(risk.level || '').toLowerCase())).length;
    const contractValue = contractDetails.reduce((sum, detail) => sum + numericValue(detail.value), 0);
    const paymentValue = paymentDetails.reduce((sum, detail) => sum + numericValue(detail.value), 0);
    const riskMovementValue = riskMovementDetails.reduce((sum, detail) => sum + numericValue(detail.value), 0);
    const topDetail = [...details].sort((left, right) => numericValue(right.value) - numericValue(left.value))[0];
    const nonTravelMoney = Math.max(Number(selectedPoliticalItem.total_public_money || 0) - Number(selectedPoliticalItem.travel_public_money || 0), 0);

    return {
      totalDetailValue,
      nonTravelMoney,
      contractCount: contractDetails.length,
      contractValue,
      paymentCount: paymentDetails.length,
      paymentValue,
      travelCount: travelDetails.length,
      riskMovementCount: riskMovementDetails.length,
      riskMovementValue,
      superpricingCount: superpricingDetails.length,
      superpricingHighCount: superpricingDetails.filter(({ risk }) => risk.label === 'Alto').length,
      superpricingMediumCount: superpricingDetails.filter(({ risk }) => risk.label === 'Médio').length,
      superpricingValue: superpricingDetails.reduce((sum, entry) => sum + entry.risk.value, 0),
      highRiskCount,
      risksCount: risks.length,
      sourcesCount: (selectedPoliticalItem.sources || []).length,
      peopleCount: (selectedPoliticalItem.people || []).length,
      topDetail
    };
  }, [selectedPoliticalItem]);

  async function loadFeed(
    nextPage = 1,
    append = false,
    query = feedQuery,
    uf = selectedUf,
    risk = feedRiskFilter,
    sizeOrder = feedSizeOrder,
    dateFrom = feedDateFrom,
    dateTo = feedDateTo,
    city = feedCityFilter
  ) {
    setLoadingFeed(true);
    setError('');
    try {
      const params = new URLSearchParams({ page: String(nextPage), page_size: '10' });
      if (query.trim()) params.set('q', query.trim());
      if (uf) params.set('uf', uf);
      if (city.trim()) params.set('city', city.trim());
      if (risk && risk !== 'todos') params.set('risk_level', risk);
      if (sizeOrder && sizeOrder !== 'data') params.set('size_order', sizeOrder);
      if (dateFrom) params.set('date_from', dateFrom);
      if (dateTo) params.set('date_to', dateTo);
      const data = await apiGet(`/api/monitoring/feed?${params}`);
      const nextItems = data.items || [];
      setItems((current) => append ? [...current, ...nextItems] : nextItems);
      setSelectedAlert((current) => {
        if (append) return current;
        if (current && nextItems.some((item) => item.id === current.id && item.date === current.date)) {
          return current;
        }
        return nextItems[0] || null;
      });
      setPage(nextPage);
      setHasMore(Boolean(data.has_more));
    } catch (error) {
      setError('Não foi possível carregar dados reais das fontes públicas agora.');
    } finally {
      setLoadingFeed(false);
    }
  }

  async function loadMap(query = mapQuery, uf = selectedUf, city = '') {
    setLoadingMap(true);
    try {
      const stateParams = new URLSearchParams({ page_size: '100', source: 'auto' });
      const cityParams = new URLSearchParams({ page_size: '240', source: 'auto' });
      if (query.trim()) {
        stateParams.set('q', query.trim());
        cityParams.set('q', query.trim());
      }
      if (uf) {
        stateParams.set('uf', uf);
        cityParams.set('uf', uf);
      }
      if (city.trim()) cityParams.set('city', city.trim());
      const [riskResult, geoResult, cityResult] = await Promise.allSettled([
        apiGet(`/api/monitoring/state-map?${stateParams}`),
        apiGet('/api/public-data/ibge/states-geojson'),
        apiGet(`/api/monitoring/map?${cityParams}`)
      ]);
      const riskData = riskResult.status === 'fulfilled' ? riskResult.value : { states: [], cache_status: 'indisponivel' };
      const nextGeoJson = geoResult.status === 'fulfilled' ? geoResult.value : geoJson;
      const cityData = cityResult.status === 'fulfilled' ? cityResult.value : { points: mapPoints, cache_status: 'stale' };
      if (!nextGeoJson) throw new Error('geojson indisponivel');
      const risksByUf = {};
      for (const state of riskData.states || []) {
        risksByUf[state.uf] = state;
      }
      setStateRisks(risksByUf);
      setGeoJson(nextGeoJson);
      setMapPoints(cityData.points || []);
      setMapCacheStatus(cityData.cache_status || riskData.cache_status || '');
      setError('');
    } catch {
      setError('Não foi possível carregar o mapa real agora.');
    } finally {
      setLoadingMap(false);
    }
  }

  async function loadPoliticalData(kind = activeTab, force = false, nextPage = 1, append = false) {
    if (!force && !append && loadedPoliticalTabsRef.current[kind]) return;
    setLoadingPolitical(true);
    setError('');
    try {
      if (kind === 'parties') {
        const params = new URLSearchParams({ limit: '24', source: 'local', page: String(nextPage), page_size: '10' });
        if (politicalSearch.trim()) params.set('q', politicalSearch.trim());
        if (politicalRiskFilter !== 'todos') params.set('risk_level', politicalRiskFilter);
        if (politicalTypeFilter !== 'todos') params.set('analysis_type', politicalTypeFilter);
        if (politicalSizeOrder) params.set('size_order', politicalSizeOrder);
        const data = await apiGet(`/api/political/parties?${params}`);
        const nextItems = data.items || [];
        setPoliticalParties((current) => append ? mergePoliticalItems(current, nextItems) : nextItems);
        loadedPoliticalTabsRef.current.parties = true;
        politicalDataStampRef.current.parties = data.generated_at || politicalDataStampRef.current.parties;
        setPoliticalPagination((current) => ({
          ...current,
          parties: { page: nextPage, hasMore: Boolean(data.has_more) }
        }));
      }
      if (kind === 'politicians') {
        const params = new URLSearchParams({ limit: '36', source: 'local', page: String(nextPage), page_size: '10' });
        if (politicalSearch.trim()) params.set('q', politicalSearch.trim());
        if (politicalRiskFilter !== 'todos') params.set('risk_level', politicalRiskFilter);
        if (politicalTypeFilter !== 'todos') params.set('analysis_type', politicalTypeFilter);
        if (politicalSizeOrder) params.set('size_order', politicalSizeOrder);
        const data = await apiGet(`/api/political/politicians?${params}`);
        const nextItems = data.items || [];
        setPoliticalPeople((current) => append ? mergePoliticalItems(current, nextItems) : nextItems);
        loadedPoliticalTabsRef.current.politicians = true;
        politicalDataStampRef.current.politicians = data.generated_at || politicalDataStampRef.current.politicians;
        setPoliticalPagination((current) => ({
          ...current,
          politicians: { page: nextPage, hasMore: Boolean(data.has_more) }
        }));
      }
    } catch {
      setError('Não foi possível carregar a varredura política pública agora.');
    } finally {
      setLoadingPolitical(false);
    }
  }

  async function loadMonitorStatus() {
    try {
      const data = await apiGet('/api/monitoring/status');
      setMonitorStatus(data);
    } catch {
      setMonitorStatus(null);
    }
  }

  async function loadUniversalSearch(query) {
    if (!query.trim()) {
      setSearchResults([]);
      return;
    }
    setLoadingSearch(true);
    try {
      const params = new URLSearchParams({ q: query.trim() });
      const [localResult, publicResult] = await Promise.allSettled([
        apiGet(`/api/search/index?${params}`),
        apiGet(`/api/search?${params}`)
      ]);
      const localItems = localResult.status === 'fulfilled' ? localResult.value.results || [] : [];
      const publicItems = publicResult.status === 'fulfilled' ? publicResult.value.results || [] : [];
      const seen = new Set();
      const merged = [...localItems, ...publicItems].filter((result) => {
        const key = `${result.type}:${result.title}:${result.subtitle || ''}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      }).sort((left, right) => {
        const order = searchResultRank(left) - searchResultRank(right);
        if (order !== 0) return order;
        return String(left.title || '').localeCompare(String(right.title || ''), 'pt-BR');
      });
      setSearchResults(merged);
    } catch {
      setError('A busca unificada não conseguiu consultar todas as fontes agora.');
    } finally {
      setLoadingSearch(false);
    }
  }

  async function loadUniversalSearchProgressive(query) {
    const cleanQuery = query.trim();
    if (!cleanQuery) {
      searchRequestIdRef.current += 1;
      setSearchResults([]);
      setLoadingSearch(false);
      return;
    }

    const requestId = searchRequestIdRef.current + 1;
    searchRequestIdRef.current = requestId;
    let accumulatedResults = [];

    function publishResults(nextResults = []) {
      if (searchRequestIdRef.current !== requestId) return;
      accumulatedResults = mergeSearchResults(accumulatedResults, nextResults);
      setSearchResults(accumulatedResults);
    }

    setLoadingSearch(true);
    setSearchResults([]);
    setError('');

    const params = new URLSearchParams({ q: cleanQuery });
    let failedSources = 0;
    const localSearch = apiGet(`/api/search/index?${params}`)
      .then((data) => publishResults(data.results || []))
      .catch(() => { failedSources += 1; });
    const publicSearch = apiGet(`/api/search?${params}`)
      .then((data) => publishResults(data.results || []))
      .catch(() => { failedSources += 1; });

    await Promise.allSettled([localSearch, publicSearch]);
    if (searchRequestIdRef.current !== requestId) return;
    if (failedSources >= 2) {
      setError('A busca unificada não conseguiu consultar todas as fontes agora.');
    }
    setLoadingSearch(false);
  }

  async function scanPriority(uf, query = '', feedOptions = {}) {
    setError('');
    const immediateRisk = feedOptions.risk ?? feedRiskFilter;
    const immediateSizeOrder = feedOptions.sizeOrder ?? feedSizeOrder;
    const immediateDateFrom = feedOptions.dateFrom ?? feedDateFrom;
    const immediateDateTo = feedOptions.dateTo ?? feedDateTo;
    const immediateCity = feedOptions.city ?? feedCityFilter;
    try {
      await Promise.all([
        loadFeed(1, false, query, uf || '', immediateRisk, immediateSizeOrder, immediateDateFrom, immediateDateTo, immediateCity),
        loadMap(mapQuery, uf || selectedUf, immediateCity),
        loadMonitorStatus()
      ]);
    } catch {
      setError('Não foi possível atualizar os dados de monitoramento agora.');
    }
  }

  function selectStateOnMap(state) {
    if (mapClickSuppressRef.current) return;
    const uf = state.uf || '';
    setSelectedState(state);
    setSelectedUf(uf);
  }

  function mapSelectionFromEvent(event) {
    let element = event.target;
    while (element && element !== event.currentTarget) {
      if (element.dataset?.mapKind) {
        const kind = element.dataset.mapKind;
        const uf = element.dataset.uf || '';
        const baseSelection = {
          uf,
          alerts_count: Number(element.dataset.alertsCount || 0),
          risk_score: Number(element.dataset.riskScore || 0),
          total_value: Number(element.dataset.totalValue || 0)
        };
        if (kind === 'city') {
          return {
            ...baseSelection,
            city: element.dataset.city || '',
            state_name: element.dataset.stateName || uf,
            lat: Number(element.dataset.lat),
            lng: Number(element.dataset.lng),
            spatial_source: element.dataset.spatialSource || ''
          };
        }
        return {
          ...baseSelection,
          name: element.dataset.name || uf,
          state_name: element.dataset.stateName || element.dataset.name || uf
        };
      }
      element = element.parentElement;
    }
    return null;
  }

  function applyFeedCityFilter(city = feedCityDraft) {
    setFeedCityFilter(city.trim());
  }

  function openStateInFeed(state) {
    const uf = state.uf || '';
    const label = state.city || state.state_name || state.name || uf;
    const city = state.city || '';
    const query = '';
    setSelectedState(state);
    setSelectedUf(uf);
    setFeedQuery(query);
    setFeedCityFilter(city);
    setFeedCityDraft(city);
    setFeedRiskFilter('todos');
    setFeedSizeOrder('data');
    setFeedDateFrom('');
    setFeedDateTo('');
    setActiveSearchFilter({ type: city ? 'municipio' : 'estado', label, detail: city ? `${city} - UF ${uf}` : `UF ${uf}` });
    setActiveTab('feed');
    suppressSearchEffectRef.current = true;
    setSearchTerm('');
    loadFeed(1, false, query, uf, 'todos', 'data', '', '', city);
    scanPriority(uf, query, { risk: 'todos', sizeOrder: 'data', dateFrom: '', dateTo: '', city });
  }

function queryFromResult(result) {
  const payload = result.payload || {};
  if (result.type === 'politico_deputado' || result.type === 'politico_senador' || result.type === 'politico_relacionado') {
    return payload.nomeCivil || payload.nome || payload.name || result.title || searchTerm;
  }
  if (result.type === 'cnpj') {
    return payload.cnpj || payload.cnpj_basico || (result.subtitle || '').replace(/\D/g, '');
  }
    if (result.type === 'contrato') {
      return payload.niFornecedor || payload.nomeRazaoSocialFornecedor || result.title;
    }
    if (result.type === 'partido_politico') {
      return payload.sigla || payload.nome || result.title;
    }
    return result.title || searchTerm;
  }

  function ufFromResult(result) {
    const payload = result.payload || {};
    const nestedPayload = payload.payload || {};
    const directUf = payload.uf || payload.sigla || payload.siglaUf || nestedPayload.uf || nestedPayload.sigla || nestedPayload.siglaUf;
    if (directUf && String(directUf).length === 2) return String(directUf).toUpperCase();
    if (result.type === 'estado') return payload.sigla || '';
    if (result.type === 'municipio') {
      return payload.microrregiao?.mesorregiao?.UF?.sigla || '';
    }
    if (result.type === 'politico_deputado') return payload.siglaUf || '';
    if (result.type === 'politico_senador') {
      return payload.Mandato?.UfParlamentar || payload.IdentificacaoParlamentar?.UfParlamentar || '';
    }
    if (result.type === 'cnpj') return payload.uf || '';
    return '';
  }

  function filterFromResult(result) {
    const uf = ufFromResult(result);
    const payload = result.payload || {};
    if (result.type === 'estado') {
      return {
        query: '',
        uf,
        city: '',
        label: result.title,
        detail: `UF ${uf}`,
        scan: true
      };
    }
    if (result.type === 'municipio') {
      return {
        query: '',
        uf,
        city: result.title,
        label: result.title,
        detail: uf ? `Municipio em ${uf}` : 'Municipio',
        scan: false
      };
    }
    if (result.type === 'politico_deputado' || result.type === 'politico_senador' || result.type === 'politico_relacionado') {
      const primaryName = payload.nomeCivil || payload.nome || payload.name || result.title;
      return {
        query: primaryName || result.title,
        uf: '',
        city: '',
        label: result.title,
        detail: 'Foco principal no politico selecionado',
        scan: false
      };
    }
    if (result.type === 'cnpj') {
      const digits = String(payload.cnpj || payload.cnpj_basico || result.subtitle || '').replace(/\D/g, '');
      return {
        query: digits || result.title,
        uf: '',
        city: '',
        label: result.title,
        detail: digits ? `CNPJ ${digits}` : 'CNPJ',
        scan: false
      };
    }
    if (result.type === 'risco_superfaturamento') {
      return {
        query: payload.id || payload.supplier_cnpj || result.title,
        uf: payload.uf || '',
        city: payload.city || payload.municipio || '',
        label: result.title,
        detail: payload.estimated_variation ? `Superfaturamento estimado R$ ${Number(payload.estimated_variation || 0).toLocaleString('pt-BR')}` : 'Risco de Superfaturamento',
        scan: false
      };
    }
    if (result.type === 'contrato') {
      const contractId = payload.idCompra || payload.id || payload.numeroContrato || payload.numeroControlePncpCompra;
      return {
        query: contractId || payload.supplier_cnpj || payload.niFornecedor || result.title,
        uf: payload.uf || '',
        city: payload.city || payload.municipio || '',
        label: result.title,
        detail: contractId ? `Contrato ${contractId}` : 'Contrato relacionado',
        scan: false
      };
    }
    if (result.type === 'partido_politico') {
      const partyQuery = [payload.sigla, payload.nome || result.title].filter(Boolean).join('|');
      return {
        query: partyQuery || result.title,
        uf: '',
        city: '',
        label: result.title,
        detail: 'Foco principal no partido selecionado',
        scan: false
      };
    }
    return {
      query: result.title || searchTerm,
      uf,
      city: '',
      label: result.title || searchTerm,
      detail: result.source || 'Resultado relacionado',
      scan: false
    };
  }

  function politicalQueryFromResult(result) {
    const payload = result.payload || {};
    if (result.type === 'partido_politico') {
      return payload.sigla || payload.nome || result.title || searchTerm;
    }
    return queryFromResult(result);
  }

  function politicalMatchCandidates(result) {
    const payload = result.payload || {};
    return [
      politicalQueryFromResult(result),
      result.title,
      result.subtitle,
      payload.nomeCivil,
      payload.nome,
      payload.name,
      payload.sigla,
      payload.party,
      payload.partido
    ]
      .map(normalizeSearchText)
      .filter((value) => value.length >= 2);
  }

  function findPoliticalItemMatch(result, itemsToSearch = []) {
    const candidates = politicalMatchCandidates(result);
    return itemsToSearch.find((item) => {
      const itemText = normalizeSearchText([
        item.id,
        item.name,
        item.subtitle,
        item.party,
        item.role,
        item.summary,
        ...(item.people || [])
      ].join(' '));
      return candidates.some((candidate) => itemText.includes(candidate) || candidate.includes(normalizeSearchText(item.name)));
    }) || null;
  }

  async function openPoliticalResultFromSearch(result) {
    const isParty = result.type === 'partido_politico';
    const kind = isParty ? 'parties' : 'politicians';
    const endpoint = isParty ? '/api/political/parties' : '/api/political/politicians';
    const nextQuery = politicalQueryFromResult(result);
    const currentItems = isParty ? politicalParties : politicalPeople;
    const localMatch = findPoliticalItemMatch(result, currentItems);

    setSelectedSearchResult(null);
    setActiveTab(kind);
    setPoliticalSearch(nextQuery);
    setPoliticalRiskFilter('todos');
    setPoliticalSizeOrder('prioridade');
    setPoliticalTypeFilter('todos');
    setActiveSearchFilter({ type: result.type, label: result.title, detail: 'Cache politico da plataforma' });
    setSearchResults([]);

    if (localMatch) {
      setSelectedPoliticalItem(localMatch);
      return;
    }

    try {
      const params = new URLSearchParams({
        source: 'local',
        q: nextQuery,
        page: '1',
        page_size: '10',
        limit: isParty ? '24' : '36',
        size_order: 'prioridade'
      });
      const data = await apiGet(`${endpoint}?${params}`, { force: true, cacheTtlMs: 15 * 1000 });
      const nextItems = data.items || [];
      if (isParty) setPoliticalParties(nextItems);
      else setPoliticalPeople(nextItems);
      loadedPoliticalTabsRef.current[kind] = true;
      politicalDataStampRef.current[kind] = data.generated_at || politicalDataStampRef.current[kind];
      setPoliticalPagination((current) => ({
        ...current,
        [kind]: { page: 1, hasMore: Boolean(data.has_more) }
      }));
      const cachedMatch = findPoliticalItemMatch(result, nextItems) || nextItems[0] || null;
      if (cachedMatch) setSelectedPoliticalItem(cachedMatch);
    } catch {
      setError('Nao foi possivel abrir a analise politica cacheada agora.');
    }
  }

  function handleSearchResultClick(result) {
    if (result.type === 'partido_politico' || result.type?.startsWith('politico')) {
      openPoliticalResultFromSearch(result);
      return;
    }
    setSelectedSearchResult(result);
  }

function applySearchResult(result) {
    setSelectedSearchResult(null);
    if (result.type === 'stf_processo' || result.type === 'stf_jurisprudencia') {
      if (result.url) window.open(result.url, '_blank', 'noopener,noreferrer');
      setSearchResults([]);
      return;
    }
    if (result.type === 'politico_deputado' || result.type === 'politico_senador' || result.type === 'politico_relacionado') {
      const nextQuery = queryFromResult(result);
      setActiveTab('politicians');
      setPoliticalSearch(nextQuery);
      setPoliticalRiskFilter('todos');
      setPoliticalSizeOrder('prioridade');
      setPoliticalTypeFilter('todos');
      setSelectedPoliticalItem(null);
      setActiveSearchFilter({ type: result.type, label: result.title, detail: 'Analise politica em segundo plano' });
      setSearchResults([]);
      return;
    }
    if (result.type === 'partido_politico') {
      const nextQuery = queryFromResult(result);
      setActiveTab('parties');
      setPoliticalSearch(nextQuery);
      setPoliticalRiskFilter('todos');
      setPoliticalSizeOrder('prioridade');
      setPoliticalTypeFilter('todos');
      setSelectedPoliticalItem(null);
      setActiveSearchFilter({ type: result.type, label: result.title, detail: 'Analise de partido em segundo plano' });
      setSearchResults([]);
      return;
    }
    const filter = filterFromResult(result);
    const nextUf = filter.uf || '';
    const nextQuery = filter.query || '';
    const nextCity = filter.city || '';
    setActiveTab('feed');
    setSelectedUf(nextUf);
    setFeedQuery(nextQuery);
    setFeedCityFilter(nextCity);
    setFeedCityDraft(nextCity);
    setFeedRiskFilter('todos');
    setFeedSizeOrder('data');
    setFeedDateFrom('');
    setFeedDateTo('');
    suppressSearchEffectRef.current = true;
    setSearchTerm(filter.label || nextQuery);
    setSearchResults([]);
    setActiveSearchFilter({ type: result.type, label: filter.label, detail: filter.detail });
    setSelectedState(nextUf ? { uf: nextUf, state_name: filter.label, city: nextCity } : null);
    if (filter.scan) {
      scanPriority(nextUf, nextQuery, { risk: 'todos', sizeOrder: 'data', dateFrom: '', dateTo: '', city: nextCity });
    } else {
      loadFeed(1, false, nextQuery, nextUf, 'todos', 'data', '', '', nextCity);
      loadMap('', nextUf, nextCity);
    }
  }

  useEffect(() => {
    loadFeed(1, false, '');
    loadMap();
    loadMonitorStatus();
  }, []);

  useEffect(() => {
    setPoliticalDetailPage(1);
    setSelectedPoliticalDetailIndex(0);
    setPoliticalDetailRiskFilter('todos');
    setPoliticalDetailSearch('');
  }, [selectedPoliticalItem?.id, selectedPoliticalItem?.type]);

  useEffect(() => {
    setPoliticalDetailPage(1);
    setSelectedPoliticalDetailIndex(0);
  }, [politicalDetailRiskFilter, politicalDetailSearch]);

  useEffect(() => {
    setShowFullAlertTitle(false);
  }, [selectedAlert?.id]);

  useEffect(() => {
    if (activeTab === 'feed') {
      loadFeed(1, false, feedQuery, selectedUf, feedRiskFilter, feedSizeOrder, feedDateFrom, feedDateTo, feedCityFilter);
    }
    if (activeTab === 'parties' || activeTab === 'politicians') {
      loadPoliticalData(activeTab);
    }
  }, [activeTab, feedRiskFilter, feedSizeOrder, feedDateFrom, feedDateTo, feedCityFilter]);

  useEffect(() => {
    if (activeTab !== 'parties' && activeTab !== 'politicians') return;
    const stamp = monitorStatus?.generated_at || '';
    if (!stamp || politicalDataStampRef.current[activeTab] === stamp) return;
    loadPoliticalData(activeTab, true);
  }, [activeTab, monitorStatus?.generated_at]);

  useEffect(() => {
    if (activeTab !== 'parties' && activeTab !== 'politicians') return undefined;
    const timeout = window.setTimeout(() => {
      loadPoliticalData(activeTab, true, 1, false);
    }, politicalSearch.trim() ? 450 : 0);
    return () => window.clearTimeout(timeout);
  }, [activeTab, politicalSearch, politicalRiskFilter, politicalSizeOrder, politicalTypeFilter]);

  useEffect(() => {
    const interval = window.setInterval(() => {
      if (document.hidden) return;
      loadMonitorStatus();
      if (activeTab === 'feed' && page === 1 && !loadingFeed) {
        loadFeed(1, false, feedQuery, selectedUf, feedRiskFilter, feedSizeOrder, feedDateFrom, feedDateTo, feedCityFilter);
      }
      if (activeTab === 'map') loadMap(mapQuery, selectedUf, '');
      if ((activeTab === 'parties' || activeTab === 'politicians') && !loadingPolitical) {
        loadPoliticalData(activeTab, true, 1, false);
      }
    }, 45000);
    return () => window.clearInterval(interval);
  }, [activeTab, page, loadingFeed, loadingPolitical, feedQuery, selectedUf, feedRiskFilter, feedSizeOrder, feedDateFrom, feedDateTo, feedCityFilter, politicalSearch, politicalRiskFilter, politicalSizeOrder, politicalTypeFilter, mapQuery]);

  useEffect(() => {
    if (suppressSearchEffectRef.current) {
      suppressSearchEffectRef.current = false;
      return;
    }
    const query = searchTerm.trim();
    if (query.length < 2) {
      setSearchResults([]);
      return;
    }

    const timeout = window.setTimeout(() => {
      loadUniversalSearchProgressive(query);
    }, 650);

    return () => window.clearTimeout(timeout);
  }, [searchTerm]);

  useEffect(() => {
    if (!loadMoreRef.current || activeTab !== 'feed' || !hasMore) return undefined;

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting && !loadingFeed) {
          loadFeed(page + 1, true);
        }
      },
      { rootMargin: '320px 0px' }
    );

    observer.observe(loadMoreRef.current);
    return () => observer.disconnect();
  }, [activeTab, hasMore, loadingFeed, page, feedQuery, selectedUf, feedRiskFilter, feedSizeOrder, feedDateFrom, feedDateTo, feedCityFilter]);

  useEffect(() => {
    if (!loadMorePoliticalRef.current || (activeTab !== 'parties' && activeTab !== 'politicians')) return undefined;
    const pagination = politicalPagination[activeTab] || { page: 1, hasMore: false };
    if (!pagination.hasMore) return undefined;

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting && !loadingPolitical) {
          loadPoliticalData(activeTab, true, pagination.page + 1, true);
        }
      },
      { rootMargin: '320px 0px' }
    );

    observer.observe(loadMorePoliticalRef.current);
    return () => observer.disconnect();
  }, [activeTab, loadingPolitical, politicalPagination, politicalSearch, politicalRiskFilter, politicalSizeOrder, politicalTypeFilter]);

  function handleSearch(event) {
    event.preventDefault();
    const stateUf = ufFromSearchText(searchTerm);
    if (stateUf) {
      setSelectedUf(stateUf);
      setFeedQuery('');
      setFeedCityFilter('');
      setFeedCityDraft('');
      setActiveSearchFilter({ type: 'estado', label: searchTerm.trim().toUpperCase(), detail: `UF ${stateUf}` });
      setSelectedState({ uf: stateUf, state_name: searchTerm.trim().toUpperCase() });
      setActiveTab('feed');
      scanPriority(stateUf, '', { city: '' });
      return;
    }
    setSelectedUf('');
    setFeedQuery(searchTerm);
    setFeedCityFilter('');
    setFeedCityDraft('');
    setActiveSearchFilter({ type: 'busca', label: searchTerm, detail: 'Termo livre' });
    setSearchResults([]);
    loadUniversalSearchProgressive(searchTerm);
    loadFeed(1, false, searchTerm, '', feedRiskFilter, feedSizeOrder, feedDateFrom, feedDateTo, '');
  }

  function handleSearchFocus() {
    const query = searchTerm.trim();
    if (query.length >= 2) {
      loadUniversalSearchProgressive(query);
    }
  }

  function reloadPage() {
    window.location.reload();
  }

  const selectedRisk = riskCopy[selectedAlert?.risk_level] || riskCopy.indeterminado;
  const selectedOfficialSources = officialSourcesForAlert(selectedAlert);
  const selectedPublicEvidence = Array.isArray(selectedAlert?.report?.public_evidence)
    ? selectedAlert.report.public_evidence
    : [];
  const selectedAlertFlags = selectedAlert
    ? contextualAttentionFlags(selectedAlert.report?.red_flags || [], selectedAlert)
    : [];
  const selectedAlertMetrics = selectedAlert ? alertComparisonMetrics(selectedAlert) : [];
  const selectedAlertDescription = selectedAlert ? alertAnalyticDescription(selectedAlert, selectedAlertFlags) : '';
  const selectedSearchReview = selectedSearchResult ? searchResultReview(selectedSearchResult) : null;
  const selectedSearchDescription = selectedSearchResult && selectedSearchReview
    ? searchAnalyticDescription(selectedSearchResult, selectedSearchReview)
    : '';

  function updateMapZoom(nextZoom, anchorPoint = null) {
    setMapZoom((currentZoom) => {
      const nextValue = typeof nextZoom === 'function' ? nextZoom(currentZoom) : nextZoom;
      const nextClampedZoom = Number(clampNumber(nextValue, 1, 3.2).toFixed(2));
      const anchor = anchorPoint || { x: mapBounds.width / 2, y: mapBounds.height / 2 };
      if (nextClampedZoom !== currentZoom) {
        setMapPan((currentPan) => {
          const centerX = mapBounds.width / 2;
          const centerY = mapBounds.height / 2;
          const scaleRatio = nextClampedZoom / currentZoom;
          const nextPan = {
            x: anchor.x - centerX - scaleRatio * (anchor.x - centerX - currentPan.x),
            y: anchor.y - centerY - scaleRatio * (anchor.y - centerY - currentPan.y)
          };
          return clampMapPan(nextPan, nextClampedZoom);
        });
      }
      return nextClampedZoom;
    });
  }

  useEffect(() => {
    if (activeTab !== 'map') return undefined;
    const node = mapViewportRef.current;
    if (!node) return undefined;
    const handleNativeWheel = (event) => {
      event.preventDefault();
      event.stopPropagation();
      const delta = event.deltaY > 0 ? -0.18 : 0.18;
      updateMapZoom((current) => current + delta, mapClientPoint(event.clientX, event.clientY, mapSvgRef.current));
    };
    node.addEventListener('wheel', handleNativeWheel, { passive: false });
    return () => node.removeEventListener('wheel', handleNativeWheel);
  }, [activeTab]);

  function handleMapPointerDown(event) {
    const point = mapClientPoint(event.clientX, event.clientY, event.currentTarget);
    mapPointerRef.current = {
      pointerId: event.pointerId,
      x: event.clientX,
      y: event.clientY,
      svgX: point.x,
      svgY: point.y,
      pan: mapPan,
      moved: false,
      selection: mapSelectionFromEvent(event)
    };
    event.currentTarget.setPointerCapture?.(event.pointerId);
  }

  function handleMapPointerMove(event) {
    const drag = mapPointerRef.current;
    if (!drag || drag.pointerId !== event.pointerId) return;
    if (Math.abs(event.clientX - drag.x) > 4 || Math.abs(event.clientY - drag.y) > 4) {
      drag.moved = true;
    }
    const point = mapClientPoint(event.clientX, event.clientY, event.currentTarget);
    setMapPan(clampMapPan({
      x: drag.pan.x + (point.x - drag.svgX),
      y: drag.pan.y + (point.y - drag.svgY)
    }, mapZoom));
  }

  function handleMapPointerUp(event) {
    if (mapPointerRef.current?.pointerId === event.pointerId) {
      if (mapPointerRef.current.moved) {
        mapClickSuppressRef.current = true;
        window.setTimeout(() => {
          mapClickSuppressRef.current = false;
        }, 0);
      } else if (mapPointerRef.current.selection) {
        selectStateOnMap(mapPointerRef.current.selection);
      }
      mapPointerRef.current = null;
      event.currentTarget.releasePointerCapture?.(event.pointerId);
    }
  }

  const mapTransform = `translate(${mapBounds.width / 2 + mapPan.x} ${mapBounds.height / 2 + mapPan.y}) scale(${mapZoom}) translate(${-mapBounds.width / 2} ${-mapBounds.height / 2})`;
  const mapSearchText = normalizeSearchText(mapQuery);
  const visibleMapPoints = useMemo(() => {
    if (!mapSearchText) return mapPoints;
    return mapPoints.filter((point) => normalizeSearchText([
      point.city,
      point.uf,
      stateRisks[point.uf]?.state_name
    ].join(' ')).includes(mapSearchText));
  }, [mapPoints, mapSearchText, stateRisks]);
  const mapPointLimit = mapZoom >= 1.6 ? 240 : 140;
  const mapCityLabelLimit = mapZoom >= 2.25 ? 140 : mapZoom >= 1.7 ? 90 : mapZoom >= 1.3 ? 45 : 0;
  const mapStateBounds = useMemo(() => {
    const bounds = {};
    for (const feature of geoJson?.features || []) {
      const props = feature.properties || {};
      const uf = props.sigla || props.UF || props.uf || props.SIGLA_UF || IBGE_CODE_TO_UF[props.codarea];
      const projectedBounds = geometryProjectedBounds(feature.geometry);
      if (uf && projectedBounds) bounds[uf] = projectedBounds;
    }
    return bounds;
  }, [geoJson]);
  const positionedMapPoints = useMemo(
    () => offsetOverlappingMapPoints(visibleMapPoints.slice(0, mapPointLimit), mapStateBounds),
    [visibleMapPoints, mapPointLimit, mapStateBounds]
  );

  return (
    <div className="min-h-screen overflow-x-clip bg-neutral-950 text-neutral-100">
      <header className="border-b border-neutral-800 bg-black md:sticky md:top-0 md:z-20">
        <div className="mx-auto flex min-h-16 max-w-7xl flex-col gap-3 px-4 py-3 sm:px-6 md:flex-row md:items-center md:justify-between lg:px-8">
          <div className="flex min-w-0 items-center gap-3">
            <Target className="h-8 w-8 text-red-600" />
            <h1 className="text-2xl font-black tracking-widest">
              Coibe<span className="ml-2 text-base tracking-normal text-red-600">IA</span>
            </h1>
          </div>

          <form onSubmit={handleSearch} className="relative min-w-0 w-full md:max-w-xl">
            <Search className="pointer-events-none absolute left-3 top-1/2 h-5 w-5 -translate-y-1/2 text-neutral-500" />
            <input
              value={searchTerm}
              onChange={(event) => setSearchTerm(event.target.value)}
              onFocus={handleSearchFocus}
              placeholder="Buscar estado, cidade, político, partido, STF, CNPJ ou contrato..."
              className="h-11 w-full rounded-lg border border-neutral-800 bg-neutral-900 pl-10 pr-4 text-sm text-white outline-none placeholder:text-neutral-500 focus:border-red-600 focus:ring-2 focus:ring-red-600/30"
            />
          </form>

          <a
            href="https://github.com/JpAndreBTA/Coibe"
            target="_blank"
            rel="noreferrer"
            className="flex h-11 shrink-0 items-center justify-center gap-2 rounded-lg border border-neutral-800 bg-neutral-900 px-3 text-xs font-bold text-neutral-300 transition hover:border-red-700 hover:text-white"
            title="Abrir código no GitHub"
          >
            <Github className="h-4 w-4" />
            <span className="hidden lg:inline">Código aberto</span>
            <span className="hidden border-l border-neutral-700 pl-2 text-neutral-500 xl:inline">Por Jp André</span>
          </a>
        </div>
      </header>

      <main className="mx-auto max-w-7xl px-4 py-7 sm:px-6 lg:px-8">
        {error && (
          <div className="mb-5 flex flex-col gap-3 rounded-lg border border-red-900 bg-red-950/30 p-4 text-sm text-red-100 sm:flex-row sm:items-center sm:justify-between">
            <span>{error}</span>
            <button
              type="button"
              onClick={reloadPage}
              className="inline-flex h-10 shrink-0 items-center justify-center gap-2 rounded-lg border border-red-800 bg-red-600 px-3 text-xs font-black text-white transition hover:bg-red-500"
              title="Atualizar a pagina"
            >
              <RefreshCw className="h-4 w-4" />
              Atualizar pagina
            </button>
          </div>
        )}

        <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          {stats.map((stat) => {
            const Icon = stat.icon;
            return (
              <article
                key={stat.label}
                className={`rounded-lg border border-neutral-800 bg-neutral-900 p-5 shadow-sm ${stat.accent ? 'border-l-4 border-l-red-600' : ''}`}
              >
                <p className="text-sm font-semibold text-neutral-400">{stat.label}</p>
                <strong className={`mt-2 block text-2xl font-black ${stat.emphasis}`}>{stat.value}</strong>
                <span className="mt-3 flex items-center gap-1.5 text-xs font-semibold text-red-500">
                  <Icon className="h-4 w-4" />
                  {stat.note}
                </span>
              </article>
            );
          })}
        </section>

        <section className="mt-5 rounded-lg border border-neutral-800 bg-neutral-900 p-4">
          <div className="flex flex-col justify-between gap-4 md:flex-row md:items-center">
            <div>
              <p className="text-xs font-black uppercase text-red-400">Monitoramento nacional contínuo</p>
              <h2 className="mt-1 font-black text-white">
                {monitorStatus?.running ? 'Análise ativa' : 'Aguardando nova verificação'}
              </h2>
              <p className="mt-1 text-sm text-neutral-400">
                {monitorStatus?.message || 'A plataforma atualiza os dados automaticamente pelos conectores configurados.'}
              </p>
            </div>
            <div className="grid w-full grid-cols-2 gap-3 text-sm md:w-auto md:min-w-[24rem] lg:grid-cols-5">
              <div className="rounded-lg border border-neutral-800 bg-neutral-950 p-3">
                <span className="text-neutral-500">Itens analisados</span>
                <strong className="block text-2xl text-white">{analyzedCount}</strong>
                <small className="mt-1 block text-[11px] text-neutral-500">{analyzedCountNote}</small>
              </div>
              <div className="rounded-lg border border-neutral-800 bg-neutral-950 p-3">
                <span className="text-neutral-500">Biblioteca</span>
                <strong className="block text-2xl text-white">{libraryCount || monitorStatus?.database_items_count || 0}</strong>
              </div>
              <div className="rounded-lg border border-neutral-800 bg-neutral-950 p-3">
                <span className="text-neutral-500">APIs públicas</span>
                <strong className="block text-2xl text-white">{monitorStatus?.public_records_count ?? 0}</strong>
              </div>
              <div className="rounded-lg border border-neutral-800 bg-neutral-950 p-3">
                <span className="text-neutral-500">Códigos públicos</span>
                <strong className="block text-2xl text-white">{monitorStatus?.public_codes_count ?? 0}</strong>
              </div>
              <div className="rounded-lg border border-neutral-800 bg-neutral-950 p-3">
                <span className="text-neutral-500">Alertas</span>
                <strong className="block text-2xl text-red-400">{monitorStatus?.alerts_count ?? 0}</strong>
              </div>
            </div>
          </div>
          {monitorStatus?.generated_at && (
            <p className="mt-3 text-xs text-neutral-500">
              Última análise: {statusUpdatedAt}
              {collectorState.next_feed_page ? ` - próxima página: ${collectorState.next_feed_page}` : ''}
            </p>
          )}
        </section>

        {(loadingSearch || searchResults.length > 0) && (
          <section className="mt-5 rounded-lg border border-neutral-800 bg-neutral-900 p-4">
            <div className="mb-3 flex items-center justify-between gap-3">
              <div>
                <p className="text-xs font-black uppercase text-red-400">Busca unificada</p>
                <h2 className="font-black text-white">Risco de Superfaturamento, políticos, partidos, STF, CNPJ e contratos</h2>
              </div>
              {loadingSearch && <Loader2 className="h-5 w-5 animate-spin text-red-500" />}
            </div>
            <div className="grid gap-3 md:grid-cols-2">
              {searchResults.map((result, index) => {
                const previewReview = searchResultReview(result);
                const previewFlag = previewReview.flags[0];
                const isPoliticalPriority = index === 0 && String(result.type || '').startsWith('politico');
                return (
                <button
                  key={`${result.type}-${result.title}-${index}`}
                  type="button"
                  onClick={() => handleSearchResultClick(result)}
                  title={result.title}
                  className={`rounded-lg border p-4 text-left transition hover:border-red-700 ${isPoliticalPriority ? 'border-red-800 bg-red-950/20 md:col-span-2' : 'border-neutral-800 bg-neutral-950/70'}`}
                >
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <span className="rounded border border-red-900 bg-red-950/40 px-2 py-1 text-[11px] font-black uppercase text-red-300">
                        {result.type.replaceAll('_', ' ')}
                      </span>
                      <strong title={result.title} className="mt-3 block text-white">{result.title}</strong>
                      {result.subtitle && <p title={result.subtitle} className="mt-1 text-sm text-neutral-400">{result.subtitle}</p>}
                    </div>
                    <ChevronRight className="h-4 w-4 shrink-0 text-red-400" />
                  </div>
                  {previewFlag && (
                    <div className="mt-3 rounded border border-red-900/40 bg-red-950/10 p-2">
                      <p className="text-[11px] font-black uppercase text-red-300">Risco e detalhe</p>
                      <strong className="mt-1 block text-sm text-white">{previewFlag.title}</strong>
                      <p className="mt-1 line-clamp-2 text-xs leading-5 text-neutral-400">{previewFlag.message}</p>
                    </div>
                  )}
                  <p className="mt-3 text-xs text-neutral-500">{result.source}</p>
                </button>
                );
              })}
              {!loadingSearch && searchResults.length === 0 && (
                <div className="rounded-lg border border-neutral-800 bg-neutral-950/70 p-4 text-sm text-neutral-400">
                  Nenhum resultado direto encontrado nas fontes unificadas.
                </div>
              )}
            </div>
          </section>
        )}

        <section className={`mt-8 grid gap-7 ${activeTab === 'feed' ? 'lg:grid-cols-[minmax(0,1fr)_360px]' : 'lg:grid-cols-1'}`}>
          <div>
            <div className="flex max-w-full overflow-x-auto border-b border-neutral-800">
              <button
                onClick={() => setActiveTab('feed')}
                className={`shrink-0 px-4 py-3 text-sm font-bold transition sm:px-5 ${activeTab === 'feed' ? 'border-b-2 border-red-600 text-red-500' : 'text-neutral-400 hover:text-white'}`}
              >
                Feed de Monitoramento
              </button>
              <button
                onClick={() => setActiveTab('map')}
                className={`shrink-0 px-4 py-3 text-sm font-bold transition sm:px-5 ${activeTab === 'map' ? 'border-b-2 border-red-600 text-red-500' : 'text-neutral-400 hover:text-white'}`}
              >
                Mapa de Alertas
              </button>
              <button
                onClick={() => {
                  setActiveTab('parties');
                  setPoliticalSearch('');
                }}
                className={`shrink-0 px-4 py-3 text-sm font-bold transition sm:px-5 ${activeTab === 'parties' ? 'border-b-2 border-red-600 text-red-500' : 'text-neutral-400 hover:text-white'}`}
              >
                Partido
              </button>
              <button
                onClick={() => {
                  setActiveTab('politicians');
                  setPoliticalSearch('');
                }}
                className={`shrink-0 px-4 py-3 text-sm font-bold transition sm:px-5 ${activeTab === 'politicians' ? 'border-b-2 border-red-600 text-red-500' : 'text-neutral-400 hover:text-white'}`}
              >
                Político
              </button>
              <button
                onClick={() => setActiveTab('about')}
                className={`shrink-0 px-4 py-3 text-sm font-bold transition sm:px-5 ${activeTab === 'about' ? 'border-b-2 border-red-600 text-red-500' : 'text-neutral-400 hover:text-white'}`}
              >
                Sobre o Coibe
              </button>
              <button
                onClick={() => setActiveTab('donate')}
                className={`shrink-0 px-4 py-3 text-sm font-bold transition sm:px-5 ${activeTab === 'donate' ? 'border-b-2 border-red-600 text-red-500' : 'text-neutral-400 hover:text-white'}`}
              >
                Doar
              </button>
            </div>
            {activeSearchFilter && activeTab === 'feed' && (
              <div className="mt-4 flex items-center justify-between rounded-lg border border-red-900/70 bg-red-950/20 px-4 py-3 text-sm text-red-100">
                <span>
                  Feed filtrado por <strong>{activeSearchFilter.label}</strong>
                  {activeSearchFilter.detail ? <span className="text-red-200/80"> - {activeSearchFilter.detail}</span> : null}
                </span>
                <button
                  onClick={() => {
                    setSelectedUf('');
                    setFeedQuery('');
                    setFeedCityFilter('');
                    setFeedCityDraft('');
                    setFeedDateFrom('');
                    setFeedDateTo('');
                    setActiveSearchFilter(null);
                    setSelectedState(null);
                    setSearchTerm('');
                    loadFeed(1, false, '', '', feedRiskFilter, feedSizeOrder, '', '', '');
                  }}
                  className="font-bold text-white hover:text-red-200"
                >
                  Limpar filtro
                </button>
              </div>
            )}

            {activeTab === 'feed' && (
              <div className="mt-4 grid gap-3 rounded-lg border border-neutral-800 bg-neutral-900 p-3 sm:grid-cols-2 lg:grid-cols-[minmax(0,0.9fr)_minmax(0,0.9fr)_minmax(0,1fr)_minmax(0,1.25fr)_auto]">
                <label className="flex min-w-0 items-center gap-3 rounded-lg border border-neutral-800 bg-neutral-950 px-3 py-2">
                  <Filter className="h-4 w-4 shrink-0 text-red-400" />
                  <span className="min-w-0 flex-1">
                    <span className="block text-[11px] font-black uppercase text-neutral-500">Risco</span>
                    <select
                      value={feedRiskFilter}
                      onChange={(event) => setFeedRiskFilter(event.target.value)}
                      className="mt-1 w-full bg-transparent text-sm font-bold text-white outline-none"
                    >
                      <option className="bg-neutral-950" value="todos">Todos os riscos</option>
                      <option className="bg-neutral-950" value="alto">Alto</option>
                      <option className="bg-neutral-950" value="médio">Medio</option>
                      <option className="bg-neutral-950" value="baixo">Baixo</option>
                    </select>
                  </span>
                </label>

                <label className="flex min-w-0 items-center gap-3 rounded-lg border border-neutral-800 bg-neutral-950 px-3 py-2">
                  <ArrowDownWideNarrow className="h-4 w-4 shrink-0 text-red-400" />
                  <span className="min-w-0 flex-1">
                    <span className="block text-[11px] font-black uppercase text-neutral-500">Ordenar</span>
                    <select
                      value={feedSizeOrder}
                      onChange={(event) => setFeedSizeOrder(event.target.value)}
                      className="mt-1 w-full bg-transparent text-sm font-bold text-white outline-none"
                    >
                      <option className="bg-neutral-950" value="data">Mais recentes</option>
                      <option className="bg-neutral-950" value="data_asc">Mais antigos</option>
                      <option className="bg-neutral-950" value="desc">Maior valor primeiro</option>
                      <option className="bg-neutral-950" value="asc">Menor valor primeiro</option>
                    </select>
                  </span>
                </label>

                <label className="flex min-w-0 items-center gap-3 rounded-lg border border-neutral-800 bg-neutral-950 px-3 py-2">
                  <MapPin className="h-4 w-4 shrink-0 text-red-400" />
                  <span className="min-w-0 flex-1">
                    <span className="block text-[11px] font-black uppercase text-neutral-500">Cidade</span>
                    <span className="mt-1 flex min-w-0 items-center gap-2">
                      <input
                        value={feedCityDraft}
                        onChange={(event) => setFeedCityDraft(event.target.value)}
                        onBlur={() => applyFeedCityFilter()}
                        onKeyDown={(event) => {
                          if (event.key === 'Enter') {
                            event.preventDefault();
                            applyFeedCityFilter(event.currentTarget.value);
                          }
                        }}
                        placeholder="Municipio"
                        className="min-w-0 flex-1 bg-transparent text-sm font-bold text-white outline-none placeholder:text-neutral-600"
                      />
                      <button
                        type="button"
                        onMouseDown={(event) => event.preventDefault()}
                        onClick={() => applyFeedCityFilter()}
                        className="inline-flex h-7 w-7 shrink-0 items-center justify-center rounded border border-neutral-800 text-neutral-300 hover:border-red-700"
                        title="Aplicar cidade"
                      >
                        <Search className="h-3.5 w-3.5" />
                      </button>
                    </span>
                  </span>
                </label>

                <label className="flex min-w-0 items-center gap-3 rounded-lg border border-neutral-800 bg-neutral-950 px-3 py-2 sm:col-span-2 lg:col-span-1">
                  <FileText className="h-4 w-4 shrink-0 text-red-400" />
                  <span className="min-w-0 flex-1">
                    <span className="block text-[11px] font-black uppercase text-neutral-500">Data do conteúdo</span>
                    <span className="mt-1 grid grid-cols-2 gap-2">
                      <input
                        type="date"
                        value={feedDateFrom}
                        onChange={(event) => {
                          const nextDateFrom = event.target.value;
                          setFeedDateFrom(nextDateFrom);
                          loadFeed(1, false, feedQuery, selectedUf, feedRiskFilter, feedSizeOrder, nextDateFrom, feedDateTo, feedCityFilter);
                        }}
                        className="min-w-0 bg-transparent text-sm font-bold text-white outline-none [color-scheme:dark]"
                        aria-label="Data inicial do conteúdo"
                      />
                      <input
                        type="date"
                        value={feedDateTo}
                        onChange={(event) => {
                          const nextDateTo = event.target.value;
                          setFeedDateTo(nextDateTo);
                          loadFeed(1, false, feedQuery, selectedUf, feedRiskFilter, feedSizeOrder, feedDateFrom, nextDateTo, feedCityFilter);
                        }}
                        className="min-w-0 bg-transparent text-sm font-bold text-white outline-none [color-scheme:dark]"
                        aria-label="Data final do conteúdo"
                      />
                    </span>
                  </span>
                </label>

                <button
                  onClick={() => {
                    setFeedRiskFilter('todos');
                    setFeedSizeOrder('data');
                    setFeedCityFilter('');
                    setFeedCityDraft('');
                    setFeedDateFrom('');
                    setFeedDateTo('');
                    loadFeed(1, false, feedQuery, selectedUf, 'todos', 'data', '', '', '');
                  }}
                  className="rounded-lg border border-neutral-700 px-4 py-2 text-sm font-bold text-neutral-200 transition hover:bg-neutral-800 sm:col-span-2 lg:col-span-1"
                >
                  Limpar filtros
                </button>
              </div>
            )}

            {activeTab === 'feed' ? (
              <div className="mt-5 space-y-4">
                {loadingFeed && items.length === 0 && (
                  <div className="flex h-56 items-center justify-center rounded-lg border border-neutral-800 bg-neutral-900 text-neutral-400">
                    <Loader2 className="mr-2 h-5 w-5 animate-spin" />
                    Carregando dados oficiais...
                  </div>
                )}

                {items.map((alert) => {
                  const risk = riskCopy[alert.risk_level] || riskCopy.indeterminado;
                  const baselineValue = alertBaselineValue(alert);
                  return (
                    <button
                      key={`${alert.id}-${alert.date}`}
                      onClick={() => setSelectedAlert(alert)}
                      title={alert.title}
                      className={`min-w-0 w-full overflow-hidden rounded-lg border bg-neutral-900 p-3 text-left transition hover:border-red-700 sm:p-5 ${selectedAlert?.id === alert.id ? 'border-red-900' : 'border-neutral-800'}`}
                    >
                      <div className="flex min-w-0 items-start justify-between gap-2 sm:gap-4">
                        <div className="flex min-w-0 items-center gap-2 text-[11px] font-semibold text-neutral-400 sm:gap-3 sm:text-xs">
                          <FileText className="h-4 w-4 shrink-0 sm:h-5 sm:w-5" />
                          <span className="min-w-0 truncate">{formatDate(alert.date)}</span>
                        </div>
                        <span className={`shrink-0 rounded-full border px-2 py-1 text-[11px] font-black sm:px-3 sm:text-xs ${risk.color}`}>
                          {risk.label}
                        </span>
                      </div>

                      <h2 title={alert.title} className="mt-3 line-clamp-3 break-words text-base font-black leading-snug text-white sm:mt-4 sm:line-clamp-2 sm:text-lg">{alert.title}</h2>
                      <div className="mt-3 grid min-w-0 gap-2 text-xs text-neutral-400 sm:flex sm:flex-wrap sm:gap-4 sm:text-sm">
                        <span className="flex min-w-0 items-center gap-1.5"><MapPin className="h-4 w-4 shrink-0" /><span className="min-w-0 truncate sm:whitespace-normal">{alert.location}</span></span>
                        <span className="flex min-w-0 items-center gap-1.5"><User className="h-4 w-4 shrink-0" /><span className="min-w-0 truncate sm:whitespace-normal">{alert.entity}</span></span>
                      </div>

                      <div className="mt-3 grid min-w-0 items-start gap-2 rounded-lg border border-neutral-800 bg-neutral-950/70 p-2 sm:mt-4 sm:gap-3 sm:p-3 md:grid-cols-[1fr_1fr_1fr_auto]">
                        <div className="min-w-0 rounded border border-neutral-800 bg-neutral-900/60 px-2 py-2 md:border-0 md:bg-transparent md:p-0">
                          <p className="text-xs text-neutral-400">Valor médio encontrado</p>
                          <strong className="text-white">{baselineValue !== null ? compactValue(baselineValue, 'baseline') : 'Sem média confiável'}</strong>
                        </div>
                        <div className="min-w-0 rounded border border-neutral-800 bg-neutral-900/60 px-2 py-2 md:border-0 md:bg-transparent md:p-0">
                          <p className="text-[11px] text-neutral-400 sm:text-xs">Pago/contratado</p>
                          <strong className="block break-words text-sm text-white sm:text-base">{alert.formatted_value}</strong>
                        </div>
                        <div className="min-w-0 rounded border border-red-900/50 bg-red-950/20 px-2 py-2 text-left md:border-0 md:bg-transparent md:p-0 md:text-right">
                          <p className="text-xs font-bold text-red-400">Possível valor acima da média</p>
                          <strong className="block break-words text-sm text-red-500 sm:text-base">{alert.formatted_variation}</strong>
                        </div>
                        <ChevronRight className="hidden h-5 w-5 text-neutral-500 sm:block" />
                      </div>
                    </button>
                  );
                })}

                {!loadingFeed && items.length === 0 && (
                  <div className="rounded-lg border border-neutral-800 bg-neutral-900 p-6 text-sm text-neutral-400">
                    Nenhum item encontrado para os filtros selecionados.
                  </div>
                )}

                {items.length > 0 && (
                  <>
                    <div ref={loadMoreRef} className="h-4" aria-hidden="true" />
                    <button
                      onClick={() => loadFeed(page + 1, true)}
                      disabled={!hasMore || loadingFeed}
                      className="flex w-full items-center justify-center rounded-lg border border-neutral-800 bg-neutral-900 px-4 py-3 text-sm font-bold text-neutral-200 transition hover:border-red-700 disabled:cursor-not-allowed disabled:opacity-50"
                    >
                      {loadingFeed ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                      {hasMore ? 'Carregando mais itens automaticamente' : 'Todos os itens disponíveis foram carregados'}
                    </button>
                  </>
                )}
              </div>
            ) : activeTab === 'map' ? (
              <div className="mt-5 overflow-hidden rounded-lg border border-neutral-800 bg-neutral-900">
                <div className="border-b border-neutral-800 px-5 py-4">
                  <h2 className="font-black text-white">Mapa de Alertas por Estado</h2>
                  <div className="mt-4 flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                    <form
                      onSubmit={(event) => {
                        event.preventDefault();
                        loadMap(mapQuery, selectedUf, '');
                      }}
                      className="flex min-w-0 flex-1 items-center gap-2 rounded-lg border border-neutral-800 bg-neutral-950 px-3 py-2"
                    >
                      <Search className="h-4 w-4 shrink-0 text-red-400" />
                      <input
                        value={mapQuery}
                        onChange={(event) => setMapQuery(event.target.value)}
                        placeholder="Buscar UF, estado ou municipio"
                        className="min-w-0 flex-1 bg-transparent text-sm font-bold text-white outline-none placeholder:text-neutral-600"
                      />
                      <button
                        type="submit"
                        className="rounded border border-neutral-700 px-3 py-1 text-xs font-black text-neutral-200 hover:bg-neutral-800"
                        title="Buscar no mapa"
                      >
                        Buscar
                      </button>
                    </form>
                    <div className="flex items-center gap-2">
                      <button
                        type="button"
                        onClick={() => updateMapZoom((current) => current - 0.25)}
                        className="flex h-10 w-10 items-center justify-center rounded-lg border border-neutral-800 bg-neutral-950 text-neutral-200 hover:border-red-700"
                        title="Reduzir zoom"
                      >
                        <ZoomOut className="h-4 w-4" />
                      </button>
                      <button
                        type="button"
                        onClick={() => updateMapZoom((current) => current + 0.25)}
                        className="flex h-10 w-10 items-center justify-center rounded-lg border border-neutral-800 bg-neutral-950 text-neutral-200 hover:border-red-700"
                        title="Aumentar zoom"
                      >
                        <ZoomIn className="h-4 w-4" />
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          setMapQuery('');
                          setSelectedState(null);
                          setSelectedUf('');
                          updateMapZoom(1);
                          setMapPan({ x: 0, y: 0 });
                          loadMap('', '', '');
                        }}
                        className="rounded-lg border border-neutral-800 bg-neutral-950 px-3 py-2 text-xs font-black text-neutral-200 hover:border-red-700"
                        title="Limpar mapa"
                      >
                        Reset
                      </button>
                    </div>
                  </div>
                  {mapCacheStatus && (
                    <p className="mt-2 text-xs text-neutral-500">Cache do mapa: {mapCacheStatus}</p>
                  )}
                  <p className="mt-1 text-sm text-neutral-400">Agregado por municipio da UASG com coordenadas e cache geoespacial quando configurado.</p>
                </div>
                <div
                  ref={mapViewportRef}
                  className="relative min-h-[520px] bg-[radial-gradient(circle_at_center,#262626_0,#111_55%,#080808_100%)] p-4"
                >
                  {loadingMap && (
                    <div className="absolute inset-0 z-10 flex items-center justify-center bg-black/40 text-neutral-300">
                      <Loader2 className="mr-2 h-5 w-5 animate-spin" />
                      Carregando mapa...
                    </div>
                  )}
                  <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_220px]">
                    <svg
                      ref={mapSvgRef}
                      viewBox={`0 0 ${mapBounds.width} ${mapBounds.height}`}
                      role="img"
                      aria-label="Mapa do Brasil por estados"
                      className="h-auto w-full max-h-[620px] touch-none select-none cursor-grab active:cursor-grabbing"
                      onPointerDown={handleMapPointerDown}
                      onPointerMove={handleMapPointerMove}
                      onPointerUp={handleMapPointerUp}
                      onPointerCancel={handleMapPointerUp}
                    >
                      <rect width={mapBounds.width} height={mapBounds.height} fill="transparent" />
                      <g transform={mapTransform}>
                      {(geoJson?.features || []).map((feature) => {
                        const props = feature.properties || {};
                        const uf = props.sigla || props.UF || props.uf || props.SIGLA_UF || IBGE_CODE_TO_UF[props.codarea];
                        const name = props.nome || props.NM_UF || props.name || stateRisks[uf]?.state_name || uf;
                        const risk = stateRisks[uf] || {};
                        const selected = selectedState?.uf === uf;
                        return (
                          <path
                            key={uf || name}
                            data-map-kind="state"
                            data-uf={uf || ''}
                            data-name={name || ''}
                            data-state-name={stateRisks[uf]?.state_name || name || uf || ''}
                            data-alerts-count={risk.alerts_count || 0}
                            data-risk-score={risk.risk_score || 0}
                            data-total-value={risk.total_value || 0}
                            d={geometryToPath(feature.geometry)}
                            fill={stateFill(risk.risk_score || 0, selected)}
                            stroke={selected ? '#fecaca' : '#525252'}
                            strokeWidth={selected ? 2.2 : 0.8}
                            className="cursor-pointer transition hover:brightness-125"
                            onClick={() => selectStateOnMap({ uf, name, ...risk })}
                          >
                            <title>{name} - {risk.alerts_count || 0} alertas - risco {risk.risk_score || 0}</title>
                          </path>
                        );
                      })}
                      {(geoJson?.features || []).map((feature) => {
                        const props = feature.properties || {};
                        const uf = props.sigla || props.UF || props.uf || props.SIGLA_UF || IBGE_CODE_TO_UF[props.codarea];
                        if (!uf) return null;
                        const name = props.nome || props.NM_UF || props.name || stateRisks[uf]?.state_name || uf;
                        const risk = stateRisks[uf] || {};
                        const selected = selectedState?.uf === uf;
                        const point = geometryLabelPoint(feature.geometry);
                        if (!point) return null;
                        return (
                          <text
                            key={`${uf}-label`}
                            x={point.x}
                            y={point.y}
                            textAnchor="middle"
                            dominantBaseline="central"
                            className="pointer-events-none select-none text-[18px] font-black"
                            fill={selected || (risk.risk_score || 0) >= 20 ? '#fff7ed' : '#d4d4d4'}
                            stroke="rgba(0,0,0,0.78)"
                            strokeWidth="3"
                            paintOrder="stroke"
                          >
                            {uf}
                            <title>{name}</title>
                          </text>
                        );
                      })}
                      {positionedMapPoints.map((point, index) => {
                        if (!Number.isFinite(Number(point.lat)) || !Number.isFinite(Number(point.lng))) return null;
                        const selected = selectedState?.city === point.city && selectedState?.uf === point.uf;
                        return (
                          <g
                            key={`${point.city}-${point.uf}-${index}`}
                            data-map-kind="city"
                            data-uf={point.uf || ''}
                            data-city={point.city || ''}
                            data-state-name={stateRisks[point.uf]?.state_name || point.uf || ''}
                            data-alerts-count={point.alerts_count || 0}
                            data-risk-score={point.risk_score || 0}
                            data-total-value={point.total_value || 0}
                            data-lat={point.lat || ''}
                            data-lng={point.lng || ''}
                            data-spatial-source={point.spatial_source || ''}
                            className="cursor-pointer"
                            onClick={() => {
                              if (mapClickSuppressRef.current) return;
                              setSelectedState({ ...point, state_name: stateRisks[point.uf]?.state_name || point.uf });
                              setSelectedUf(point.uf || '');
                            }}
                          >
                            <circle
                              cx={point.displayX}
                              cy={point.displayY}
                              r={selected ? 5 : 3.4}
                              fill={selected ? '#ffffff' : '#f87171'}
                              stroke="#7f1d1d"
                              strokeWidth="1.2"
                              opacity="0.92"
                            >
                              <title>{point.city} - {point.uf}: {point.alerts_count || 0} alertas</title>
                            </circle>
                          </g>
                        );
                      })}
                      {mapCityLabelLimit > 0 && positionedMapPoints.slice(0, mapCityLabelLimit).map((point, index) => {
                        if (!Number.isFinite(Number(point.lat)) || !Number.isFinite(Number(point.lng))) return null;
                        const selected = selectedState?.city === point.city && selectedState?.uf === point.uf;
                        return (
                          <text
                            key={`${point.city}-${point.uf}-city-label-${index}`}
                            x={point.displayX + 6}
                            y={point.displayY - 5}
                            textAnchor="start"
                            dominantBaseline="central"
                            pointerEvents="none"
                            fill={selected ? '#ffffff' : '#fecaca'}
                            stroke="rgba(0,0,0,0.8)"
                            strokeWidth={2.6 / mapZoom}
                            paintOrder="stroke"
                            fontSize={Math.max(7, 11 / mapZoom)}
                            fontWeight="800"
                          >
                            {compactText(point.city, mapZoom >= 2 ? 18 : 12)}
                          </text>
                        );
                      })}
                      </g>
                    </svg>
                    <aside className="rounded-lg border border-neutral-800 bg-black/60 p-4 text-sm">
                      <p className="text-xs font-black uppercase text-red-400">Recorte selecionado</p>
                      {selectedState ? (
                        <div className="mt-3 space-y-3">
                          <h3 className="text-xl font-black text-white">{selectedState.city || selectedState.state_name || selectedState.name || selectedState.uf}</h3>
                          <p className="text-neutral-400">
                            {selectedState.city ? `${selectedState.city} - ` : ''}UF {selectedState.uf}
                          </p>
                          <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                            <span className="text-neutral-500">Alertas</span>
                            <strong className="block text-2xl text-white">{selectedState.alerts_count || 0}</strong>
                          </div>
                          <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                            <span className="text-neutral-500">Score de risco</span>
                            <strong className="block text-2xl text-red-400">{selectedState.risk_score || 0}</strong>
                          </div>
                          <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                            <span className="text-neutral-500">Valor monitorado</span>
                            <strong className="block text-white">
                              {Number(selectedState.total_value || 0).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 })}
                            </strong>
                          </div>
                          {Number.isFinite(Number(selectedState.lat)) && Number.isFinite(Number(selectedState.lng)) && (
                            <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                              <span className="text-neutral-500">Coordenadas</span>
                              <strong className="block text-white">
                                {Number(selectedState.lat).toFixed(4)}, {Number(selectedState.lng).toFixed(4)}
                              </strong>
                              <small className="mt-1 block text-neutral-500">{selectedState.spatial_source || 'ibge_uasg_centroid'}</small>
                            </div>
                          )}
                          <button
                            onClick={() => {
                              openStateInFeed(selectedState);
                            }}
                            className="w-full rounded-lg bg-red-600 px-3 py-2 font-bold text-white hover:bg-red-700"
                          >
                            Ver no feed
                          </button>
                        </div>
                      ) : (
                        <p className="mt-3 text-neutral-400">Clique em um estado para visualizar alertas, valor monitorado e score.</p>
                      )}
                    </aside>
                  </div>
                  <div className="absolute bottom-4 left-4 flex max-w-[calc(100%-2rem)] flex-col gap-2">
                    <div className="flex w-fit overflow-hidden rounded-lg border border-neutral-800 bg-black/70">
                      <button
                        type="button"
                        onClick={() => updateMapZoom((current) => current - 0.25)}
                        disabled={mapZoom <= 1}
                        className="flex h-9 w-9 items-center justify-center border-r border-neutral-800 text-lg font-black text-neutral-200 hover:bg-neutral-900 disabled:cursor-not-allowed disabled:opacity-40"
                        title="Reduzir zoom"
                        aria-label="Reduzir zoom"
                      >
                        -
                      </button>
                      <button
                        type="button"
                        onClick={() => updateMapZoom((current) => current + 0.25)}
                        disabled={mapZoom >= 3.2}
                        className="flex h-9 w-9 items-center justify-center text-lg font-black text-neutral-200 hover:bg-neutral-900 disabled:cursor-not-allowed disabled:opacity-40"
                        title="Aumentar zoom"
                        aria-label="Aumentar zoom"
                      >
                        +
                      </button>
                    </div>
                    <div className="w-fit rounded-lg border border-neutral-800 bg-black/70 px-3 py-2 text-xs font-black text-neutral-200">
                      Zoom {Math.round(mapZoom * 100)}%
                    </div>
                    <div className="rounded-lg border border-neutral-800 bg-black/70 p-3 text-xs text-neutral-300">
                      <p className="font-bold text-white">Intensidade</p>
                      <div className="mt-2 flex flex-wrap items-center gap-2">
                      <span className="h-3 w-3 rounded-full bg-yellow-400" /> Baixa
                      <span className="h-3 w-3 rounded-full bg-orange-500" /> Média
                      <span className="h-3 w-3 rounded-full bg-red-500" /> Alta
                    </div>
                  </div>
                </div>
              </div>
              </div>
            ) : activeTab === 'about' ? (
              <div className="mt-5 space-y-5">
                <section className="rounded-lg border border-neutral-800 bg-neutral-900 p-5">
                  <p className="text-xs font-black uppercase text-red-400">Sobre o Coibe</p>
                  <h2 className="mt-1 text-xl font-black text-white">Monitoramento público com dados cruzados</h2>
                  <p className="mt-3 max-w-3xl text-sm leading-6 text-neutral-300">
                    O COIBE organiza dados públicos, compara valores, cruza nomes e mostra sinais que merecem conferência. Ele consulta múltiplas fontes públicas em uma resposta única, para reduzir tempo de pesquisa e facilitar a checagem.
                  </p>
                  <p className="mt-3 max-w-3xl text-sm leading-6 text-neutral-300">
                    O monitor também evolui estratégias de verificação: aprende termos, alvos e métodos inspirados em red flags de contratação pública, OCDS/Open Contracting, PNCP, TCU, CGU, Portal da Transparência, CEIS/CNEP, STF e TSE. A leitura busca sinais como fornecedor único, aditivos, sobrecusto, fracionamento, sanções, relações em grafo e movimentação de alto valor.
                  </p>
                  <p className="mt-3 max-w-3xl text-sm leading-6 text-neutral-300">
                    O mapa de monitoramento cruza UF, municipio, coordenadas e cache local/PostGIS opcional para reduzir chamadas repetidas ao backend em uso multiusuario. O feed tambem aceita filtro direto por cidade, inclusive quando a abertura vem de um ponto do mapa.
                  </p>
                  <div className="mt-5 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                    <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                      <ShieldCheck className="h-5 w-5 text-red-400" />
                      <strong className="mt-2 block text-white">Alerta preventivo</strong>
                      <p className="mt-1 text-xs leading-5 text-neutral-400">Aponta valor alto, diferenca de preco, repeticao ou vinculo textual.</p>
                    </div>
                    <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                      <Activity className="h-5 w-5 text-red-400" />
                      <strong className="mt-2 block text-white">Comparação clara</strong>
                      <p className="mt-1 text-xs leading-5 text-neutral-400">Mostra valor pago, média, diferença em reais e percentual.</p>
                    </div>
                    <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                      <Search className="h-5 w-5 text-red-400" />
                      <strong className="mt-2 block text-white">Fontes em conjunto</strong>
                      <p className="mt-1 text-xs leading-5 text-neutral-400">Reúne contratos, políticos, partidos, doações, despesas e processos em uma leitura.</p>
                    </div>
                    <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                      <FileText className="h-5 w-5 text-red-400" />
                      <strong className="mt-2 block text-white">Fonte verificavel</strong>
                      <p className="mt-1 text-xs leading-5 text-neutral-400">Prioriza links oficiais e dados que podem ser conferidos fora da plataforma.</p>
                    </div>
                  </div>
                </section>

                <section className="grid gap-4 lg:grid-cols-2">
                  <div className="rounded-lg border border-neutral-800 bg-neutral-900 p-5">
                    <p className="text-xs font-black uppercase text-neutral-500">O que a plataforma faz</p>
                    <div className="mt-4 space-y-3 text-sm leading-6 text-neutral-300">
                      <p><strong className="text-white">1. Lê contratos, compras e despesas:</strong> valor, objeto, órgão, fornecedor, CNPJ, data, local, documentos e fonte oficial.</p>
                      <p><strong className="text-white">2. Prioriza riscos de preço:</strong> compara médias da base, referências, aditivos, sobrecusto, fracionamento e padrões numéricos atípicos.</p>
                      <p><strong className="text-white">3. Analisa políticos e partidos:</strong> busca dados públicos sob demanda, cacheia no backend e cruza despesas, contratos, doações, processos e controle externo.</p>
                      <p><strong className="text-white">4. Aprende estratégias novas:</strong> evolui termos, alvos e verificações com red flags de contratação, fontes públicas e padrões de combate à corrupção.</p>
                      <p><strong className="text-white">5. Explica para leigos:</strong> transforma cada achado em parecer claro, com o que foi comprado, quem aparece, valores e motivo da conferência.</p>
                    </div>
                  </div>

                  <div className="rounded-lg border border-neutral-800 bg-neutral-900 p-5">
                    <p className="text-xs font-black uppercase text-neutral-500">Cruzamentos de dados</p>
                    <div className="mt-4 grid gap-2 text-sm text-neutral-300">
                      {[
                        'Contratos x média de mercado encontrada na base',
                        'Fornecedor x órgão público x valor contratado',
                        'Partido/político x contratos, despesas e viagens',
                        'Doações eleitorais x nomes, siglas e fornecedores',
                        'Processos e controle externo x pessoas e entidades',
                        'CEIS/CNEP e sanções x CNPJ e fornecedores recorrentes',
                        'PNCP, Compras.gov.br, TCU, CGU, STF e TSE x evidências oficiais',
                        'Sócios, pessoas próximas e fornecedores x movimentação de alto valor',
                        'Aditivos, prazo, custo e execução x risco de sobrepreço',
                        'Fornecedor único, dispensa, emergencial e baixa competição',
                        'Fracionamento, valores arredondados, Benford e anomalias de rede',
                        'Repetição de fornecedor, valor alto e concentração financeira'
                      ].map((item) => (
                        <p key={item} className="rounded border border-neutral-800 bg-neutral-950 px-3 py-2 leading-5">
                          {item}
                        </p>
                      ))}
                    </div>
                  </div>
                </section>

                <section className="rounded-lg border border-neutral-800 bg-neutral-900 p-5">
                  <p className="text-xs font-black uppercase text-neutral-500">APIs e fontes públicas</p>
                  <div className="mt-4 grid gap-3 md:grid-cols-2">
                    {[
                      ['Compras.gov.br', 'Contratos, compras públicas, valores, fornecedores e órgãos federais.', COMPRAS_CONTRATOS_URL],
                      ['Portal Nacional de Contratações Públicas', 'Editais, atas, contratos e dados nacionais de compras públicas.', 'https://www.gov.br/pncp'],
                      ['Dados Abertos da Câmara', 'Deputados, despesas, viagens, cotas parlamentares e dados legislativos.', 'https://dadosabertos.camara.leg.br/'],
                      ['Senado Federal', 'Senadores, despesas, legislação e dados parlamentares públicos.', 'https://www12.senado.leg.br/dados-abertos'],
                      ['TSE', 'Partidos, candidaturas, contas eleitorais e doações oficiais.', 'https://dadosabertos.tse.jus.br/'],
                      ['STF, TCU e portais oficiais', 'Processos, controle externo, acórdãos e documentos para conferência.', 'https://portal.stf.jus.br/']
                    ].map(([name, text, url]) => (
                      <a
                        key={name}
                        href={url}
                        target="_blank"
                        rel="noreferrer"
                        className="flex min-w-0 items-start justify-between gap-3 rounded border border-neutral-800 bg-neutral-950 p-3 text-sm text-neutral-300 transition hover:border-red-700"
                      >
                        <span className="min-w-0">
                          <strong className="block break-words text-white">{name}</strong>
                          <span className="mt-1 block leading-5 text-neutral-400">{text}</span>
                        </span>
                        <ExternalLink className="mt-1 h-4 w-4 shrink-0 text-red-400" />
                      </a>
                    ))}
                  </div>
                </section>

                <section className="grid gap-4 lg:grid-cols-2">
                  <div className="rounded-lg border border-neutral-800 bg-neutral-900 p-5">
                    <p className="text-xs font-black uppercase text-neutral-500">Como ler os alertas</p>
                    <div className="mt-4 space-y-3 text-sm leading-6 text-neutral-300">
                      <p><strong className="text-red-300">Atenção Alta:</strong> valor, diferença, repetição ou vínculo forte pedem conferência rápida.</p>
                      <p><strong className="text-amber-300">Atenção Média:</strong> existe sinal relevante, mas ainda depende de contexto e documento.</p>
                      <p><strong className="text-neutral-200">Atenção Baixa:</strong> há dado lido, porém sem sinal forte no recorte atual.</p>
                    </div>
                  </div>

                  <div className="rounded-lg border border-neutral-800 bg-neutral-900 p-5">
                    <p className="text-xs font-black uppercase text-neutral-500">Uso responsável dos dados</p>
                    <div className="mt-4 space-y-3 text-sm leading-6 text-neutral-300">
                      <p>O COIBE mostra indícios para orientar checagem. Indício não é acusação.</p>
                      <p>Cada alerta deve ser validado na fonte oficial, com documento, contexto, objeto, prazo e justificativa técnica.</p>
                      <p>Diferença de preço pode ter motivo legítimo: logística, quantidade, urgência, especificação ou local de entrega.</p>
                      <p>A conclusão final cabe à revisão humana e aos órgãos competentes.</p>
                    </div>
                  </div>
                </section>
              </div>
            ) : activeTab === 'donate' ? (
              <div className="mt-5 grid gap-5 lg:grid-cols-[minmax(0,1fr)_360px]">
                <section className="rounded-lg border border-neutral-800 bg-neutral-900 p-5">
                  <p className="text-xs font-black uppercase text-red-400">Doar por Pix</p>
                  <h2 className="mt-1 text-xl font-black text-white">Apoie o COIBE</h2>
                  <p className="mt-3 max-w-2xl text-sm leading-6 text-neutral-300">
                    Sua doação ajuda a manter a plataforma, melhorar as análises e ampliar os cruzamentos com dados públicos.
                  </p>

                  <div className="mt-5 grid gap-3 sm:grid-cols-3">
                    <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                      <strong className="block text-white">Mantém o projeto</strong>
                      <p className="mt-1 text-xs leading-5 text-neutral-400">Ajuda com hospedagem, testes e melhorias.</p>
                    </div>
                    <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                      <strong className="block text-white">Mais fontes</strong>
                      <p className="mt-1 text-xs leading-5 text-neutral-400">Permite integrar novas bases públicas.</p>
                    </div>
                    <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                      <strong className="block text-white">Mais transparência</strong>
                      <p className="mt-1 text-xs leading-5 text-neutral-400">Fortalece uma ferramenta aberta de conferência.</p>
                    </div>
                  </div>

                  <div className="mt-5 rounded-lg border border-neutral-800 bg-neutral-950 p-4 text-sm leading-6 text-neutral-300">
                    <p><strong className="text-white">Como doar:</strong> escaneie o QR Code ou copie a chave Pix.</p>
                    <p className="mt-2"><strong className="text-white">Nome:</strong> JOAO PEDRO ANDRE</p>
                    <p><strong className="text-white">Chave Pix:</strong> 0c81c958-1ab4-4cd1-a68f-fe6ab841944c</p>
                  </div>
                </section>

                <aside className="rounded-lg border border-neutral-800 bg-neutral-900 p-5 lg:self-start">
                  <p className="text-xs font-black uppercase text-neutral-500">QR Code Pix</p>
                  <div className="mt-3 overflow-hidden rounded-lg border border-neutral-800 bg-white p-2">
                    <img
                      src="/pix-chave.jpg"
                      alt="QR Code Pix para doação ao COIBE"
                      className="h-auto w-full"
                    />
                  </div>
                  <button
                    type="button"
                    onClick={() => navigator.clipboard?.writeText('0c81c958-1ab4-4cd1-a68f-fe6ab841944c')}
                    className="mt-4 flex w-full items-center justify-center rounded-lg bg-red-600 px-4 py-3 text-sm font-black text-white transition hover:bg-red-700"
                  >
                    Copiar chave Pix
                  </button>
                  <p className="mt-3 text-center text-xs leading-5 text-neutral-500">
                    Obrigado por apoiar o desenvolvimento do COIBE.
                  </p>
                </aside>
              </div>
            ) : (
              <div className="mt-4 space-y-3 sm:mt-5 sm:space-y-4">
                <div className="rounded-lg border border-neutral-800 bg-neutral-900 p-5">
                  <p className="text-xs font-black uppercase text-red-400">
                    {activeTab === 'parties' ? 'Partidos' : 'Políticos'}
                  </p>
                  <h2 className="mt-1 font-black text-white">
                    Varredura de dinheiro público e fatores de atenção
                  </h2>
                  <p className="mt-2 text-sm leading-6 text-neutral-400">
                    Leitura preventiva com dados oficiais. A plataforma mostra pontos para conferir, sem afirmar culpa ou irregularidade.
                  </p>
                </div>

                <div className="grid gap-3 rounded-lg border border-neutral-800 bg-neutral-900 p-3 md:grid-cols-2 xl:grid-cols-[minmax(0,1.4fr)_minmax(0,0.85fr)_minmax(0,0.85fr)_minmax(0,1fr)]">
                  <label className="flex min-w-0 items-center gap-3 rounded-lg border border-neutral-800 bg-neutral-950 px-3 py-2">
                    <Search className="h-4 w-4 shrink-0 text-red-400" />
                    <span className="min-w-0 flex-1">
                      <span className="block text-[11px] font-black uppercase text-neutral-500">Pesquisar</span>
                      <input
                        value={politicalSearch}
                        onChange={(event) => setPoliticalSearch(event.target.value)}
                        placeholder={activeTab === 'parties' ? 'partido, sigla, pessoa' : 'nome, partido, fornecedor'}
                        className="mt-1 w-full bg-transparent text-sm font-bold text-white outline-none placeholder:text-neutral-600"
                      />
                    </span>
                  </label>
                  <label className="flex min-w-0 items-center gap-3 rounded-lg border border-neutral-800 bg-neutral-950 px-3 py-2">
                    <Filter className="h-4 w-4 shrink-0 text-red-400" />
                    <span className="min-w-0 flex-1">
                      <span className="block text-[11px] font-black uppercase text-neutral-500">Fatores de risco</span>
                      <select
                        value={politicalRiskFilter}
                        onChange={(event) => setPoliticalRiskFilter(event.target.value)}
                        className="mt-1 w-full bg-transparent text-sm font-bold text-white outline-none"
                      >
                        <option className="bg-neutral-950" value="todos">Todos</option>
                        <option className="bg-neutral-950" value="alto">Alto</option>
                        <option className="bg-neutral-950" value="médio">Medio</option>
                        <option className="bg-neutral-950" value="baixo">Baixo</option>
                      </select>
                    </span>
                  </label>
                  <label className="flex min-w-0 items-center gap-3 rounded-lg border border-neutral-800 bg-neutral-950 px-3 py-2">
                    <ArrowDownWideNarrow className="h-4 w-4 shrink-0 text-red-400" />
                    <span className="min-w-0 flex-1">
                      <span className="block text-[11px] font-black uppercase text-neutral-500">Tamanho</span>
                      <select
                        value={politicalSizeOrder}
                        onChange={(event) => setPoliticalSizeOrder(event.target.value)}
                        className="mt-1 w-full bg-transparent text-sm font-bold text-white outline-none"
                      >
                        <option className="bg-neutral-950" value="prioridade">Prioridade pública</option>
                        <option className="bg-neutral-950" value="valor">Maior dinheiro publico</option>
                        <option className="bg-neutral-950" value="viagens">Maior valor em viagens</option>
                        <option className="bg-neutral-950" value="registros">Mais registros</option>
                        <option className="bg-neutral-950" value="risco">Maior risco</option>
                      </select>
                    </span>
                  </label>
                  <label className="flex min-w-0 items-center gap-3 rounded-lg border border-neutral-800 bg-neutral-950 px-3 py-2">
                    <FileText className="h-4 w-4 shrink-0 text-red-400" />
                    <span className="min-w-0 flex-1">
                      <span className="block text-[11px] font-black uppercase text-neutral-500">Tipo</span>
                      <select
                        value={politicalTypeFilter}
                        onChange={(event) => setPoliticalTypeFilter(event.target.value)}
                        className="mt-1 w-full bg-transparent text-sm font-bold text-white outline-none"
                      >
                        {Object.entries(POLITICAL_TYPE_LABELS).map(([value, label]) => (
                          <option key={value} className="bg-neutral-950" value={value}>{label}</option>
                        ))}
                      </select>
                    </span>
                  </label>
                </div>

                {loadingPolitical && politicalCurrentItems.length === 0 && (
                  <div className="flex h-44 items-center justify-center rounded-lg border border-neutral-800 bg-neutral-900 text-neutral-400">
                    <Loader2 className="mr-2 h-5 w-5 animate-spin" />
                    Carregando base ja analisada...
                  </div>
                )}

                {filteredPoliticalItems.map((item) => {
                  const risk = riskCopy[item.attention_level] || riskCopy.baixo;
                  const analyzedDate = item.analyzed_at || politicalDataStampRef.current[activeTab];
                  const nonTravelMoney = Math.max(Number(item.total_public_money || 0) - Number(item.travel_public_money || 0), 0);
                  return (
                    <button
                      key={`${item.type}-${item.id}`}
                      type="button"
                      onClick={() => setSelectedPoliticalItem(item)}
                      className="w-full rounded-lg border border-neutral-800 bg-neutral-900 p-5 text-left transition hover:border-red-700"
                    >
                      <div className="flex items-start justify-between gap-4">
                        <div>
                          <h3 className="text-lg font-black text-white">{item.name}</h3>
                          {item.subtitle && <p className="mt-1 text-sm text-neutral-400">{item.subtitle}</p>}
                          {analyzedDate && <p className="mt-1 text-xs font-bold uppercase text-neutral-500">Analisado em {formatDate(analyzedDate)}</p>}
                        </div>
                        <span className={`shrink-0 rounded-full border px-2 py-1 text-[11px] font-black sm:px-3 sm:text-xs ${risk.color}`}>
                          {risk.label}
                        </span>
                      </div>
                      <p className="mt-3 text-sm leading-6 text-neutral-300">{item.summary}</p>
                      <div className="mt-4 grid gap-3 rounded-lg border border-neutral-800 bg-neutral-950/70 p-3 sm:grid-cols-3">
                        <div>
                          <p className="text-xs text-neutral-500">Dinheiro público total</p>
                          <strong className="text-white">{compactValue(item.total_public_money, 'value')}</strong>
                        </div>
                        <div>
                          <p className="text-xs text-neutral-500">Pagamentos/contratos</p>
                          <strong className="text-white">{compactValue(nonTravelMoney, 'value')}</strong>
                        </div>
                        <div>
                          <p className="text-xs text-neutral-500">Registros lidos</p>
                          <strong className="text-white">{Number(item.records_count || 0).toLocaleString('pt-BR')}</strong>
                        </div>
                      </div>
                      {item.people?.length > 0 && (
                        <p className="mt-3 text-xs text-neutral-500">
                          Envolvidos no recorte: {item.people.slice(0, 4).join(', ')}
                        </p>
                      )}
                      {item.analysis_types?.length > 0 && (
                        <p className="mt-2 text-xs text-neutral-500">
                          Tipos: {item.analysis_types.slice(0, 5).map(politicalTypeLabel).join(', ')}
                        </p>
                      )}
                    </button>
                  );
                })}

                <div ref={loadMorePoliticalRef} className="h-1" />
                {politicalCurrentItems.length > 0 && (
                  <button
                    type="button"
                    onClick={() => loadPoliticalData(activeTab, true, currentPoliticalPagination.page + 1, true)}
                    disabled={!currentPoliticalPagination.hasMore || loadingPolitical}
                    className="flex w-full items-center justify-center rounded-lg border border-neutral-800 bg-neutral-900 px-4 py-3 text-sm font-bold text-neutral-300 transition hover:border-red-700 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {loadingPolitical ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                    {currentPoliticalPagination.hasMore ? 'Carregar mais registros' : 'Base carregada'}
                  </button>
                )}

                {!loadingPolitical && filteredPoliticalItems.length === 0 && (
                  <div className="rounded-lg border border-neutral-800 bg-neutral-900 p-6 text-sm text-neutral-400">
                    Nenhum registro consolidado ainda. A varredura em segundo plano vai preencher esta aba.
                  </div>
                )}
              </div>
            )}
          </div>

          {activeTab === 'feed' && (
          <aside className="coibe-feed-analysis-panel hidden rounded-lg border border-neutral-800 bg-neutral-900 lg:block lg:overflow-hidden">
            {selectedAlert ? (
              <div className="flex h-full min-h-0 flex-col">
                <div className={`rounded-t-lg px-5 py-4 ${selectedRisk.panel}`}>
                  <h2 className="flex items-center gap-2 text-sm font-black text-red-200">
                    <AlertTriangle className="h-5 w-5" />
                    Parecer Analítico do COIBE
                  </h2>
                </div>
                <div className="min-h-0 flex-1 overflow-y-auto p-5">
                  <button
                    type="button"
                    onClick={() => setShowFullAlertTitle((current) => !current)}
                    title={selectedAlert.title}
                    className="block w-full text-left font-black leading-snug text-white hover:text-red-100"
                    aria-expanded={showFullAlertTitle}
                  >
                    {selectedAlert.title}
                  </button>
                  <p className="mt-1 text-sm text-neutral-400">Id. {selectedAlert.report.id}</p>
                  <p className="mt-3 text-sm leading-6 text-neutral-300">{selectedAlertDescription}</p>

                  {selectedAlertMetrics.length > 0 && (
                    <div className="mt-5 grid gap-2 sm:grid-cols-2">
                      {selectedAlertMetrics.map(([label, value]) => (
                        <div key={`${label}-${value}`} className="rounded border border-neutral-800 bg-neutral-950/70 p-3">
                          <p className="text-[11px] font-black uppercase text-neutral-500">{label}</p>
                          <strong className="mt-1 block break-words text-sm text-white">{value}</strong>
                        </div>
                      ))}
                    </div>
                  )}

                  <div className="mt-5 rounded-lg border border-neutral-800 bg-neutral-950/70 p-4">
                    <p className="text-xs font-bold uppercase text-neutral-500">Fontes oficiais reais</p>
                    <div className="mt-3 space-y-2">
                      {selectedOfficialSources.map((source) => (
                        <a
                          key={`${source.label}-${source.url}`}
                          href={source.url}
                          target="_blank"
                          rel="noreferrer"
                          className="flex items-start justify-between gap-2 rounded border border-neutral-800 bg-neutral-900 p-2 text-sm text-neutral-200 hover:border-red-700"
                        >
                          <span>
                            <strong className="block">{source.label}</strong>
                            <small className="text-neutral-500">{source.kind}</small>
                          </span>
                          <ExternalLink className="mt-1 h-4 w-4 shrink-0 text-red-400" />
                        </a>
                      ))}
                    </div>
                  </div>

                  {selectedPublicEvidence.length > 0 && (
                    <div className="mt-5 rounded-lg border border-neutral-800 bg-neutral-950/70 p-4">
                      <p className="text-xs font-bold uppercase text-neutral-500">Evidencias publicas cruzadas</p>
                      <div className="mt-3 grid gap-2">
                        {selectedPublicEvidence.slice(0, 6).map((evidence, index) => (
                          <a
                            key={`${evidence.record_type}-${index}`}
                            href={evidence.url}
                            target="_blank"
                            rel="noreferrer"
                            className="rounded border border-neutral-800 bg-neutral-900 p-2 text-sm text-neutral-200 hover:border-red-700"
                          >
                            <strong className="block">{evidence.title || evidence.source}</strong>
                            <small className="text-neutral-500">
                              {evidence.source || 'Fonte publica'} - {Number(evidence.matches_count || 0).toLocaleString('pt-BR')} registro(s)
                            </small>
                          </a>
                        ))}
                      </div>
                    </div>
                  )}

                  <h4 className="mt-5 text-xs font-black uppercase text-neutral-500">Risco e detalhes</h4>
                  <ul className="mt-3 space-y-3">
                    {selectedAlertFlags.map((flag) => {
                      const comparisons = friendlyComparison(flag);
                      const details = flagDetails(flag);
                      return (
                        <li key={`${flag.code}-${flag.title}`} className="flex gap-3 rounded-lg border border-red-900/70 bg-red-950/20 p-3 text-sm text-neutral-200">
                          <AlertCircle className="mt-0.5 h-5 w-5 shrink-0 text-red-500" />
                          <span>
                            <strong className="block text-white">{flag.title}</strong>
                            <span className="mt-1 block leading-6 text-neutral-300">{compactText(flag.message, 170)}</span>
                            {comparisons.length > 0 && (
                              <span className="mt-3 grid gap-1 text-xs font-semibold text-red-100">
                                {comparisons.map((comparison) => (
                                  <span key={`${flag.code}-${comparison}`} className="rounded border border-red-900/60 bg-red-950/30 px-2 py-1">
                                    {comparison}
                                  </span>
                                ))}
                              </span>
                            )}
                            {details.length > 0 && (
                              <span className="mt-3 grid gap-1 text-xs text-neutral-400">
                                {details.map(([key, value]) => (
                                  <span key={`${flag.code}-${key}`} className="rounded border border-neutral-800 bg-neutral-950/70 px-2 py-1">
                                    <strong className="text-neutral-300">{readableKey(key)}:</strong> {compactValue(value, key)}
                                  </span>
                                ))}
                              </span>
                            )}
                          </span>
                        </li>
                      );
                    })}
                  </ul>

                  <div className="mt-5 space-y-3">
                    <a
                      href={selectedOfficialSources[0]?.url}
                      target="_blank"
                      rel="noreferrer"
                      className="flex w-full items-center justify-center gap-2 rounded-lg bg-red-600 px-4 py-3 text-sm font-black text-white transition hover:bg-red-700"
                    >
                      <FileText className="h-4 w-4" />
                      Acessar Fontes Oficiais
                    </a>
                    <button
                      onClick={() => downloadReport(selectedAlert)}
                      className="flex w-full items-center justify-center gap-2 rounded-lg border border-neutral-700 px-4 py-3 text-sm font-bold text-neutral-200 transition hover:bg-neutral-800"
                    >
                      <Download className="h-4 w-4" />
                      Exportar Relatório Técnico
                    </button>
                  </div>

                  <div className="mt-5 rounded-lg border border-neutral-800 bg-neutral-950 p-3 text-center text-xs leading-5 text-neutral-400">
                    <strong className="text-neutral-300">Nota:</strong> O COIBE aponta sinais em dados abertos. Conclusão oficial cabe aos órgãos de controle.
                  </div>
                </div>
              </div>
            ) : (
              <div className="p-8 text-center">
                <Target className="mx-auto h-16 w-16 text-neutral-700" />
                <h2 className="mt-4 font-black text-white">Painel de Análise COIBE</h2>
                <p className="mt-2 text-sm text-neutral-400">Selecione um item real no feed ou no mapa para visualizar fontes oficiais e relatório técnico.</p>
              </div>
            )}
          </aside>
          )}
        </section>
        {selectedAlert && activeTab === 'feed' && (
          <div
            className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-black/75 p-2 lg:hidden"
            onClick={() => setSelectedAlert(null)}
          >
            <div
              className="my-2 max-h-[calc(100dvh-1rem)] w-full max-w-[calc(100vw-1rem)] overflow-y-auto rounded-lg border border-neutral-800 bg-neutral-950 shadow-2xl"
              onClick={(event) => event.stopPropagation()}
            >
              <div className={`rounded-t-lg px-4 py-3 ${selectedRisk.panel}`}>
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <p className="text-xs font-black uppercase text-red-200">Parecer Analítico do COIBE</p>
                    <button
                      type="button"
                      onClick={() => setShowFullAlertTitle((current) => !current)}
                      title={selectedAlert.title}
                      className="mt-2 block w-full text-left text-base font-black leading-snug text-white hover:text-red-100"
                      aria-expanded={showFullAlertTitle}
                    >
                      {selectedAlert.title}
                    </button>
                    <p className="mt-1 text-xs text-neutral-400">Id. {selectedAlert.report.id}</p>
                  </div>
                  <button
                    type="button"
                    onClick={() => setSelectedAlert(null)}
                    className="shrink-0 rounded border border-neutral-700 px-3 py-1 text-sm font-bold text-neutral-300 hover:bg-neutral-800"
                  >
                    Fechar
                  </button>
                </div>
              </div>

              <div className="space-y-4 p-4">
                <p className="text-sm leading-6 text-neutral-300">{selectedAlertDescription}</p>

                {selectedAlertMetrics.length > 0 && (
                  <div className="grid gap-2">
                    {selectedAlertMetrics.slice(0, 6).map(([label, value]) => (
                      <div key={`mobile-${label}-${value}`} className="rounded border border-neutral-800 bg-neutral-900 p-3">
                        <p className="text-[11px] font-black uppercase text-neutral-500">{label}</p>
                        <strong className="mt-1 block break-words text-sm text-white">{value}</strong>
                      </div>
                    ))}
                  </div>
                )}

                {selectedAlertFlags.length > 0 && (
                  <section>
                    <h4 className="text-xs font-black uppercase text-neutral-500">Risco e detalhes</h4>
                    <div className="mt-2 space-y-2">
                      {selectedAlertFlags.slice(0, 4).map((flag) => {
                        const comparisons = friendlyComparison(flag);
                        return (
                          <div key={`mobile-${flag.code}-${flag.title}`} className="rounded border border-red-900/70 bg-red-950/20 p-3 text-sm text-neutral-200">
                            <strong className="block text-white">{flag.title}</strong>
                            <p className="mt-1 leading-6 text-neutral-300">{compactText(flag.message, 150)}</p>
                            {comparisons.length > 0 && (
                              <div className="mt-2 grid gap-1 text-xs font-semibold text-red-100">
                                {comparisons.slice(0, 3).map((comparison) => (
                                  <span key={`mobile-${flag.code}-${comparison}`} className="rounded border border-red-900/60 bg-red-950/30 px-2 py-1">
                                    {comparison}
                                  </span>
                                ))}
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  </section>
                )}

                {selectedOfficialSources.length > 0 && (
                  <section className="rounded-lg border border-neutral-800 bg-neutral-900 p-3">
                    <p className="text-xs font-black uppercase text-neutral-500">Fontes oficiais</p>
                    <div className="mt-2 grid gap-2">
                      {selectedOfficialSources.slice(0, 3).map((source) => (
                        <a
                          key={`mobile-${source.label}-${source.url}`}
                          href={source.url}
                          target="_blank"
                          rel="noreferrer"
                          className="flex min-w-0 items-start justify-between gap-2 rounded border border-neutral-800 bg-neutral-950 p-2 text-sm text-neutral-200 hover:border-red-700"
                        >
                          <span className="min-w-0">
                            <strong className="block break-words">{source.label}</strong>
                            <small className="text-neutral-500">{source.kind}</small>
                          </span>
                          <ExternalLink className="mt-1 h-4 w-4 shrink-0 text-red-400" />
                        </a>
                      ))}
                    </div>
                  </section>
                )}

                {selectedPublicEvidence.length > 0 && (
                  <section className="rounded-lg border border-neutral-800 bg-neutral-900 p-3">
                    <p className="text-xs font-black uppercase text-neutral-500">Evidências cruzadas</p>
                    <div className="mt-2 grid gap-2">
                      {selectedPublicEvidence.slice(0, 3).map((evidence, index) => (
                        <a
                          key={`mobile-${evidence.record_type}-${index}`}
                          href={evidence.url}
                          target="_blank"
                          rel="noreferrer"
                          className="rounded border border-neutral-800 bg-neutral-950 p-2 text-sm text-neutral-200 hover:border-red-700"
                        >
                          <strong className="block break-words">{evidence.title || evidence.source}</strong>
                          <small className="text-neutral-500">
                            {evidence.source || 'Fonte pública'} - {Number(evidence.matches_count || 0).toLocaleString('pt-BR')} registro(s)
                          </small>
                        </a>
                      ))}
                    </div>
                  </section>
                )}

                <div className="rounded-lg border border-neutral-800 bg-neutral-900 p-3 text-center text-xs leading-5 text-neutral-400">
                  O COIBE aponta sinais em dados abertos. A conclusão oficial cabe aos órgãos de controle.
                </div>
              </div>
            </div>
          </div>
        )}
        {selectedSearchResult && selectedSearchReview && (
          <div
            className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-black/75 p-2 sm:items-center sm:p-4"
            onClick={() => setSelectedSearchResult(null)}
          >
            <div
              className="my-2 max-h-[calc(100dvh-1rem)] w-full max-w-4xl overflow-hidden rounded-lg border border-neutral-800 bg-neutral-950 shadow-2xl sm:my-0 sm:max-h-[90vh]"
              onClick={(event) => event.stopPropagation()}
            >
              <div className="flex items-start justify-between gap-4 border-b border-neutral-800 p-5">
                <div className="min-w-0">
                  <p className="text-xs font-black uppercase text-red-400">Busca unificada</p>
                  <h2 className="mt-1 text-lg font-black text-white">Risco de Superfaturamento, políticos, partidos, STF, CNPJ e contratos</h2>
                  <p className="mt-2 text-sm text-neutral-400">{selectedSearchResult.source}</p>
                </div>
                <button
                  type="button"
                  onClick={() => setSelectedSearchResult(null)}
                  className="shrink-0 rounded border border-neutral-700 px-3 py-1 text-sm font-bold text-neutral-300 hover:bg-neutral-800"
                >
                  Fechar
                </button>
              </div>

              <div className="grid max-h-[calc(90vh-92px)] gap-5 overflow-y-auto p-5 lg:grid-cols-[minmax(0,1fr)_320px]">
                <section className="space-y-4">
                  <div className="rounded-lg border border-neutral-800 bg-neutral-900 p-4">
                    <span className="rounded border border-red-900 bg-red-950/40 px-2 py-1 text-[11px] font-black uppercase text-red-300">
                      {selectedSearchResult.type.replaceAll('_', ' ')}
                    </span>
                    <h3 title={selectedSearchResult.title} className="mt-3 break-words text-xl font-black text-white">{selectedSearchResult.title}</h3>
                    {selectedSearchResult.subtitle && <p title={selectedSearchResult.subtitle} className="mt-2 text-sm leading-6 text-neutral-300">{selectedSearchResult.subtitle}</p>}
                  </div>

                  <div className="rounded-lg border border-red-900/60 bg-red-950/20 p-4">
                    <h3 className="flex items-center gap-2 text-sm font-black text-red-100">
                      <AlertTriangle className="h-4 w-4 text-red-400" />
                      Parecer Analítico do COIBE
                    </h3>
                    <strong className="mt-3 block text-white">{selectedSearchReview.title}</strong>
                    <p className="mt-2 text-sm leading-6 text-neutral-300">{selectedSearchDescription}</p>
                  </div>

                  <div className="rounded-lg border border-neutral-800 bg-neutral-900 p-4">
                    <p className="text-xs font-black uppercase text-neutral-500">Riscos e detalhes do resultado</p>
                    <div className="mt-3 space-y-3">
                      {selectedSearchReview.flags.map((flag) => {
                        const flagRisk = riskCopy[flag.level] || riskCopy[flag.risk_level] || riskCopy.baixo;
                        const comparisons = friendlyComparison(flag);
                        return (
                          <div key={`${flag.code}-${flag.title}`} className="rounded border border-red-900/60 bg-red-950/20 p-3">
                            <div className="flex flex-wrap items-center justify-between gap-2">
                              <strong className="text-sm text-white">{flag.title}</strong>
                              <span className={`rounded-full border px-2 py-0.5 text-[11px] font-black ${flagRisk.color}`}>
                                {flagRisk.label}
                              </span>
                            </div>
                            <p className="mt-2 text-sm leading-6 text-neutral-300">{compactText(flag.message, 160)}</p>
                            {comparisons.length > 0 && (
                              <div className="mt-3 grid gap-1 text-xs font-semibold text-red-100">
                                {comparisons.map((comparison) => (
                                  <span key={`${flag.code}-${comparison}`} className="rounded border border-red-900/60 bg-red-950/30 px-2 py-1">
                                    {comparison}
                                  </span>
                                ))}
                              </div>
                            )}
                            {flagDetails(flag).length > 0 && (
                              <div className="mt-3 grid gap-1 text-xs text-neutral-400">
                                {flagDetails(flag).map(([key, value]) => (
                                  <span key={`${flag.code}-${key}`} className="rounded border border-neutral-800 bg-neutral-950/70 px-2 py-1">
                                    <strong className="text-neutral-300">{readableKey(key)}:</strong> {compactValue(value, key)}
                                  </span>
                                ))}
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  </div>

                  <div className="rounded-lg border border-neutral-800 bg-neutral-900 p-4">
                    <p className="text-xs font-black uppercase text-neutral-500">Como ler este resultado</p>
                    <div className="mt-3 space-y-2">
                      {selectedSearchReview.checks.map((check) => (
                        <p key={check} className="rounded border border-neutral-800 bg-neutral-950 px-3 py-2 text-sm leading-6 text-neutral-300">
                          {check}
                        </p>
                      ))}
                    </div>
                  </div>
                </section>

                <aside className="space-y-4 rounded-lg border border-neutral-800 bg-neutral-900 p-4 lg:sticky lg:top-0 lg:self-start">
                  <div>
                    <p className="text-xs font-black uppercase text-red-400">Dados do resultado</p>
                    <h3 className="mt-1 font-black text-white">Resumo rápido</h3>
                  </div>
                  <div className="grid gap-2">
                    <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                      <p className="text-xs text-neutral-500">Tipo</p>
                      <strong className="text-white">{selectedSearchResult.type.replaceAll('_', ' ')}</strong>
                    </div>
                    <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                      <p className="text-xs text-neutral-500">Risco</p>
                      <strong className="text-white">{riskCopy[selectedSearchResult.risk_level]?.label || selectedSearchResult.risk_level || 'Baixo'}</strong>
                    </div>
                    {selectedSearchReview.metrics.map(([label, value]) => (
                      <div key={`${label}-${value}`} className="rounded border border-neutral-800 bg-neutral-950 p-3">
                        <p className="text-xs text-neutral-500">{label}</p>
                        <strong className="break-words text-white">{value}</strong>
                      </div>
                    ))}
                  </div>
                  {selectedSearchResult.url && (
                    <a
                      href={selectedSearchResult.url}
                      target="_blank"
                      rel="noreferrer"
                      className="flex w-full items-center justify-center gap-2 rounded bg-red-600 px-3 py-2 text-sm font-black text-white hover:bg-red-500"
                    >
                      Abrir fonte oficial <ExternalLink className="h-4 w-4" />
                    </a>
                  )}
                  <button
                    type="button"
                    onClick={() => applySearchResult(selectedSearchResult)}
                    className="flex w-full items-center justify-center gap-2 rounded border border-neutral-700 px-3 py-2 text-sm font-bold text-neutral-200 hover:bg-neutral-800"
                  >
                    Aplicar no feed <ChevronRight className="h-4 w-4 text-red-400" />
                  </button>
                </aside>
              </div>
            </div>
          </div>
        )}
        {selectedPoliticalItem && (
          <div
            className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-black/75 p-2 sm:items-center sm:p-4"
            onClick={() => setSelectedPoliticalItem(null)}
          >
            <div
              className="my-2 max-h-[calc(100dvh-1rem)] w-full max-w-6xl overflow-y-auto rounded-lg border border-neutral-800 bg-neutral-950 shadow-2xl sm:my-0 sm:max-h-[90vh]"
              onClick={(event) => event.stopPropagation()}
            >
              <div className="flex items-start justify-between gap-3 border-b border-neutral-800 p-4 sm:gap-4 sm:p-5">
                <div className="min-w-0">
                  <p className="text-xs font-black uppercase text-red-400">
                    {selectedPoliticalItem.type === 'partido' ? 'Partido' : 'Político'}
                  </p>
                  <h2 className="mt-1 break-words text-lg font-black text-white sm:text-xl">{selectedPoliticalItem.name}</h2>
                  {selectedPoliticalItem.subtitle && <p className="mt-1 text-sm text-neutral-400">{selectedPoliticalItem.subtitle}</p>}
                  {(selectedPoliticalItem.analyzed_at || politicalDataStampRef.current[activeTab]) && (
                    <p className="mt-1 text-xs font-bold uppercase text-neutral-500">
                      Analisado em {formatDate(selectedPoliticalItem.analyzed_at || politicalDataStampRef.current[activeTab])}
                    </p>
                  )}
                </div>
                <button
                  type="button"
                  onClick={() => setSelectedPoliticalItem(null)}
                  className="shrink-0 rounded border border-neutral-700 px-3 py-1 text-sm font-bold text-neutral-300 hover:bg-neutral-800"
                >
                  Fechar
                </button>
              </div>

              <div className="grid gap-5 p-5 lg:grid-cols-[minmax(0,1fr)_360px]">
                <section className="space-y-5">
                <p className="text-sm leading-6 text-neutral-300">{compactText(selectedPoliticalItem.summary, 210)}</p>
                {selectedPoliticalItem.analysis_types?.length > 0 && (
                  <div className="flex flex-wrap gap-2">
                    {selectedPoliticalItem.analysis_types.map((type) => (
                      <span key={type} className="rounded border border-neutral-800 bg-neutral-900 px-2 py-1 text-xs font-bold text-neutral-300">
                        {politicalTypeLabel(type)}
                      </span>
                    ))}
                  </div>
                )}
                <div className="grid gap-3 sm:grid-cols-3">
                  <div className="rounded border border-neutral-800 bg-neutral-900 p-3">
                    <p className="text-xs text-neutral-500">Dinheiro público total</p>
                    <strong className="text-white">{compactValue(selectedPoliticalItem.total_public_money, 'value')}</strong>
                  </div>
                  <div className="rounded border border-neutral-800 bg-neutral-900 p-3">
                    <p className="text-xs text-neutral-500">Pagamentos/contratos</p>
                    <strong className="text-white">{compactValue(selectedPoliticalMetrics?.nonTravelMoney || 0, 'value')}</strong>
                  </div>
                  <div className="rounded border border-neutral-800 bg-neutral-900 p-3">
                    <p className="text-xs text-neutral-500">Atenção</p>
                    <strong className="text-white">{riskCopy[selectedPoliticalItem.attention_level]?.label || selectedPoliticalItem.attention_level}</strong>
                  </div>
                </div>

                {selectedPoliticalItem.analysis_details?.length > 0 && (
                  <section>
                    <div className="mb-3 grid gap-3 rounded-lg border border-neutral-800 bg-neutral-900 p-3 md:grid-cols-[180px_minmax(0,1fr)]">
                      <label className="flex min-w-0 items-center gap-3 rounded border border-neutral-800 bg-neutral-950 px-3 py-2">
                        <Filter className="h-4 w-4 shrink-0 text-red-400" />
                        <span className="min-w-0 flex-1">
                          <span className="block text-[11px] font-black uppercase text-neutral-500">Risco</span>
                          <select
                            value={politicalDetailRiskFilter}
                            onChange={(event) => setPoliticalDetailRiskFilter(event.target.value)}
                            className="mt-1 w-full bg-transparent text-sm font-bold text-white outline-none"
                          >
                            <option className="bg-neutral-950" value="todos">Todos</option>
                            <option className="bg-neutral-950" value="alto">Alto</option>
                            <option className="bg-neutral-950" value="medio">Medio</option>
                            <option className="bg-neutral-950" value="baixo">Baixo</option>
                          </select>
                        </span>
                      </label>
                      <label className="flex min-w-0 items-center gap-3 rounded border border-neutral-800 bg-neutral-950 px-3 py-2">
                        <Search className="h-4 w-4 shrink-0 text-red-400" />
                        <span className="min-w-0 flex-1">
                          <span className="block text-[11px] font-black uppercase text-neutral-500">Analises cacheadas</span>
                          <input
                            value={politicalDetailSearch}
                            onChange={(event) => setPoliticalDetailSearch(event.target.value)}
                            placeholder="Buscar descricao, fornecedor, pessoa ou orgao"
                            className="mt-1 w-full bg-transparent text-sm font-bold text-white outline-none placeholder:text-neutral-600"
                          />
                        </span>
                      </label>
                    </div>
                    <h3 className="text-xs font-black uppercase text-neutral-500">Descrições da análise</h3>
                    <div className="mt-3 space-y-3">
                      {pagedPoliticalDetails.map((detail, index) => {
                        const absoluteDetailIndex = (selectedPoliticalDetailPage - 1) * POLITICAL_DETAIL_PAGE_SIZE + index;
                        const isSelectedDetail = absoluteDetailIndex === selectedPoliticalDetailIndex;
                        return (
                        <div
                          key={`${detail.title || detail.type}-${index}`}
                          role="button"
                          tabIndex={0}
                          onClick={() => setSelectedPoliticalDetailIndex(absoluteDetailIndex)}
                          onKeyDown={(event) => {
                            if (event.key === 'Enter' || event.key === ' ') {
                              event.preventDefault();
                              setSelectedPoliticalDetailIndex(absoluteDetailIndex);
                            }
                          }}
                          className={`cursor-pointer rounded border p-3 text-left text-sm text-neutral-200 transition ${isSelectedDetail ? 'border-red-700 bg-red-950/20' : 'border-neutral-800 bg-neutral-900 hover:border-red-800'}`}
                        >
                          {(() => {
                            const superpricingRisk = politicalSuperpricingRisk(detail);
                            return (
                              <div className="mb-3 rounded border border-neutral-800 bg-neutral-950 p-2">
                                <div className="flex flex-wrap items-center justify-between gap-2">
                                  <span className="text-[11px] font-black uppercase text-neutral-500">Risco de superfaturamento no registro</span>
                                  <span className={`rounded-full border px-2 py-0.5 text-[11px] font-black ${superpricingRisk.color}`}>
                                    {superpricingRisk.label}
                                  </span>
                                </div>
                                <p className="mt-1 text-xs leading-5 text-neutral-400">{compactText(superpricingRisk.message, 120)}</p>
                                {(superpricingRisk.score > 0 || superpricingRisk.value > 0) && (
                                  <div className="mt-2 flex flex-wrap gap-2 text-[11px] text-neutral-500">
                                    {superpricingRisk.score > 0 && <span>Score {superpricingRisk.score.toLocaleString('pt-BR')}</span>}
                                    {superpricingRisk.value > 0 && <span>Valor {compactValue(superpricingRisk.value, 'value')}</span>}
                                  </div>
                                )}
                              </div>
                            );
                          })()}
                          <div className="flex flex-wrap items-start justify-between gap-2">
                            <strong className="text-white">{detail.title || politicalTypeLabel(detail.type)}</strong>
                            <span className="rounded-full border border-neutral-700 bg-neutral-950 px-2 py-0.5 text-[11px] font-black text-neutral-300">
                              {politicalTypeLabel(detail.type)}
                            </span>
                          </div>
                          {detail.description && <p className="mt-2 leading-6 text-neutral-300">{compactText(detail.description, 180)}</p>}
                          <div className="mt-3 grid gap-2 text-xs text-neutral-400 sm:grid-cols-2">
                            <p><strong className="text-neutral-300">Data:</strong> {detail.date ? formatDate(detail.date) : detail.month && detail.year ? `${String(detail.month).padStart(2, '0')}/${detail.year}` : 'Não informada'}</p>
                            {detail.value !== undefined && detail.value !== null && detail.value !== '' && (
                              <p><strong className="text-neutral-300">Valor:</strong> {compactValue(detail.value, 'value')}</p>
                            )}
                            {detail.person && <p><strong className="text-neutral-300">Pessoa:</strong> {detail.person}</p>}
                            {detail.party && <p><strong className="text-neutral-300">Partido:</strong> {detail.party}</p>}
                            {detail.supplier && <p><strong className="text-neutral-300">Fornecedor:</strong> {detail.supplier}</p>}
                            {detail.supplier_document && <p><strong className="text-neutral-300">CPF/CNPJ informado:</strong> {detail.supplier_document}</p>}
                            {detail.type === 'viagem' && (
                              <>
                                <p><strong className="text-neutral-300">Local da viagem:</strong> {detail.travel_location || 'Não informado pela fonte'}</p>
                                <p><strong className="text-neutral-300">Motivo da viagem:</strong> {detail.travel_reason || 'Não informado pela fonte'}</p>
                                <p><strong className="text-neutral-300">Dias em viagem:</strong> {detail.travel_days || 'Não informado pela fonte'}</p>
                              </>
                            )}
                          </div>
                          {detail.document_url && (
                            <a
                              href={detail.document_url}
                              target="_blank"
                              rel="noreferrer"
                              onClick={(event) => event.stopPropagation()}
                              className="mt-3 inline-flex items-center gap-2 rounded border border-neutral-700 px-3 py-1.5 text-xs font-bold text-neutral-200 hover:border-red-700"
                            >
                              Documento oficial <ExternalLink className="h-3 w-3 text-red-400" />
                            </a>
                          )}
                        </div>
                        );
                      })}
                      {filteredSelectedPoliticalDetails.length === 0 && (
                        <div className="rounded border border-neutral-800 bg-neutral-900 p-3 text-sm text-neutral-400">
                          Nenhuma analise cacheada encontrada para estes filtros.
                        </div>
                      )}
                    </div>
                    <div className="mt-3 flex flex-wrap items-center justify-between gap-3 rounded border border-neutral-800 bg-neutral-900 p-3 text-xs text-neutral-400">
                      <span>
                        Pagina {selectedPoliticalDetailPage} de {selectedPoliticalDetailPages} - {filteredSelectedPoliticalDetails.length.toLocaleString('pt-BR')} registro(s)
                      </span>
                      <span className="flex gap-2">
                        <button
                          type="button"
                          onClick={() => setPoliticalDetailPage((current) => {
                            const nextPage = Math.max(1, current - 1);
                            setSelectedPoliticalDetailIndex((nextPage - 1) * POLITICAL_DETAIL_PAGE_SIZE);
                            return nextPage;
                          })}
                          disabled={selectedPoliticalDetailPage <= 1}
                          className="rounded border border-neutral-700 px-3 py-1 font-bold text-neutral-200 disabled:cursor-not-allowed disabled:opacity-40"
                        >
                          Anterior
                        </button>
                        <button
                          type="button"
                          onClick={() => setPoliticalDetailPage((current) => {
                            const nextPage = Math.min(selectedPoliticalDetailPages, current + 1);
                            setSelectedPoliticalDetailIndex((nextPage - 1) * POLITICAL_DETAIL_PAGE_SIZE);
                            return nextPage;
                          })}
                          disabled={selectedPoliticalDetailPage >= selectedPoliticalDetailPages}
                          className="rounded border border-neutral-700 px-3 py-1 font-bold text-neutral-200 disabled:cursor-not-allowed disabled:opacity-40"
                        >
                          Proxima
                        </button>
                      </span>
                    </div>
                  </section>
                )}

                <section>
                  <h3 className="text-xs font-black uppercase text-neutral-500">Riscos e atenções</h3>
                  <div className="mt-3 space-y-2">
                    {(selectedPoliticalItem.risks || []).map((risk, index) => (
                      <div key={`${risk.title}-${index}`} className="rounded border border-red-900/50 bg-red-950/20 p-3 text-sm text-neutral-200">
                        <div className="flex items-start justify-between gap-3">
                          <strong className="text-white">{risk.title}</strong>
                          <span className={`rounded-full border px-2 py-0.5 text-[11px] font-black ${riskCopy[risk.level]?.color || riskCopy.baixo.color}`}>
                            {riskCopy[risk.level]?.label || risk.level}
                          </span>
                        </div>
                        <p className="mt-2 leading-6 text-neutral-300">{compactText(risk.message, 160)}</p>
                      </div>
                    ))}
                  </div>
                </section>

                {selectedPoliticalItem.people?.length > 0 && (
                  <section>
                    <h3 className="text-xs font-black uppercase text-neutral-500">Pessoas e fornecedores no recorte</h3>
                    <div className="mt-3 flex flex-wrap gap-2">
                      {selectedPoliticalItem.people.map((person) => (
                        <span key={person} className="rounded border border-neutral-800 bg-neutral-900 px-2 py-1 text-xs text-neutral-300">
                          {person}
                        </span>
                      ))}
                    </div>
                  </section>
                )}

                <section>
                  <h3 className="text-xs font-black uppercase text-neutral-500">Fontes reais</h3>
                  <div className="mt-3 grid gap-2">
                    {(selectedPoliticalItem.sources || []).map((source) => (
                      <a
                        key={`${source.label}-${source.url}`}
                        href={source.url}
                        target="_blank"
                        rel="noreferrer"
                        className="flex items-start justify-between gap-3 rounded border border-neutral-800 bg-neutral-900 p-3 text-sm text-neutral-200 hover:border-red-700"
                      >
                        <span>
                          <strong className="block">{source.label}</strong>
                          <small className="text-neutral-500">{source.kind}</small>
                        </span>
                        <ExternalLink className="mt-1 h-4 w-4 shrink-0 text-red-400" />
                      </a>
                    ))}
                  </div>
                </section>

                <div className="rounded border border-neutral-800 bg-neutral-900 p-3 text-xs leading-5 text-neutral-400">
                  Nota: estes pontos indicam prioridade de conferência em dados oficiais. Links de processos, contas e controle externo são consultas públicas para leitura humana. A plataforma não afirma crime, culpa, suborno, corrupção nem desfecho jurídico.
                </div>
                </section>

                <aside className="max-h-[calc(100dvh-8rem)] space-y-4 overflow-y-auto rounded-lg border border-neutral-800 bg-neutral-900 p-4 lg:sticky lg:top-4 lg:self-start">
                  {selectedPoliticalDetail && (
                    <section className="rounded-lg border border-red-900/60 bg-red-950/20">
                      <div className="border-b border-red-900/50 px-4 py-3">
                        <h3 className="flex items-center gap-2 text-sm font-black text-red-100">
                          <AlertTriangle className="h-4 w-4 text-red-400" />
                          Parecer Analítico do COIBE
                        </h3>
                        <p className="mt-1 text-xs text-red-100/70">{politicalTypeLabel(selectedPoliticalDetail.type)}</p>
                      </div>
                      <div className="max-h-[42vh] space-y-3 overflow-y-auto p-4 text-sm text-neutral-200">
                        <div>
                          <strong className="block text-white">{selectedPoliticalDetail.title || politicalTypeLabel(selectedPoliticalDetail.type)}</strong>
                          {selectedPoliticalDetail.description && (
                            <p className="mt-2 leading-6 text-neutral-300">{compactText(selectedPoliticalDetail.description, 180)}</p>
                          )}
                        </div>
                        {selectedPoliticalDetailRisk && (
                          <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                            <div className="flex items-center justify-between gap-2">
                              <span className="text-xs font-black uppercase text-neutral-500">Superfaturamento no registro</span>
                              <span className={`rounded-full border px-2 py-0.5 text-[11px] font-black ${selectedPoliticalDetailRisk.color}`}>
                                {selectedPoliticalDetailRisk.label}
                              </span>
                            </div>
                            <p className="mt-2 text-xs leading-5 text-neutral-400">{compactText(selectedPoliticalDetailRisk.message, 120)}</p>
                          </div>
                        )}
                        {selectedPoliticalDetailReview && (
                          <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                            <p className="text-xs font-black uppercase text-neutral-500">Como verificar</p>
                            <p className="mt-2 leading-6 text-neutral-300">{compactText(selectedPoliticalDetailReview.title, 170)}</p>
                            {selectedPoliticalDetailReview.hiddenSignals.length > 0 && (
                              <div className="mt-3 space-y-2">
                                {selectedPoliticalDetailReview.hiddenSignals.map((signal) => (
                                  <p key={signal} className="rounded border border-neutral-800 bg-neutral-900 px-2 py-1 text-xs text-neutral-300">
                                    {signal}
                                  </p>
                                ))}
                              </div>
                            )}
                          </div>
                        )}
                        <div className="grid gap-2 text-xs text-neutral-400">
                          {selectedPoliticalDetail.value !== undefined && selectedPoliticalDetail.value !== null && selectedPoliticalDetail.value !== '' && (
                            <p><strong className="text-neutral-300">Valor:</strong> {compactValue(selectedPoliticalDetail.value, 'value')}</p>
                          )}
                          <p><strong className="text-neutral-300">Data:</strong> {selectedPoliticalDetail.date ? formatDate(selectedPoliticalDetail.date) : selectedPoliticalDetail.month && selectedPoliticalDetail.year ? `${String(selectedPoliticalDetail.month).padStart(2, '0')}/${selectedPoliticalDetail.year}` : 'Não informada'}</p>
                          {selectedPoliticalDetail.person && <p><strong className="text-neutral-300">Pessoa:</strong> {selectedPoliticalDetail.person}</p>}
                          {selectedPoliticalDetail.party && <p><strong className="text-neutral-300">Partido:</strong> {selectedPoliticalDetail.party}</p>}
                          {selectedPoliticalDetail.supplier && <p><strong className="text-neutral-300">Fornecedor:</strong> {selectedPoliticalDetail.supplier}</p>}
                          {selectedPoliticalDetail.supplier_document && <p><strong className="text-neutral-300">CPF/CNPJ:</strong> {selectedPoliticalDetail.supplier_document}</p>}
                          {selectedPoliticalDetail.entity && <p><strong className="text-neutral-300">Órgão:</strong> {selectedPoliticalDetail.entity}</p>}
                        </div>
                        {selectedPoliticalDetail.document_url && (
                          <a
                            href={selectedPoliticalDetail.document_url}
                            target="_blank"
                            rel="noreferrer"
                            className="inline-flex w-full items-center justify-center gap-2 rounded border border-red-800 bg-red-600 px-3 py-2 text-xs font-black text-white hover:bg-red-500"
                          >
                            Abrir documento oficial <ExternalLink className="h-3 w-3" />
                          </a>
                        )}
                      </div>
                    </section>
                  )}

                  <div>
                    <p className="text-xs font-black uppercase text-red-400">Dinheiro e numeros</p>
                    <h3 className="mt-1 font-black text-white">Resumo financeiro do recorte</h3>
                  </div>
                  <div className="grid gap-3">
                    <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                      <p className="text-xs text-neutral-500">Gasto publico analisado</p>
                      <strong className="text-lg text-white">{compactValue(selectedPoliticalItem.total_public_money, 'value')}</strong>
                    </div>
                    <div className="grid grid-cols-2 gap-3">
                      <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                        <p className="text-xs text-neutral-500">Viagens no total</p>
                        <strong className="text-white">{compactValue(selectedPoliticalItem.travel_public_money, 'value')}</strong>
                      </div>
                      <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                        <p className="text-xs text-neutral-500">Registros</p>
                        <strong className="text-white">{Number(selectedPoliticalItem.records_count || 0).toLocaleString('pt-BR')}</strong>
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-3">
                      <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                        <p className="text-xs text-neutral-500">Pagamentos</p>
                        <strong className="text-white">{selectedPoliticalMetrics?.paymentCount || 0}</strong>
                        <p className="mt-1 text-xs text-neutral-500">{compactValue(selectedPoliticalMetrics?.paymentValue || 0, 'value')}</p>
                      </div>
                      <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                        <p className="text-xs text-neutral-500">Contratos</p>
                        <strong className="text-white">{selectedPoliticalMetrics?.contractCount || 0}</strong>
                        <p className="mt-1 text-xs text-neutral-500">{compactValue(selectedPoliticalMetrics?.contractValue || 0, 'value')}</p>
                      </div>
                    </div>
                    <div className="rounded border border-red-900/60 bg-red-950/20 p-3">
                      <p className="text-xs text-red-200">Movimentacao de risco</p>
                      <strong className="text-white">{selectedPoliticalMetrics?.riskMovementCount || 0} sinal(is)</strong>
                      <p className="mt-1 text-xs text-red-100/80">{compactValue(selectedPoliticalMetrics?.riskMovementValue || 0, 'value')}</p>
                    </div>
                    <div className="rounded border border-red-900/60 bg-red-950/20 p-3">
                      <p className="text-xs text-red-200">Superfaturamento relacionado</p>
                      <strong className="text-white">{selectedPoliticalMetrics?.superpricingCount || 0} registro(s)</strong>
                      <p className="mt-1 text-xs text-red-100/80">
                        {selectedPoliticalMetrics?.superpricingHighCount || 0} alto(s), {selectedPoliticalMetrics?.superpricingMediumCount || 0} medio(s)
                      </p>
                      <p className="mt-1 text-xs text-red-100/80">{compactValue(selectedPoliticalMetrics?.superpricingValue || 0, 'value')}</p>
                    </div>
                    <div className="rounded border border-neutral-800 bg-neutral-950 p-3">
                      <p className="text-xs text-neutral-500">Atencao</p>
                      <strong className="text-white">{riskCopy[selectedPoliticalItem.attention_level]?.label || selectedPoliticalItem.attention_level}</strong>
                      <p className="mt-1 text-xs text-neutral-500">{selectedPoliticalMetrics?.highRiskCount || 0} fator(es) medio/alto</p>
                    </div>
                  </div>

                  {selectedPoliticalMetrics?.topDetail && (
                    <div className="rounded border border-neutral-800 bg-neutral-950 p-3 text-sm text-neutral-300">
                      <p className="text-xs font-black uppercase text-neutral-500">Maior item financeiro</p>
                      <strong className="mt-1 block text-white">{selectedPoliticalMetrics.topDetail.title || politicalTypeLabel(selectedPoliticalMetrics.topDetail.type)}</strong>
                      <p className="mt-1 text-neutral-400">{compactValue(selectedPoliticalMetrics.topDetail.value, 'value')}</p>
                    </div>
                  )}

                  {(selectedPoliticalItem.risks || []).length > 0 && (
                    <section>
                      <h3 className="text-xs font-black uppercase text-neutral-500">Riscos principais</h3>
                      <div className="mt-3 space-y-2">
                        {(selectedPoliticalItem.risks || []).slice(0, 5).map((risk, index) => (
                          <div key={`${risk.title}-side-${index}`} className="rounded border border-red-900/50 bg-red-950/20 p-3 text-xs text-neutral-200">
                            <strong className="block text-white">{risk.title}</strong>
                            <p className="mt-2 leading-5 text-neutral-300">{compactText(risk.message, 140)}</p>
                          </div>
                        ))}
                      </div>
                    </section>
                  )}
                </aside>
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}
