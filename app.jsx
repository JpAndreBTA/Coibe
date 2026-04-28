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
  Loader2,
  MapPin,
  Search,
  ShieldCheck,
  Target,
  User
} from 'lucide-react';

const configuredApiBases = (import.meta.env.VITE_API_BASE_URL || '')
  .split(',')
  .map((base) => base.trim().replace(/\/$/, ''))
  .filter(Boolean);

const API_BASES = configuredApiBases.length
  ? configuredApiBases
  : ['', 'http://127.0.0.1:8000', 'http://127.0.0.1:8001'];
const COMPRAS_CONTRATOS_URL = 'https://dadosabertos.compras.gov.br/modulo-contratos/1_consultarContratos';

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

const SEARCH_TYPE_ORDER = {
  estado: 10,
  municipio: 20,
  politico_relacionado: 30,
  politico_deputado: 31,
  politico_senador: 32,
  cnpj: 40,
  partido_politico: 50,
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

async function apiGet(path) {
  let lastError;
  for (const base of API_BASES) {
    try {
      const response = await fetch(`${base}${path}`);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return response.json();
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError || new Error('Backend indisponível');
}

async function apiPost(path) {
  let lastError;
  for (const base of API_BASES) {
    try {
      const response = await fetch(`${base}${path}`, { method: 'POST' });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return response.json();
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError || new Error('Backend indisponível');
}

function normalizeSearchText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
    .toLowerCase();
}

function ufFromSearchText(value) {
  return STATE_NAME_TO_UF[normalizeSearchText(value)] || '';
}

function formatDate(value) {
  if (!value) return 'DATA NÃO INFORMADA';
  return new Date(`${value}T00:00:00`).toLocaleDateString('pt-BR', {
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
  const isGeneric = url.includes('swagger-ui') || !url.includes('/modulo-contratos/1_consultarContratos');
  if (!isGeneric && index !== 0) return source;
  return {
    ...source,
    label: `Compras.gov.br Dados Abertos - contrato ${alert?.id || ''}`.trim(),
    url: comprasUrlForAlert(alert),
    kind: 'API oficial federal com filtros do item'
  };
}

function officialSourcesForAlert(alert) {
  return (alert?.report?.official_sources || []).map((source, index) => normalizeOfficialSource(alert, source, index));
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
  'window_total_value'
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

function flagDetails(flag) {
  const evidence = Object.entries(flag.evidence || {});
  return evidence
    .filter(([key, value]) => !INTERNAL_DETAIL_KEYS.has(key) && value !== undefined && value !== null && value !== '')
    .slice(0, 6);
}

export default function CoibeApp() {
  const [searchTerm, setSearchTerm] = useState('');
  const [feedRiskFilter, setFeedRiskFilter] = useState('todos');
  const [feedSizeOrder, setFeedSizeOrder] = useState('data');
  const [feedDateFrom, setFeedDateFrom] = useState('');
  const [feedDateTo, setFeedDateTo] = useState('');
  const [activeTab, setActiveTab] = useState('feed');
  const [feedQuery, setFeedQuery] = useState('');
  const [activeSearchFilter, setActiveSearchFilter] = useState(null);
  const [items, setItems] = useState([]);
  const [mapPoints, setMapPoints] = useState([]);
  const [stateRisks, setStateRisks] = useState({});
  const [geoJson, setGeoJson] = useState(null);
  const [selectedState, setSelectedState] = useState(null);
  const [selectedUf, setSelectedUf] = useState('');
  const [selectedAlert, setSelectedAlert] = useState(null);
  const [page, setPage] = useState(1);
  const [hasMore, setHasMore] = useState(false);
  const [loadingFeed, setLoadingFeed] = useState(false);
  const [loadingMap, setLoadingMap] = useState(false);
  const [loadingSearch, setLoadingSearch] = useState(false);
  const [priorityScanning, setPriorityScanning] = useState(false);
  const [searchResults, setSearchResults] = useState([]);
  const [monitorStatus, setMonitorStatus] = useState(null);
  const [error, setError] = useState('');
  const loadMoreRef = useRef(null);
  const suppressSearchEffectRef = useRef(false);
  const searchRequestIdRef = useRef(0);

  const analyzedCount = Math.max(
    Number(monitorStatus?.items_analyzed || 0),
    Number(monitorStatus?.database_items_count || 0)
  );
  const libraryCount = Number(monitorStatus?.library_records_count || 0);

  const stats = useMemo(() => {
    const feedTotal = items.reduce((sum, item) => sum + Number(item.value || 0), 0);
    const feedVariation = items.reduce((sum, item) => sum + Number(item.estimated_variation || 0), 0);
    const feedHigh = items.filter((item) => item.risk_level === 'alto').length;
    const feedEntities = new Set(items.map((item) => item.entity).filter(Boolean)).size;
    const total = Number(monitorStatus?.total_value ?? feedTotal);
    const variation = Number(monitorStatus?.estimated_variation_total ?? feedVariation);
    const high = Number(monitorStatus?.high_alerts_count ?? feedHigh);
    const entities = Number(monitorStatus?.monitored_entities_count ?? feedEntities);
    const alerts = Number(monitorStatus?.alerts_count ?? items.length);

    return [
      {
        label: 'Volume Analisado',
        value: total.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 }),
        note: 'Compras.gov.br Dados Abertos',
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

  async function loadFeed(
    nextPage = 1,
    append = false,
    query = feedQuery,
    uf = selectedUf,
    risk = feedRiskFilter,
    sizeOrder = feedSizeOrder,
    dateFrom = feedDateFrom,
    dateTo = feedDateTo
  ) {
    setLoadingFeed(true);
    setError('');
    try {
      const params = new URLSearchParams({ page: String(nextPage), page_size: '10' });
      if (query.trim()) params.set('q', query.trim());
      if (uf) params.set('uf', uf);
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

  async function loadMap() {
    setLoadingMap(true);
    try {
      const [riskData, geoData] = await Promise.all([
        apiGet('/api/monitoring/state-map?page_size=80'),
        apiGet('/api/public-data/ibge/states-geojson')
      ]);
      const risksByUf = {};
      for (const state of riskData.states || []) {
        risksByUf[state.uf] = state;
      }
      setStateRisks(risksByUf);
      setGeoJson(geoData);
      setMapPoints(riskData.states || []);
    } catch {
      setError('Não foi possível carregar o mapa real agora.');
    } finally {
      setLoadingMap(false);
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
    setPriorityScanning(true);
    setError('');
    const immediateRisk = feedOptions.risk ?? feedRiskFilter;
    const immediateSizeOrder = feedOptions.sizeOrder ?? feedSizeOrder;
    const immediateDateFrom = feedOptions.dateFrom ?? feedDateFrom;
    const immediateDateTo = feedOptions.dateTo ?? feedDateTo;
    loadFeed(1, false, query, uf || '', immediateRisk, immediateSizeOrder, immediateDateFrom, immediateDateTo);
    try {
      const params = new URLSearchParams({ pages: '6', page_size: '50', limit: '120' });
      if (uf) params.set('uf', uf);
      if (query && !uf) params.set('q', query.trim());
      await apiPost(`/api/monitoring/priority-scan?${params}`);
      await Promise.all([
        loadFeed(1, false, query, uf || '', immediateRisk, immediateSizeOrder, immediateDateFrom, immediateDateTo),
        loadMap(),
        loadMonitorStatus()
      ]);
    } catch {
      setError('Não foi possível concluir a varredura prioritária agora.');
    } finally {
      setPriorityScanning(false);
    }
  }

  function selectStateOnMap(state) {
    const uf = state.uf || '';
    setSelectedState(state);
    setSelectedUf(uf);
  }

  function openStateInFeed(state) {
    const uf = state.uf || '';
    const label = state.state_name || state.name || uf;
    setSelectedState(state);
    setSelectedUf(uf);
    setFeedQuery('');
    setFeedRiskFilter('todos');
    setFeedSizeOrder('data');
    setFeedDateFrom('');
    setFeedDateTo('');
    setActiveSearchFilter({ type: 'estado', label, detail: `UF ${uf}` });
    setActiveTab('feed');
    suppressSearchEffectRef.current = true;
    setSearchTerm('');
    loadFeed(1, false, '', uf, 'todos', 'data', '', '');
    scanPriority(uf, '', { risk: 'todos', sizeOrder: 'data', dateFrom: '', dateTo: '' });
  }

  function queryFromResult(result) {
    const payload = result.payload || {};
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
        label: result.title,
        detail: `UF ${uf}`,
        scan: true
      };
    }
    if (result.type === 'municipio') {
      return {
        query: result.title,
        uf,
        label: result.title,
        detail: uf ? `Municipio em ${uf}` : 'Municipio',
        scan: false
      };
    }
    if (result.type === 'politico_deputado' || result.type === 'politico_senador' || result.type === 'politico_relacionado') {
      const relatedQueries = Array.isArray(payload.related_queries) ? payload.related_queries : [];
      const relatedQuery = relatedQueries.length > 0 ? relatedQueries.join('|') : '';
      return {
        query: uf ? '' : relatedQuery || result.title,
        uf,
        label: result.title,
        detail: uf ? `Dados relacionados ao estado do politico: ${uf}` : 'Politico relacionado',
        scan: Boolean(uf)
      };
    }
    if (result.type === 'cnpj') {
      const digits = String(payload.cnpj || payload.cnpj_basico || result.subtitle || '').replace(/\D/g, '');
      return {
        query: digits || result.title,
        uf: '',
        label: result.title,
        detail: digits ? `CNPJ ${digits}` : 'CNPJ',
        scan: false
      };
    }
    if (result.type === 'risco_superfaturamento') {
      return {
        query: payload.id || payload.supplier_cnpj || result.title,
        uf: payload.uf || '',
        label: result.title,
        detail: payload.estimated_variation ? `Variação estimada R$ ${Number(payload.estimated_variation || 0).toLocaleString('pt-BR')}` : 'Risco de Superfaturamento',
        scan: false
      };
    }
    if (result.type === 'contrato') {
      const contractId = payload.idCompra || payload.id || payload.numeroContrato || payload.numeroControlePncpCompra;
      return {
        query: contractId || payload.supplier_cnpj || payload.niFornecedor || result.title,
        uf: payload.uf || '',
        label: result.title,
        detail: contractId ? `Contrato ${contractId}` : 'Contrato relacionado',
        scan: false
      };
    }
    if (result.type === 'partido_politico') {
      return {
        query: payload.sigla || payload.nome || result.title,
        uf: '',
        label: result.title,
        detail: 'Partido politico - Dados Abertos da Camara',
        scan: false
      };
    }
    return {
      query: result.title || searchTerm,
      uf,
      label: result.title || searchTerm,
      detail: result.source || 'Resultado relacionado',
      scan: false
    };
  }

  function applySearchResult(result) {
    if (result.type === 'stf_processo' || result.type === 'stf_jurisprudencia') {
      if (result.url) window.open(result.url, '_blank', 'noopener,noreferrer');
      setSearchResults([]);
      return;
    }
    const filter = filterFromResult(result);
    const nextUf = filter.uf || '';
    const nextQuery = filter.query || '';
    setActiveTab('feed');
    setSelectedUf(nextUf);
    setFeedQuery(nextQuery);
    setFeedRiskFilter('todos');
    setFeedSizeOrder('data');
    setFeedDateFrom('');
    setFeedDateTo('');
    suppressSearchEffectRef.current = true;
    setSearchTerm(filter.label || nextQuery);
    setSearchResults([]);
    setActiveSearchFilter({ type: result.type, label: filter.label, detail: filter.detail });
    setSelectedState(nextUf ? { uf: nextUf, state_name: filter.label } : null);
    if (filter.scan) {
      scanPriority(nextUf, nextQuery, { risk: 'todos', sizeOrder: 'data', dateFrom: '', dateTo: '' });
    } else {
      loadFeed(1, false, nextQuery, nextUf, 'todos', 'data', '', '');
      loadMap();
    }
  }

  useEffect(() => {
    loadFeed(1, false, '');
    loadMap();
    loadMonitorStatus();
  }, []);

  useEffect(() => {
    if (activeTab === 'feed') {
      loadFeed(1, false, feedQuery, selectedUf, feedRiskFilter, feedSizeOrder, feedDateFrom, feedDateTo);
    }
  }, [feedRiskFilter, feedSizeOrder, feedDateFrom, feedDateTo]);

  useEffect(() => {
    const interval = window.setInterval(() => {
      loadMonitorStatus();
      if (activeTab === 'map') loadMap();
    }, 30000);
    return () => window.clearInterval(interval);
  }, [activeTab]);

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
  }, [activeTab, hasMore, loadingFeed, page, feedQuery, selectedUf, feedRiskFilter, feedSizeOrder, feedDateFrom, feedDateTo]);

  function handleSearch(event) {
    event.preventDefault();
    const stateUf = ufFromSearchText(searchTerm);
    if (stateUf) {
      setSelectedUf(stateUf);
      setFeedQuery('');
      setActiveSearchFilter({ type: 'estado', label: searchTerm.trim().toUpperCase(), detail: `UF ${stateUf}` });
      setSelectedState({ uf: stateUf, state_name: searchTerm.trim().toUpperCase() });
      setActiveTab('feed');
      scanPriority(stateUf, '');
      return;
    }
    setSelectedUf('');
    setFeedQuery(searchTerm);
    setActiveSearchFilter({ type: 'busca', label: searchTerm, detail: 'Termo livre' });
    setSearchResults([]);
    loadFeed(1, false, searchTerm, '');
  }

  function handleSearchFocus() {
    const query = searchTerm.trim();
    if (query.length >= 2) {
      loadUniversalSearchProgressive(query);
    }
  }

  const selectedRisk = riskCopy[selectedAlert?.risk_level] || riskCopy.indeterminado;
  const selectedOfficialSources = officialSourcesForAlert(selectedAlert);

  return (
    <div className="min-h-screen bg-neutral-950 text-neutral-100">
      <header className="sticky top-0 z-20 border-b border-neutral-800 bg-black">
        <div className="mx-auto flex min-h-16 max-w-7xl flex-col gap-3 px-4 py-3 sm:px-6 md:flex-row md:items-center md:justify-between lg:px-8">
          <div className="flex items-center gap-3">
            <Target className="h-8 w-8 text-red-600" />
            <h1 className="text-2xl font-black tracking-widest">
              COIBE<span className="ml-1 text-sm tracking-normal text-red-600">.IA</span>
            </h1>
          </div>

          <form onSubmit={handleSearch} className="relative w-full md:max-w-xl">
            <Search className="pointer-events-none absolute left-3 top-1/2 h-5 w-5 -translate-y-1/2 text-neutral-500" />
            <input
              value={searchTerm}
              onChange={(event) => setSearchTerm(event.target.value)}
              onFocus={handleSearchFocus}
              placeholder="Buscar estado, cidade, político, partido, STF, CNPJ ou contrato..."
              className="h-11 w-full rounded-lg border border-neutral-800 bg-neutral-900 pl-10 pr-4 text-sm text-white outline-none placeholder:text-neutral-500 focus:border-red-600 focus:ring-2 focus:ring-red-600/30"
            />
          </form>
        </div>
      </header>

      <main className="mx-auto max-w-7xl px-4 py-7 sm:px-6 lg:px-8">
        {error && (
          <div className="mb-5 rounded-lg border border-red-900 bg-red-950/30 p-4 text-sm text-red-100">
            {error}
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
            <div className="grid grid-cols-2 gap-3 text-sm sm:min-w-96 lg:grid-cols-5">
              <div className="rounded-lg border border-neutral-800 bg-neutral-950 p-3">
                <span className="text-neutral-500">Itens analisados</span>
                <strong className="block text-2xl text-white">{analyzedCount}</strong>
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
              Última análise: {new Date(monitorStatus.generated_at).toLocaleString('pt-BR')}
            </p>
          )}
          {priorityScanning && (
            <p className="mt-3 text-xs font-bold uppercase text-red-300">
              Varredura prioritária em andamento
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
              {searchResults.map((result, index) => (
                <button
                  key={`${result.type}-${result.title}-${index}`}
                  type="button"
                  onClick={() => applySearchResult(result)}
                  className="rounded-lg border border-neutral-800 bg-neutral-950/70 p-4 text-left transition hover:border-red-700"
                >
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <span className="rounded border border-red-900 bg-red-950/40 px-2 py-1 text-[11px] font-black uppercase text-red-300">
                        {result.type.replaceAll('_', ' ')}
                      </span>
                      <strong className="mt-3 block text-white">{result.title}</strong>
                      {result.subtitle && <p className="mt-1 text-sm text-neutral-400">{result.subtitle}</p>}
                    </div>
                    <ChevronRight className="h-4 w-4 shrink-0 text-red-400" />
                  </div>
                  <p className="mt-3 text-xs text-neutral-500">{result.source}</p>
                </button>
              ))}
              {!loadingSearch && searchResults.length === 0 && (
                <div className="rounded-lg border border-neutral-800 bg-neutral-950/70 p-4 text-sm text-neutral-400">
                  Nenhum resultado direto encontrado nas fontes unificadas.
                </div>
              )}
            </div>
          </section>
        )}

        <section className="mt-8 grid gap-7 lg:grid-cols-[minmax(0,1fr)_360px]">
          <div>
            <div className="flex border-b border-neutral-800">
              <button
                onClick={() => setActiveTab('feed')}
                className={`px-5 py-3 text-sm font-bold transition ${activeTab === 'feed' ? 'border-b-2 border-red-600 text-red-500' : 'text-neutral-400 hover:text-white'}`}
              >
                Feed de Monitoramento
              </button>
              <button
                onClick={() => setActiveTab('map')}
                className={`px-5 py-3 text-sm font-bold transition ${activeTab === 'map' ? 'border-b-2 border-red-600 text-red-500' : 'text-neutral-400 hover:text-white'}`}
              >
                Mapa de Alertas
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
                    setFeedDateFrom('');
                    setFeedDateTo('');
                    setActiveSearchFilter(null);
                    setSelectedState(null);
                    setSearchTerm('');
                    loadFeed(1, false, '', '');
                  }}
                  className="font-bold text-white hover:text-red-200"
                >
                  Limpar filtro
                </button>
              </div>
            )}

            {activeTab === 'feed' && (
              <div className="mt-4 grid gap-3 rounded-lg border border-neutral-800 bg-neutral-900 p-3 sm:grid-cols-2 lg:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_minmax(0,1.25fr)_auto]">
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
                    <span className="block text-[11px] font-black uppercase text-neutral-500">Tamanho</span>
                    <select
                      value={feedSizeOrder}
                      onChange={(event) => setFeedSizeOrder(event.target.value)}
                      className="mt-1 w-full bg-transparent text-sm font-bold text-white outline-none"
                    >
                      <option className="bg-neutral-950" value="data">Mais recentes</option>
                      <option className="bg-neutral-950" value="desc">Maior valor primeiro</option>
                      <option className="bg-neutral-950" value="asc">Menor valor primeiro</option>
                    </select>
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
                        onChange={(event) => setFeedDateFrom(event.target.value)}
                        className="min-w-0 bg-transparent text-sm font-bold text-white outline-none [color-scheme:dark]"
                        aria-label="Data inicial do conteúdo"
                      />
                      <input
                        type="date"
                        value={feedDateTo}
                        onChange={(event) => setFeedDateTo(event.target.value)}
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
                    setFeedDateFrom('');
                    setFeedDateTo('');
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
                  return (
                    <button
                      key={`${alert.id}-${alert.date}`}
                      onClick={() => setSelectedAlert(alert)}
                      className={`w-full rounded-lg border bg-neutral-900 p-5 text-left transition hover:border-red-700 ${selectedAlert?.id === alert.id ? 'border-red-900' : 'border-neutral-800'}`}
                    >
                      <div className="flex items-start justify-between gap-4">
                        <div className="flex items-center gap-3 text-xs font-semibold text-neutral-400">
                          <FileText className="h-5 w-5" />
                          {formatDate(alert.date)}
                        </div>
                        <span className={`rounded-full border px-3 py-1 text-xs font-black ${risk.color}`}>
                          {risk.label}
                        </span>
                      </div>

                      <h2 className="mt-4 line-clamp-2 text-lg font-black text-white">{alert.title}</h2>
                      <div className="mt-3 flex flex-wrap gap-4 text-sm text-neutral-400">
                        <span className="flex items-center gap-1.5"><MapPin className="h-4 w-4" />{alert.location}</span>
                        <span className="flex items-center gap-1.5"><User className="h-4 w-4" />{alert.entity}</span>
                      </div>

                      <div className="mt-4 flex items-center justify-between gap-4 rounded-lg border border-neutral-800 bg-neutral-950/70 p-3">
                        <div>
                          <p className="text-xs text-neutral-400">Valor do Contrato/Despesa</p>
                          <strong className="text-white">{alert.formatted_value}</strong>
                        </div>
                        <div className="text-right">
                          <p className="text-xs font-bold text-red-400">Variação Estimada (IA)</p>
                          <strong className="text-red-500">{alert.formatted_variation}</strong>
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
            ) : (
              <div className="mt-5 overflow-hidden rounded-lg border border-neutral-800 bg-neutral-900">
                <div className="border-b border-neutral-800 px-5 py-4">
                  <h2 className="font-black text-white">Mapa de Alertas por Estado</h2>
                  <p className="mt-1 text-sm text-neutral-400">Agregado por município da UASG a partir de contratos oficiais.</p>
                </div>
                <div className="relative min-h-[520px] bg-[radial-gradient(circle_at_center,#262626_0,#111_55%,#080808_100%)] p-4">
                  {loadingMap && (
                    <div className="absolute inset-0 z-10 flex items-center justify-center bg-black/40 text-neutral-300">
                      <Loader2 className="mr-2 h-5 w-5 animate-spin" />
                      Carregando mapa...
                    </div>
                  )}
                  <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_220px]">
                    <svg
                      viewBox={`0 0 ${mapBounds.width} ${mapBounds.height}`}
                      role="img"
                      aria-label="Mapa do Brasil por estados"
                      className="h-auto w-full max-h-[620px]"
                    >
                      <rect width={mapBounds.width} height={mapBounds.height} fill="transparent" />
                      {(geoJson?.features || []).map((feature) => {
                        const props = feature.properties || {};
                        const uf = props.sigla || props.UF || props.uf || props.SIGLA_UF || IBGE_CODE_TO_UF[props.codarea];
                        const name = props.nome || props.NM_UF || props.name || stateRisks[uf]?.state_name || uf;
                        const risk = stateRisks[uf] || {};
                        const selected = selectedState?.uf === uf;
                        return (
                          <path
                            key={uf || name}
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
                    </svg>
                    <aside className="rounded-lg border border-neutral-800 bg-black/60 p-4 text-sm">
                      <p className="text-xs font-black uppercase text-red-400">Estado selecionado</p>
                      {selectedState ? (
                        <div className="mt-3 space-y-3">
                          <h3 className="text-xl font-black text-white">{selectedState.state_name || selectedState.name || selectedState.uf}</h3>
                          <p className="text-neutral-400">UF {selectedState.uf}</p>
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
                  <div className="absolute bottom-4 left-4 rounded-lg border border-neutral-800 bg-black/70 p-3 text-xs text-neutral-300">
                    <p className="font-bold text-white">Intensidade</p>
                    <div className="mt-2 flex flex-wrap items-center gap-2">
                      <span className="h-3 w-3 rounded-full bg-yellow-400" /> Baixa
                      <span className="h-3 w-3 rounded-full bg-orange-500" /> Média
                      <span className="h-3 w-3 rounded-full bg-red-500" /> Alta
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>

          <aside className="rounded-lg border border-neutral-800 bg-neutral-900 lg:sticky lg:top-24 lg:max-h-[calc(100vh-7rem)] lg:self-start lg:overflow-y-auto">
            {selectedAlert ? (
              <>
                <div className={`rounded-t-lg px-5 py-4 ${selectedRisk.panel}`}>
                  <h2 className="flex items-center gap-2 text-sm font-black text-red-200">
                    <AlertTriangle className="h-5 w-5" />
                    Parecer Analítico do COIBE
                  </h2>
                </div>
                <div className="p-5">
                  <h3 className="line-clamp-3 font-black text-white">{selectedAlert.title}</h3>
                  <p className="mt-1 text-sm text-neutral-400">Id. {selectedAlert.report.id}</p>
                  <p className="mt-3 text-sm leading-6 text-neutral-300">{selectedAlert.report.summary}</p>

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

                  <h4 className="mt-5 text-xs font-black uppercase text-neutral-500">Fatores de atenção identificados</h4>
                  <ul className="mt-3 space-y-3">
                    {selectedAlert.report.red_flags.map((flag) => (
                      <li key={`${flag.code}-${flag.title}`} className="flex gap-3 rounded-lg border border-red-900/70 bg-red-950/20 p-3 text-sm text-neutral-200">
                        <AlertCircle className="mt-0.5 h-5 w-5 shrink-0 text-red-500" />
                        <span>
                          <strong className="block text-white">{flag.title}</strong>
                          {flag.message}
                          {friendlyComparison(flag).length > 0 && (
                            <span className="mt-3 grid gap-1 text-xs font-semibold text-red-100">
                              {friendlyComparison(flag).map((comparison) => (
                                <span key={`${flag.code}-${comparison}`} className="rounded border border-red-900/60 bg-red-950/30 px-2 py-1">
                                  {comparison}
                                </span>
                              ))}
                            </span>
                          )}
                          {flagDetails(flag).length > 0 && (
                            <span className="mt-3 grid gap-1 text-xs text-neutral-400">
                              {flagDetails(flag).map(([key, value]) => (
                                <span key={`${flag.code}-${key}`} className="rounded border border-neutral-800 bg-neutral-950/70 px-2 py-1">
                                  <strong className="text-neutral-300">{readableKey(key)}:</strong> {compactValue(value, key)}
                                </span>
                              ))}
                            </span>
                          )}
                        </span>
                      </li>
                    ))}
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
                    <strong className="text-neutral-300">Nota Legal:</strong> A plataforma aponta variações estatísticas e padrões atípicos com base em dados abertos. A análise final e conclusão sobre eventuais irregularidades cabem exclusivamente aos órgãos de controle competentes.
                  </div>
                </div>
              </>
            ) : (
              <div className="p-8 text-center">
                <Target className="mx-auto h-16 w-16 text-neutral-700" />
                <h2 className="mt-4 font-black text-white">Painel de Análise COIBE</h2>
                <p className="mt-2 text-sm text-neutral-400">Selecione um item real no feed ou no mapa para visualizar fontes oficiais e relatório técnico.</p>
              </div>
            )}
          </aside>
        </section>
      </main>
    </div>
  );
}
