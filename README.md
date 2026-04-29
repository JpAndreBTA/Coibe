# Coibe IA - Plataforma aberta de monitoramento público

Coibe IA é uma plataforma aberta para acompanhar contratações públicas, cruzar bases governamentais e destacar fatores de atenção que merecem revisão humana.

O objetivo é tornar dados públicos mais acessíveis, pesquisáveis e compreensíveis para cidadãos, jornalistas, pesquisadores, desenvolvedores, organizações civis e equipes de controle. A plataforma organiza informações de fontes oficiais e apresenta indícios estatísticos, históricos e contextuais sem fazer acusações definitivas.

## O que a plataforma faz

- Consulta e cruza fontes públicas oficiais.
- Monitora contratos, fornecedores, órgãos, cidades e estados.
- Aponta variações atípicas de valor e outros fatores de atenção.
- Mantém um feed pesquisável com lazy load para grandes bases.
- Expõe uma API pública para integração e auditoria técnica.
- Preserva rastreabilidade por links e evidências das fontes consultadas.

## Para quem é

- Cidadãos interessados em transparência pública.
- Jornalistas e pesquisadores investigando bases abertas.
- Desenvolvedores que queiram auditar, reaproveitar ou melhorar os conectores.
- Organizações e equipes que precisam priorizar leitura humana de muitos contratos.

## Acesse

- API pública via túnel: `https://api.coibe.com.br`
- Repositório: `https://github.com/JpAndreBTA/Coibe`
- Autor: Jp André

## Código aberto

Coibe IA é um projeto de código aberto. Contribuições, auditorias, issues e melhorias são bem-vindas, especialmente em conectores de dados públicos, qualidade de evidências, performance, documentação e revisão das regras de risco.

## Arquitetura atual

- Frontend: React, Vite, TailwindCSS e Lucide React.
- Backend: FastAPI + Uvicorn.
- Dados: pasta local `data/` no backend do operador ou storage S3 compatível.
- Monitoramento: `local_monitor.py` coleta, normaliza, analisa e atualiza a base.
- Publicação leve: frontend estático na Hostinger e backend local exposto via Cloudflare Tunnel ou backend Docker em Render.

## Uso responsável dos dados

A Coibe IA não acusa fraude, corrupção ou irregularidade definitiva. Os resultados indicam fatores de atenção, padrões estatísticos e cruzamentos públicos que precisam ser interpretados por pessoas e, quando aplicável, por órgãos competentes.

## Backend

```powershell
Copy-Item .env.example .env
# Edite o .env e preencha PORTAL_TRANSPARENCIA_API_KEY
py -3.10 -m pip install -r requirements.txt
py -3.10 -m uvicorn main:app --reload
```

Acesse:

- API: http://127.0.0.1:8000/docs
- Healthcheck: http://127.0.0.1:8000/health

## Endpoints principais

### Variáveis de ambiente

Crie um arquivo `.env` a partir de [.env.example](</c:/Users/jpzin/Downloads/Coibe/.env.example:1>).

```env
PORTAL_TRANSPARENCIA_API_KEY=sua_chave_aqui
PORTAL_TRANSPARENCIA_EMAIL=seu_email_aqui
PORTAL_TRANSPARENCIA_BASE_URL=https://api.portaldatransparencia.gov.br/api-de-dados
```

O backend envia a chave no header oficial `chave-api-dados`.

```http
GET /api/public-data/portal-transparencia/status
GET /api/public-data/portal-transparencia/proxy?path=contratos
```

### Fontes públicas configuradas

```http
GET /api/sources
```

Lista conectores ativos e a política de atualização automática.

### Busca unificada

```http
GET /api/search?q=Tiririca
GET /api/search?q=00000000000191
GET /api/search?q=São Paulo
GET /api/search?q=contrato computador
GET /api/search/index?q=odebrech
GET /api/search/autocomplete?q=ode
```

Consulta múltiplas fontes públicas em uma resposta única:

- Câmara dos Deputados Dados Abertos
- Senado Federal Dados Abertos
- Brasil API CNPJ
- IBGE Localidades
- Compras.gov.br Dados Abertos

A busca indexada usa a base acumulada em `data/processed` com fuzzy search simples para resposta instantanea e tolerancia a erro. Em producao pesada, o proximo salto e embeddings persistidos em PostgreSQL com `pgvector`, distancia geografica no PostGIS e OpenSearch/Elasticsearch para indice invertido.

### Feed real com lazy load

```http
GET /api/monitoring/feed?page=1&page_size=10
GET /api/monitoring/feed?page=1&page_size=10&q=banco
GET /api/monitoring/feed?page=1&page_size=10&source=live
GET /api/monitoring/feed?page=1&page_size=10
```

Observacao operacional: o feed carrega 10 itens por vez apenas na interface. A base da plataforma nao fica limitada a 10 registros; o monitor acumula dados continuamente e o parametro `source=live` consulta as APIs publicas diretamente.

Retorna 10 itens por vez, em ordem cronológica, usando contratos reais do Compras.gov.br Dados Abertos. A busca aceita órgão, fornecedor, objeto ou CNPJ.
O backend tenta janelas recentes automaticamente: 45, 120 e 240 dias. Se a fonte ainda não tiver dados no período, usa a janela histórica mais recente disponível como fallback.

### Mapa real de alertas

```http
GET /api/monitoring/map?page_size=50
GET /api/monitoring/state-map?page_size=80
GET /api/monitoring/state-map?page_size=80&source=live
GET /api/public-data/ibge/states-geojson
```

Agrega contratos reais por município/UF da UASG e retorna coordenadas, quantidade de alertas, valor total e score de risco.
O frontend usa a malha oficial do IBGE para renderizar o mapa do Brasil por estados, com clique por UF e filtro do feed.

Fontes usadas nesta etapa:

- Compras.gov.br Dados Abertos: contratos públicos federais
- IBGE/UASG: município e UF da unidade gestora
- PNCP: referência oficial de contratação quando o identificador estiver disponível
- Querido Diário: busca complementar em diários oficiais municipais

### CNPJ + Red Flag 01

```http
GET /api/analyze-cnpj/00000000000191?valor_contrato=750000&data_assinatura=2026-04-27
```

Consulta a Brasil API, retorna abertura, QSA e aplica:

- idade do CNPJ menor que 180 dias
- valor do contrato maior que R$ 500.000

### Análise completa de contrato

```http
POST /api/analyze-contract
Content-Type: application/json

{
  "cnpj": "00000000000191",
  "valor_contrato": 750000,
  "data_assinatura": "2026-04-27",
  "objeto": "Computadores",
  "cidade": "São Paulo",
  "preco_unitario": 11900,
  "quantidade": 60
}
```

Cruza:

- Brasil API para dados cadastrais e QSA
- IBGE para validação do município
- motor de regras COIBE para red flags
- tabela demonstrativa de preços de referência

### Superfaturamento com IA/ML

```http
POST /api/analyze-superpricing
GET /api/analyze-superpricing/index?q=computador&uf=SP
Content-Type: application/json

{
  "contamination": 0.18
}
```

Quando `pandas` e `scikit-learn` estão disponíveis, usa `IsolationForest`.
Se não estiverem instalados no Python atual, usa fallback estatístico por z-score para não derrubar a API.

`/api/analyze-superpricing/index` compara contratos da base acumulada por termo e UF usando z-score. Essa camada representa o motor estatistico atual; em producao pesada, o agrupamento semantico deve migrar para embeddings persistidos em PostgreSQL com `pgvector`.

### Fatores de atencao

O parecer tecnico nunca acusa formalmente corrupcao ou fraude. Os achados aparecem como fatores de atencao, risco potencial ou variacao atipica, sempre com evidencia e criterio calculado:

- Superfaturamento potencial: z-score acima de 3,0 contra media e desvio padrao do grupo comparavel.
- Maturidade suspeita: CNPJ com menos de 180 dias em contrato acima de R$ 500.000, quando a data de abertura estiver disponivel em fonte publica.
- Inviabilidade logistica potencial: objeto com presenca fisica, fornecedor a mais de 800 km do orgao e contrato acima de R$ 500.000.
- Fracionamento potencial: mesmo fornecedor e orgao em janela de 60 dias, contratos abaixo do limite de referencia e soma acima do limite.
- Conflito de interesses potencial: somente quando houver base publica eleitoral/relacional carregada e cruzamento positivo com CNPJ/CPF.

Além dos fatores do guia, o monitor contínuo aplica uma camada adaptativa de machine learning sobre a base acumulada. Ela cria fatores emergentes apenas quando há evidência estatística, como concentração atípica por fornecedor ou recorrência incomum do mesmo fornecedor no mesmo órgão em janela curta. Esses fatores aparecem como `ML-NEW-*` e devem ser tratados como triagem para revisão humana.

### Risco espacial/logistico

```http
POST /api/analyze-spatial-risk
```

Recebe coordenadas da empresa e do orgao publico, calcula distancia por Haversine e sinaliza `alerta_logistico` quando a atividade exige presenca fisica, a distancia passa do limite e o contrato e de alto valor. Em producao pesada, a mesma regra deve migrar para distancia geografica no PostGIS com `ST_DistanceSphere`.

### Readiness do pipeline

```http
GET /api/pipeline/readiness
```

Mostra o que ja esta implementado na plataforma e quais componentes devem ser trocados por infraestrutura de producao: PostgreSQL particionado, embeddings persistidos em PostgreSQL com pgvector, distancia geografica no PostGIS e OpenSearch/Elasticsearch.

### Scraping de página pública

```http
POST /api/scrape/public-page
Content-Type: application/json

{
  "url": "https://example.org",
  "keywords": ["licitação", "contrato", "computadores"]
}
```

Baixa uma página pública, extrai texto, retorna um trecho e conta ocorrências das palavras-chave.

## Frontend

```powershell
npm install
npm run dev
```

A landing page roda em Vite e usa React, TailwindCSS e Lucide React.

Para build público em bucket S3 ou S3 compatível, configure a URL pública da API antes do build:

```powershell
$env:VITE_API_BASE_URL="https://api.seu-dominio.com"
npm run build
aws s3 sync dist/ s3://NOME_DO_BUCKET --delete
```

Também há um script exemplo:

```powershell
.\deploy-s3.ps1 -Bucket "NOME_DO_BUCKET" -ApiBaseUrl "https://api.seu-dominio.com"
```

Se o bucket for S3 compatível e exigir endpoint próprio:

```powershell
.\deploy-s3.ps1 -Bucket "NOME_DO_BUCKET" -EndpointUrl "https://endpoint-s3-compat.com" -ApiBaseUrl "https://api.seu-dominio.com"
```

## Deploy do backend

Crie o `.env` a partir de `.env.example` e configure pelo menos:

```env
PORTAL_TRANSPARENCIA_API_KEY=sua_chave
COIBE_CORS_ORIGINS=https://seu-frontend-publico.com
COIBE_AUTO_MONITOR=false
```

O backend pode ser publicado com o `Dockerfile`:

```powershell
docker build -t coibe-api .
docker run --env-file .env -p 8000:8000 coibe-api
```

Em plataformas como Render, Railway, Fly.io ou VPS, use o comando:

```bash
uvicorn main:app --host 0.0.0.0 --port $PORT
```

### Dados pesados em bucket S3/Mega S4

Para manter o deploy leve, suba apenas os arquivos essenciais da pasta `data/` para o bucket:

```text
data/processed/latest_analysis.json
data/processed/monitoring_items.json
data/processed/public_api_records.json
data/library/library_records.jsonl
data/library/library_index.json
data/library/public_codes.json
```

No Render, configure:

```env
COIBE_DATA_S3_SYNC=true
COIBE_DATA_S3_WRITE_THROUGH=true
COIBE_DATA_LOCAL_CACHE=false
COIBE_DATA_S3_BUCKET=nome-do-bucket
COIBE_DATA_S3_PREFIX=data
COIBE_DATA_S3_ENDPOINT_URL=https://endpoint-s3-ou-mega-s4
COIBE_DATA_S3_REGION=us-east-1
AWS_ACCESS_KEY_ID=sua_access_key
AWS_SECRET_ACCESS_KEY=sua_secret_key
```

Se usar AWS S3 oficial, normalmente `COIBE_DATA_S3_ENDPOINT_URL` pode ficar vazio. Em S3 compatível, como Mega S4, preencha com o endpoint informado pela plataforma.

Com `COIBE_DATA_LOCAL_CACHE=false`, o Render/hosting não mantém cache persistente em disco: as leituras e gravações da pasta `data/` vão direto ao bucket. Isso inclui `data/cache/search`, usado pelas consultas de APIs públicas.

Para enviar toda a pasta `data/` local para o Mega S4 no prefixo correto:

```powershell
.\upload-data-mega-s4.ps1 -Bucket "coibe" -EndpointUrl "https://s3.g.s4.mega.io" -Prefix "data"
```

Depois do deploy, valide se a API está vendo os arquivos:

```text
https://sua-api.onrender.com/api/storage/status
```

## Monitoramento contínuo

O monitor segue a ordem correta do projeto: primeiro coleta dados públicos e salva snapshots da plataforma, depois aplica regras e ML nos dados coletados.
O arquivo responsável por deixar a máquina verificando e analisando os dados nacionais é:

[local_monitor.py](</c:/Users/jpzin/Downloads/Coibe/local_monitor.py:1>)

Rodar uma vez:

```powershell
py -3.10 local_monitor.py --once --pages 10 --page-size 50
```

Rodar em varredura constante, sem pausa entre ciclos:

```powershell
py -3.10 local_monitor.py --interval-minutes 0 --pages 10 --page-size 50
```

Para reduzir carga nas APIs publicas, informe outro intervalo, por exemplo `--interval-minutes 60`.

Rodar como painel visual direto no prompt, sem abrir a plataforma web:

```powershell
.\abrir_coibe_prompt.cmd
```

Esse script inicia somente o backend necessario em segundo plano, executa coleta/scrapes/conectores, aplica as metricas e o motor de ML, salva os dados em `data/` e redesenha no terminal os indicadores, riscos por UF e alertas priorizados. Para parar, use `Ctrl+C`.

Subir backend, frontend e monitoramento de uma vez:

```powershell
.\run_coibe_local.ps1
```

Alternativa em `.bat`, abrindo backend, frontend e coleta continua em janelas separadas:

```powershell
.\iniciar_coibe_completo.bat
```

Saídas da plataforma:

- `data/raw`: snapshots brutos das APIs e conectores
- `data/processed/monitoring_items.json`: base acumulada e deduplicada dos itens de monitoramento
- `data/processed/public_api_records.json`: registros acumulados de conectores, APIs publicas, estados e buscas unificadas
- `data/library/library_records.jsonl`: biblioteca incremental da plataforma, alimentada por APIs publicas, conectores e itens analisados
- `data/library/library_index.json`: indice de chaves ja vistas para inserir novos registros sem reconstruir/deduplicar a biblioteca inteira
- `data/library/public_codes.json`: codigos publicos extraidos, como CNPJs, UFs, fontes, URLs, consultas e tipos de registro
- `data/processed/latest_analysis.json`: análise consolidada mais recente
- `data/alerts`: alertas individuais gerados
- `logs`: logs do backend, frontend e monitor quando iniciado pelo PowerShell

### Ordem obrigatoria do pipeline

1. `connectors_and_scrapers`: consulta conectores publicos e APIs configuradas, incluindo Compras.gov.br, IBGE, Camara, Senado, Brasil API, Querido Diario e status do Portal da Transparencia.
2. `raw_database_merge`: grava snapshots brutos e acumula registros deduplicados em `monitoring_items.json` e `public_api_records.json`.
3. `risk_rules_and_ml`: aplica regras COIBE e deteccao estatistica/ML apenas sobre os dados ja coletados.

A plataforma pode carregar 10 itens por vez no feed, mas o monitor continuo busca varias paginas por ciclo e preserva o historico. A base cresce conforme os conectores encontram novos registros.
A biblioteca cresce por insercao incremental: cada registro recebe uma chave estavel, entra uma vez no JSONL e nos ciclos seguintes o indice evita reprocessar o que ja foi salvo.
