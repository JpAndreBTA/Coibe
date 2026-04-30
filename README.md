# Coibe IA - Plataforma aberta de monitoramento público

Coibe IA é uma plataforma aberta para acompanhar contratações públicas, cruzar bases governamentais e destacar fatores de atenção que merecem revisão humana.

O objetivo é tornar dados públicos mais acessíveis, pesquisáveis e compreensíveis para cidadãos, jornalistas, pesquisadores, desenvolvedores, organizações civis e equipes de controle. A plataforma organiza informações de fontes oficiais e apresenta indícios estatísticos, históricos e contextuais sem fazer acusações definitivas.

O COIBE organiza dados públicos, compara valores, cruza nomes e mostra sinais que merecem conferência. Ele consulta múltiplas fontes públicas em uma resposta única, para reduzir tempo de pesquisa e facilitar a checagem.

O monitor também evolui estratégias de verificação: aprende termos, alvos e métodos inspirados em red flags de contratação pública, OCDS/Open Contracting, PNCP, TCU, CGU, Portal da Transparência, CEIS/CNEP, STF e TSE. A leitura busca sinais como fornecedor único, aditivos, sobrecusto, fracionamento, sanções, relações em grafo e movimentação de alto valor.

## O que a plataforma faz

- Consulta e cruza fontes públicas oficiais.
- Monitora contratos, fornecedores, órgãos, cidades e estados.
- Aponta variações atípicas de valor e outros fatores de atenção.
- Cruza empresas, CNPJs, sócios, localização, histórico de contratação e valores.
- Exibe mapas por UF e município para leitura geográfica dos alertas.
- Usa coordenadas aproximadas e distância logística para indicar possíveis incompatibilidades operacionais.
- Mantém um feed pesquisável com lazy load para grandes bases.
- Expõe uma API pública para integração e auditoria técnica.
- Preserva rastreabilidade por links e evidências das fontes consultadas.

- Permite zoom por scroll no mapa, exibindo UF e municipios cacheados por aproximacao.
- Usa cache geoespacial local e PostGIS opcional para reduzir chamadas repetidas ao backend em uso multiusuario.

## Cruzamentos de dados

A Coibe IA organiza cada contrato como um registro investigável, combinando dados públicos de contratação, empresa, território e contexto institucional.

Principais cruzamentos:

- **Contratos e fornecedores:** objeto contratado, valor, data, órgão, unidade gestora, fornecedor, CNPJ e fonte oficial.
- **Empresas e CNPJ:** dados cadastrais, abertura da empresa, capital social, atividade econômica e quadro societário quando disponível em fonte pública.
- **Órgãos e território:** cidade, UF, unidade gestora, estado e agregações por região para leitura rápida no painel.
- **GPS, mapas e distância:** uso de coordenadas públicas ou aproximadas por município/UF para mapear alertas e estimar distância entre fornecedor e órgão contratante.
- **Risco logístico:** sinalização quando há contrato de alto valor, atividade que exige presença física e grande distância entre empresa e órgão.
- **Histórico e recorrência:** comparação com contratos anteriores, concentração por fornecedor, recorrência em janela curta e possíveis padrões de repetição.
- **Preço e superfaturamento estimado:** comparação estatística por grupos de itens similares para estimar valores atípicos que merecem revisão.
- **Pessoas e relações públicas:** busca complementar por políticos, partidos, CNPJ, STF, Câmara, Senado e bases abertas relacionadas.
- **Evidências rastreáveis:** cada alerta preserva links, critérios e campos usados no cálculo para facilitar auditoria humana.

Esses cruzamentos servem para priorizar leitura e investigação. Eles não substituem análise jurídica, contábil, técnica ou decisão de autoridade competente.

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

## Licenca e creditos

Uso publico permitido com atribuicao obrigatoria ao autor original. Nao e
permitida a venda do software, de derivados, hospedagens, acessos, APIs,
integracoes, paineis, servicos ou qualquer oferta comercial relacionada ao
aplicativo COIBE.IA sem autorizacao previa e expressa de Jp Andre.

Ao usar, copiar, modificar, hospedar, redistribuir, apresentar ou criar trabalho
derivado do COIBE.IA, mantenha credito visivel e razoavel:

`COIBE.IA por Jp Andre`

A licenca completa esta em [LICENSE](LICENSE).

## Arquitetura atual

- Frontend: React, Vite, TailwindCSS e Lucide React.
- Backend: FastAPI + Uvicorn.
- Dados: pasta local `data/` no backend do operador ou storage S3 compatível.
- Monitoramento: `local_monitor.py` coleta, normaliza, analisa e atualiza a base.
- Publicação leve: frontend estático na Hostinger e backend local exposto via Cloudflare Tunnel ou backend Docker em Render.

### Geoespacial e cache

- O mapa usa cache JSON por TTL mesmo sem banco externo.
- Quando `COIBE_POSTGIS_DATABASE_URL` estiver configurada, o backend cria `CREATE EXTENSION IF NOT EXISTS postgis` e usa PostGIS para armazenar, indexar e consultar pontos de municipio/UF.
- A API aceita filtros separados de `uf`, `city`, `q` e bbox quando aplicavel, reduzindo varreduras textuais e chamadas repetidas.
- O zip de frontend continua independente do backend: a URL publica da API vem de `VITE_API_BASE_URL` ou do tunel local configurado.

## Uso responsável dos dados

A Coibe IA não acusa fraude, corrupção ou irregularidade definitiva. Os resultados indicam fatores de atenção, padrões estatísticos e cruzamentos públicos que precisam ser interpretados por pessoas e, quando aplicável, por órgãos competentes.

## Backend

```powershell
Copy-Item .env.example .env
# Edite o .env e preencha PORTAL_TRANSPARENCIA_API_KEY
py -3.10 -m pip install -r requirements.txt
py -3.10 -m uvicorn main:app --reload
```

PostGIS local opcional:

```powershell
.\setup-postgis-local.ps1 -PostgresPassword coibe_local_2026
```

No Windows, `start-coibe-backend.bat`, `start-coibe-backend.ps1` e
`iniciar_coibe_completo.bat` tentam iniciar o servico PostgreSQL local e validar
o PostGIS antes de subir o backend.

No `.env`, habilite:

```env
COIBE_POSTGIS_ENABLED=true
COIBE_POSTGIS_DATABASE_URL=postgresql://postgres:coibe_local_2026@127.0.0.1:5432/coibe
COIBE_MAP_CACHE_TTL_SECONDS=120
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
- PNCP API Consulta
- Portal da Transparencia CGU, quando `PORTAL_TRANSPARENCIA_API_KEY` estiver configurada

A busca indexada usa a base acumulada em `data/processed` com fuzzy search simples para resposta instantanea e tolerancia a erro. Em producao pesada, o proximo salto e embeddings persistidos em PostgreSQL com `pgvector`, distancia geografica no PostGIS e OpenSearch/Elasticsearch para indice invertido.

### Feed real com lazy load

```http
GET /api/monitoring/feed?page=1&page_size=10
GET /api/monitoring/feed?page=1&page_size=10&q=banco
GET /api/monitoring/feed?page=1&page_size=10&uf=SP&city=Sao%20Paulo
GET /api/monitoring/feed?page=1&page_size=10&source=live
GET /api/monitoring/feed?page=1&page_size=10
```

Observacao operacional: o feed carrega 10 itens por vez apenas na interface. A base da plataforma nao fica limitada a 10 registros; o monitor acumula dados continuamente e o parametro `source=live` consulta as APIs publicas diretamente.

Retorna 10 itens por vez, em ordem cronológica, consolidando contratos reais do Compras.gov.br Dados Abertos e da API Consulta do PNCP. A busca aceita órgão, fornecedor, objeto, CNPJ, cidade ou UF.
O backend tenta janelas recentes automaticamente: 45, 120 e 240 dias. Se uma fonte ainda não tiver dados no período, usa a janela histórica mais recente disponível como fallback.
Cada item pode ser enriquecido por BrasilAPI CNPJ, CEIS/CNEP, contratos por fornecedor e notas fiscais do Portal da Transparencia. Esses registros entram em `report.public_evidence`, alimentam flags de risco e são persistidos em `data/processed/public_api_records.json`.

### Mapa real de alertas

```http
GET /api/monitoring/map?page_size=50
GET /api/monitoring/map?page_size=240&uf=SP&city=Sao%20Paulo
GET /api/monitoring/map?page_size=240&min_lat=-25&max_lat=-19&min_lng=-53&max_lng=-44
GET /api/monitoring/state-map?page_size=80
GET /api/monitoring/state-map?page_size=80&source=live
GET /api/public-data/ibge/states-geojson
```

Agrega contratos reais por município/UF da UASG e retorna coordenadas, quantidade de alertas, valor total e score de risco.
O frontend usa a malha oficial do IBGE para renderizar o mapa do Brasil por estados, com zoom por scroll, pontos por municipio, labels em aproximacao e filtro do feed por cidade/UF.
Quando PostGIS esta configurado, os pontos entram em tabela indexada por `geometry(Point, 4326)`; sem PostGIS, o backend usa `data/cache/monitoring-map.json`.

Fontes usadas nesta etapa:

- Compras.gov.br Dados Abertos: contratos públicos federais
- PNCP API Consulta: contratos, empenhos e contratacoes publicados no portal nacional
- IBGE/UASG: município e UF da unidade gestora
- PNCP: referência oficial de contratação quando o identificador estiver disponível
- Querido Diário: busca complementar em diários oficiais municipais

### Decisao geoespacial em 2026

PostGIS continua sendo a melhor escolha padrao para o COIBE porque fica no mesmo banco transacional do backend, oferece tipos `geometry/geography`, indice espacial GiST e funcoes SQL maduras para filtro por area, distancia e intersecao.

Alternativas avaliadas:

- DuckDB Spatial: excelente para analise local, GeoParquet e pipelines offline, mas nao substitui tao bem um cache transacional multiusuario da API.
- Apache Sedona/SedonaDB: forte para processamento espacial em escala lakehouse/Spark/Flink, porem mais pesado para o mapa operacional atual.
- ClickHouse geo/H3/S2: bom para analitica de eventos e grande volume de leitura, mas exige outra arquitetura para CRUD/cache de pontos do painel.
- H3/S2 podem complementar PostGIS como indice hierarquico de zoom no futuro, principalmente para tiles e agregacoes por celula.

### Varredura política preventiva

```http
GET /api/political/parties?limit=12
GET /api/political/politicians?limit=18
```

As abas **Partido** e **Político** mostram fatores de atenção em linguagem simples,
sem acusação ou conclusão jurídica. A leitura usa dados reais da Câmara dos
Deputados, Senado Federal, TSE, STF, TCU e cruzamentos internos da plataforma
para destacar: políticos em exercício, políticos fora do exercício, pessoas
públicas relacionadas, volume de dinheiro público no recorte,
viagens/deslocamentos, concentração de fornecedores, pessoas e empresas
envolvidas e fontes oficiais para conferência de processos, contas e controle
externo. A plataforma lista consultas oficiais; não conclui crime, culpa,
suborno, corrupção ou desfecho judicial.

O monitor também cruza cada político e partido com a base local de contratos já
analisados, registros eleitorais/doações já carregados, fornecedores do recorte
e movimentos de alto valor em pessoas ou nomes próximos. Esses sinais aparecem
como fatores de conferência humana, sem afirmar parentesco, favorecimento ou
irregularidade automaticamente.

A ordem padrão da varredura política usa prioridade institucional: Presidência,
Vice-Presidência, partido da Presidência atual, partido da Vice-Presidência,
ex-presidentes, Senado, Câmara e demais registros. Isso só define prioridade de
fila; a plataforma continua varrendo outros políticos e partidos conforme a
base cresce.

Por padrao, essas abas leem a base local consolidada em segundo plano pelo
monitor, como o feed. A consulta publica das abas nao dispara nova varredura:
se o termo nao estiver no cache, retorna vazio. O modo `source=live` fica
reservado ao monitor local em `127.0.0.1` ou a chamadas administrativas com
`X-Coibe-Admin-Token`.
Na interface, Partido e Politico tambem usam lazy loading: a tela carrega
blocos pequenos da base ja analisada e busca mais registros conforme o usuario
desce, sem recalcular totais nem reconsultar as fontes oficiais.

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

O backend local tambem pode usar o modelo `coibe-deep-mlp`, treinado com `scikit-learn` sobre texto, fornecedor, orgao, valor, UF, score e fatores de risco ja calculados. Quando esse modelo e selecionado em `/backend`, a inferencia entra no ciclo de analise e adiciona o fator `DL-LOCAL-RISK-CLASSIFIER` apenas quando a confianca minima por classe e atingida. O modelo padrao continua sendo `coibe-adaptive-default`.

Para nao depender somente das APIs oficiais, o ciclo de monitoramento tambem faz varredura HTML publica em paginas oficiais e URLs descobertas por buscas locais. Essa varredura valida DNS/IP para bloquear localhost, rede privada e redirecionamentos inseguros antes de baixar texto publico.

O estado do aprendizado fica em `Models/`:

```text
Models/monitor_config.json
Models/monitor_model_state.json
Models/monitor_training_history.jsonl
Models/model_registry.json
Models/coibe_adaptive_deep_model.joblib
Models/coibe_adaptive_deep_model.onnx.json
Models/coibe_adaptive_deep_model.quant.json
```

O monitor reaproveita termos aprendidos nos ciclos seguintes para buscar mais
rápido. Se houver GPU NVIDIA e bibliotecas compatíveis (`cudf`/`cuml`), defina:

```powershell
$env:COIBE_ML_USE_GPU="true"
$env:COIBE_GPU_MEMORY_LIMIT_MB="2048"
```

Sem GPU, o monitor continua em CPU com `scikit-learn` ou fallback estatístico.
Em Windows com NVIDIA/CUDA, instale as dependências opcionais de GPU com:

```powershell
py -3.10 -m pip install -r requirements-gpu.txt
```

O backend controla o limite de memória da GPU pela UI local em
`/backend`, salvando o valor em `Models/monitor_config.json`.

A UI do backend fica em `http://127.0.0.1:8000/backend` e o status JSON em
`/api/models/status`. Essas rotas são locais: não ficam disponíveis pelo domínio
público/túnel.

Na UI local do backend é possível configurar limite de memória da GPU, uso de
memória compartilhada/RAM, tempo máximo de pesquisa, rodadas por ciclo e limites
de varredura política. Também há botões para iniciar/parar o treinamento sem
parar o backend, iniciar o backend em terminal visível e reiniciar o backend.
No Windows, o treinamento abre uma janela própria do terminal com o que está
sendo analisado, quantidade de registros lidos, tempo por fonte e velocidade
aproximada da varredura.

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
COIBE_ENV=production
PORTAL_TRANSPARENCIA_API_KEY=sua_chave
COIBE_CORS_ORIGINS=https://coibe.com.br,https://www.coibe.com.br
COIBE_ADMIN_TOKEN=gere-um-token-longo-e-secreto
COIBE_REQUIRE_ADMIN_TOKEN=true
COIBE_ENABLE_DOCS=false
COIBE_ENABLE_STORAGE_STATUS=false
COIBE_AUTO_MONITOR=false
```

Em producao, os endpoints pesados e administrativos exigem o header
`X-Coibe-Admin-Token` com o valor de `COIBE_ADMIN_TOKEN`. Isso protege rotas
como varredura prioritaria, scraping, analises POST, proxy da API do Portal da
Transparencia e diagnostico de storage.

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

Depois do deploy, valide se a API esta vendo os arquivos somente se voce ativar
temporariamente `COIBE_ENABLE_STORAGE_STATUS=true` e enviar o header
`X-Coibe-Admin-Token`:

```text
https://sua-api.onrender.com/api/storage/status
```

### Hardening para internet publica

- CORS deve ficar limitado a `https://coibe.com.br` e `https://www.coibe.com.br`.
- `/docs`, `/redoc` e `/openapi.json` ficam desligados por padrao em `COIBE_ENV=production`.
- `/api/scrape/public-page` bloqueia localhost, IPs privados, IPs reservados e respostas grandes para reduzir risco de SSRF.
- O backend aplica rate limit simples por IP e caminho. Para Cloudflare, adicione tambem regra de rate limit/WAF no painel.
- As abas de Partido e Politico consultam somente cache por padrao. `source=live`
  nessas rotas e permitido apenas a partir do loopback local ou com token admin,
  evitando que usuarios externos pelo tunnel iniciem varreduras repetidas.
- Nao coloque `COIBE_ADMIN_TOKEN` no frontend. Ele e somente para operacao administrativa fora do navegador publico.

## Monitoramento contínuo

O monitor segue a ordem correta do projeto: primeiro coleta dados públicos e salva snapshots da plataforma, depois aplica regras e ML nos dados coletados.
O arquivo responsável por deixar a máquina verificando e analisando os dados nacionais é:

[local_monitor.py](</c:/Users/jpzin/Downloads/Coibe/local_monitor.py:1>)

Rodar uma vez:

```powershell
py -3.10 local_monitor.py --once --pages 10 --page-size 50
```

Rodar em varredura de fundo periodica:

```powershell
py -3.10 local_monitor.py --interval-minutes 15 --pages 10 --page-size 50
```

Para reduzir ainda mais a carga nas APIs publicas, informe outro intervalo, por exemplo `--interval-minutes 60`.

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

1. `connectors_and_scrapers`: consulta conectores publicos e APIs configuradas, incluindo Compras.gov.br, PNCP, IBGE, Camara, Senado, Brasil API, Querido Diario, CEIS/CNEP, contratos/notas fiscais do Portal da Transparencia quando houver chave.
2. `raw_database_merge`: grava snapshots brutos e acumula registros deduplicados em `monitoring_items.json` e `public_api_records.json`.
3. `risk_rules_and_ml`: aplica regras COIBE e deteccao estatistica/ML apenas sobre os dados ja coletados.

A plataforma pode carregar 10 itens por vez no feed, mas o monitor continuo busca varias paginas por ciclo e preserva o historico. A base cresce conforme os conectores encontram novos registros.
A biblioteca cresce por insercao incremental: cada registro recebe uma chave estavel, entra uma vez no JSONL e nos ciclos seguintes o indice evita reprocessar o que ja foi salvo.
