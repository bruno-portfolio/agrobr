# Changelog

Todas as mudanças notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/),
e este projeto adere ao [Versionamento Semântico](https://semver.org/lang/pt-BR/).

## [Unreleased]

## [0.11.1] - 2026-02-21

### Changed
- **URLs centralizadas em `constants.py`** — 18 clients migrados de URLs hardcoded locais
  (`BASE_URL = "https://..."`) para `URLS[Fonte.XXX]["chave"]` importado de `agrobr.constants`.
  Dominio ou endpoint muda em UM lugar so. Clients afetados: abiove, anda, antaq, bcb, b3,
  comexstat, comtrade, deral, desmatamento, imea, inmet, mapbiomas, nasa_power, queimadas,
  usda, conab/serie_historica, conab/custo_producao, conab/progresso
- **Timeouts centralizados** — 3 clients (anda, inmet, nasa_power) migrados de
  `httpx.Timeout(connect=10.0, read=X, ...)` hardcoded para `HTTPSettings()` com override
  de `read` onde necessario. Todos os 25 clients agora usam `HTTPSettings`
- **Magic numbers substituidos por constantes nomeadas** — 19 thresholds de tamanho minimo
  (`< 50`, `< 100`, `< 500`, `< 1_000`, `< 5_000`) substituidos por `MIN_WFS_SIZE`,
  `MIN_CSV_SIZE`, `MIN_HTML_SIZE`, `MIN_ZIP_SIZE`, `MIN_XLSX_SIZE`, `MIN_HTML_PAGE_SIZE`
  em 16 clients. Ajuste de threshold agora requer edicao em UM lugar so

### Added
- `Fonte.COMTRADE` no StrEnum + URLs (base, auth, guest) + `rate_limit_comtrade` no HTTPSettings
- `URLS[Fonte.B3]["arquivos"]` — endpoint `arquivos.b3.com.br` centralizado
- `URLS[Fonte.DERAL]["downloads"]` — endpoint de downloads centralizado
- 6 constantes de tamanho minimo em `constants.py`: `MIN_WFS_SIZE` (50), `MIN_CSV_SIZE` (100),
  `MIN_HTML_SIZE` (500), `MIN_ZIP_SIZE` (500), `MIN_XLSX_SIZE` (1000), `MIN_HTML_PAGE_SIZE` (5000)

## [0.11.0] - 2026-02-21

### Fixed
- **CEPEA/NA — parser failure em soft block (#14)** — `cepea.indicador("soja")` e
  `datasets.preco_diario("soja")` falhavam com `ParseError` quando Noticias Agricolas
  retornava pagina de consent/challenge (~10KB sem tabela) em vez dos dados (~75KB com
  tabela). Tres mudancas: (1) `FetchResult(html, source)` NamedTuple no client CEPEA
  identifica explicitamente a fonte do HTML ("cepea", "browser", "noticias_agricolas"),
  eliminando deteccao fragil por markers no conteudo (`"noticiasagricolas" in html`);
  (2) `_validate_html_has_data()` no client NA rejeita respostas < 20KB sem `<table`
  (soft block) com `SourceUnavailableError`, ativando cache fallback; (3) roteamento
  no `api.py` usa `FetchResult.source` em vez de inspecionar HTML
- **ANP Diesel — normalizar produto** — `"OLEO DIESEL"` / `"ÓLEO DIESEL S10"` agora
  normalizado para `"DIESEL"` / `"DIESEL S10"` no output. Afeta `parse_precos`,
  `_parse_vendas_wide`, `_parse_vendas_long`. Regex `^[OÓ]LEO\s+` strip no produto.
  Filtro `produto=` tambem normaliza antes de comparar. Schema guarantee atualizada.
- **ANP Diesel — normalizar UF** — Coluna `ESTADO` com nome completo (ex: `"MATO GROSSO"`)
  agora convertida para sigla via `normalizar_uf()`. Fallback `or v.upper()` corrigido
  para `or ""` (antes retornava nome completo se normalizar_uf falhasse). Fix aplicado
  em `parse_precos`, `_parse_vendas_wide`, `_parse_vendas_long`.
- **CONAB Serie Historica — engine Excel** — `parse_serie_historica()` agora detecta
  formato via magic bytes (OLE2 BIFF = xlrd, senao openpyxl). Antes: `pd.ExcelFile()`
  sem engine falhava com `ValueError` para arquivos `.xls` reais da CONAB. Commit
  `38f5112` adicionou xlrd como dep mas nao corrigiu este parser.
- **Queimadas — fallback historico** — `fetch_focos_mensal()` agora tenta em cascata:
  `.csv` mensal (2024+) → `.zip` mensal (2023) → `.zip` anual (2003-2022). Antes:
  HTTP 404 para qualquer mes <2024 sem fallback. INPE migrou CSVs historicos para
  formato ZIP e dados pre-2023 so disponiveis como ZIP anual. Filtro por mes aplicado
  na api quando fonte e anual.

### Added
- **SICAR — Cadastro Ambiental Rural** — Novo namespace `agrobr/alt/sicar/` para dados do
  Cadastro Ambiental Rural (CAR) via GeoServer WFS (OGC 2.0.0, CSV sem geometria, sem auth).
  Funcoes: `sicar.imoveis(uf, municipio, status, tipo, area_min, area_max, criado_apos)` para
  registros individuais de imoveis rurais e `sicar.resumo(uf, municipio)` para estatisticas
  agregadas (total, por status, area, modulos fiscais, por tipo). Pagination transparente
  (resultType=hits + startIndex/count=10000), progressive delay apos pagina 5, timeout 180s.
  CQL_FILTER server-side para municipio (ILIKE), status, tipo, area range, data criacao.
  Contrato `SICAR_IMOVEIS_V1` (11 colunas, PK cod_imovel). Schema JSON, golden data (DF + MT).
  114 novos testes (models, client, parser, api). Sync wrapper via `agrobr.sync.alt.sicar`.
  Docs: `api/sicar.md`, `sources/sicar.md`
- **Dataset semantico `cadastro_rural`** — `datasets.cadastro_rural(uf, municipio, status, tipo,
  area_min, area_max, criado_apos)` na camada semantica. Wraps `sicar.imoveis()` com validacao
  de contrato, return_meta, modo deterministico e fallback pattern. Registrado no registry com
  contrato `SICAR_IMOVEIS_V1`. 10 novos testes.
- **ANTT Pedagio — Fluxo de Veiculos em Pracas de Pedagio** — Novo namespace `agrobr/alt/antt_pedagio/`
  para dados de fluxo de veiculos em pracas de pedagio rodoviario (ANTT Dados Abertos, CC-BY).
  Funcoes: `antt_pedagio.fluxo_pedagio(ano, ano_inicio, ano_fim, uf, apenas_pesados)` para
  trafego mensal com filtros por UF/concessionaria/rodovia/tipo de veiculo e
  `antt_pedagio.pracas_pedagio(uf, rodovia)` para cadastro georreferenciado (200+ pracas).
  CSV bulk 2010-2025 (16 arquivos), schema V1 (2010-2023) com categorias texto e V2 (2024+)
  com eixos numerico, encoding Windows-1252 com fallback automatico, join com cadastro de
  pracas para enriquecimento geografico (rodovia, UF, municipio).
  Contratos `ANTT_PEDAGIO_FLUXO_V1` (10 colunas) e `ANTT_PEDAGIO_PRACAS_V1` (9 colunas).
  Schemas JSON, golden data. 117 novos testes (models, client, parser, api). Sync wrapper
  via `agrobr.sync.alt.antt_pedagio`. Docs: `api/antt_pedagio.md`, `sources/antt_pedagio.md`
- **MAPA PSR — Seguro Rural** — Novo namespace `agrobr/alt/mapa_psr/` para dados de
  apolices e sinistros do seguro rural brasileiro (SISSER/MAPA, CC-BY). Funcoes:
  `mapa_psr.sinistros(cultura, uf, ano, evento)` para indenizacoes pagas e
  `mapa_psr.apolices(cultura, uf, ano)` para todas as apolices com subvencao federal.
  CSV bulk 2006+ (3 periodos), encoding auto-detect, PII removido automaticamente.
  Contratos `MAPA_PSR_SINISTROS_V1` (17 colunas) e `MAPA_PSR_APOLICES_V1` (18 colunas).
  Schemas JSON, golden data. 104 novos testes (models, client, parser, api). Sync wrapper
  via `agrobr.sync.alt.mapa_psr`. Docs: `api/mapa_psr.md`, `sources/mapa_psr.md`
- **ANP Diesel — Precos + Volumes** — Novo namespace `agrobr/alt/anp_diesel/` para dados de
  precos de revenda e volumes de venda de diesel da ANP. Funcoes:
  `anp_diesel.precos_diesel(uf, municipio, produto, nivel, agregacao)` para precos
  semanais/mensais por municipio/UF/Brasil e `anp_diesel.vendas_diesel(uf)` para volumes
  mensais por UF. XLSX bulk 2013+ (openpyxl), cache por periodo do arquivo.
  Contratos `ANP_DIESEL_PRECOS_V1` (8 colunas) e `ANP_DIESEL_VENDAS_V1` (5 colunas).
  Schemas JSON, golden data. 103 novos testes (models, client, parser, api). Sync wrapper
  via `agrobr.sync.alt.anp_diesel`. Docs: `api/anp_diesel.md`, `sources/anp_diesel.md`
- **UN Comtrade — Trade Mirror** — Novo modulo `comtrade/` para dados de comercio
  internacional bilateral via UN Comtrade API. Funcoes: `comtrade.comercio()` (dados
  bilaterais por HS code/pais/periodo) e `comtrade.trade_mirror()` (compara exportacoes
  do reporter vs importacoes do parceiro, calcula discrepancias peso/valor/ratio).
  Guest mode (sem API key) + `AGROBR_COMTRADE_API_KEY` para rate limit maior.
  Chunking automatico para periodos > 12 meses. 17 produtos agro mapeados por HS code.
  Contratos `COMERCIO_BILATERAL_V1` e `TRADE_MIRROR_V1`. Golden data (comercio + mirror).
  70 novos testes. Sync wrapper. Docs: `api/comtrade.md`, `sources/comtrade.md`
- **ANTAQ — Movimentacao Portuaria** — Novo modulo `antaq/` para dados de movimentacao
  portuaria de carga do Estatistico Aquaviario (ANTAQ). Funcao `antaq.movimentacao(ano)`
  baixa ZIP bulk anual (~80MB), extrai e faz join de 3 tabelas (Atracacao + Carga + Mercadoria).
  Filtros: tipo_navegacao, natureza_carga, mercadoria, porto, uf, sentido.
  Encoding UTF-8-sig, separador `;`, decimal brasileiro (`,`). Historico desde 2010.
  Contrato `MOVIMENTACAO_PORTUARIA_V1` com 21 colunas. Schema JSON, golden data.
  72 novos testes (client, parser, models, api). Sync wrapper. Docs: `sources/antaq.md`
- **B3 Posicoes em Aberto (Open Interest)** — Novas funcoes `b3.posicoes_abertas()` e
  `b3.oi_historico()` para dados de open interest diario de futuros e opcoes agro
  (BGI, CCM, ETH, ICF, SJC). CSV publico via `arquivos.b3.com.br` (2-step: token + download).
  Parser filtra segmento AGRIBUSINESS, classifica futuro/opcao, enriquece com descricao e unidade.
  Contrato `POSICOES_ABERTAS_V1` com PK `[data, ticker_completo]`, 11 colunas. Schema JSON,
  golden data (518 linhas agro, 2025-12-19), 61 novos testes. Docs: `api/b3.md`, `sources/b3.md`
  atualizados
- **BCB/SICOR dimensoes ocultas** — Expoe 5 dimensoes que a API retorna mas eram ignoradas:
  programa, fonte de recurso, tipo de seguro, modalidade e atividade. Cada dimensao gera
  duas colunas: codigo (`cd_programa`) e nome legivel (`programa`). Dicionarios hardcoded
  com fallback `"Desconhecido ({code})"` + log warning para codigos novos. Enriquecimento
  no parser (PARSER_VERSION=2). Novos parametros `programa` e `tipo_seguro` para filtro
  client-side. Nova agregacao `agregacao="programa"`. Contract v1.1 com 11 novas colunas
  nullable (nao quebra consumidores v1.0). Schema JSON regenerado. 87 novos testes
  (models, parser, api). Suite: 2778 passed, 0 failed

### Changed
- `credito_rural` contract bump v1.0 → v1.1 (minor — novas colunas nullable)
- `PARSER_VERSION` bump 1 → 2 (novas colunas + enriquecimento)
- Golden data `custeio_sample/expected.json` atualizado com 20 colunas (era 15)

## [0.10.1] - 2026-02-16

### Fixed
- **DuckDB thread-safety** — `DuckDBStore` agora usa `threading.Lock` em todos os
  métodos que acessam a conexão. `get_store()` usa double-checked locking. Corrige
  segfault/deadlock quando múltiplas threads compartilham o singleton (ex: MCP server
  despachando requests para threads diferentes)
- **Parser NA semanal** — `_parse_date` aceita formato semanal `'09 - 13/02/2026'`
  (média CEPEA semanal). Registros semanais marcados com `anomalies=["media_semanal"]`
  e `meta["tipo"]="media_semanal"`. Antes: linhas ignoradas com warning
  `parse_row_failed`
- **ANDA ano errado** — `fetch_entregas_pdf` agora retorna `tuple[bytes, int]` com
  `ano_real` extraído do texto do link (não da URL de upload que contém o ano do
  upload, não dos dados). Corrige parser buscando header "2026" em PDF de dados 2025

### Changed
- `integration_tests.yml` — timeout global adicionado
- `pyproject.toml` — `pytest-timeout` adicionado como dependência de teste
- 3 testes de thread-safety no DuckDB store (`test_threaded_reads`,
  `test_threaded_writes`, `test_threaded_indicadores` — 5 threads × 10-20 ops cada)
- Suite: 2719 passed, 0 failed (era 2660+)

## [0.10.0] - 2026-02-15

### Added
- **CONAB CEASA/PROHORT (Precos de Atacado Hortifruti)** — Nova fonte: precos diarios de atacado
  de 48 produtos (20 frutas, 28 hortalicas) em 43 CEASAs do Brasil. Modulo `agrobr/conab/ceasa/`
  com client (Pentaho CDA REST API, JSON), parser (pivot 48x43 -> long-form, datas por header,
  mapeamento posicional CEASAs), models (48 produtos, 43 CEASAs, UF map, categorias).
  API publica `conab.ceasa_precos()` com filtros por produto/ceasa, `conab.ceasa_produtos()`,
  `conab.lista_ceasas()`, `conab.ceasa_categorias()`. Contrato `PRECO_ATACADO_V1` com
  PK `[data, produto, ceasa]`. Schema JSON, golden data (Pentaho real), 70 testes. Warning
  zona_cinza na primeira chamada. Docs: `sources/conab.md`, licenses atualizado
- **B3 Futuros Agro** — Nova fonte: ajustes diarios de futuros agricolas (boi gordo, milho,
  cafe arabica, cafe conillon, etanol, soja cross, soja FOB). Modulo `agrobr/b3/` com client
  (HTML parse de `www2.bmf.com.br`, encoding iso-8859-1), parser (tabela `tblDadosAjustes`,
  carry-forward de ticker, numeros BR), models (7 contratos, month codes, unidades).
  API publica `b3.ajustes()` com filtro por contrato, `b3.historico()` para serie temporal,
  `b3.contratos()`. Contrato `AJUSTE_DIARIO_V1` com PK `[data, ticker, vencimento_codigo]`.
  Schema JSON, golden data (dia util + weekend), 71 testes. Warning zona_cinza na primeira
  chamada. Sync wrapper. Docs: `api/b3.md`, `sources/b3.md`, licenses atualizado
- **IBGE Censo Agropecuário (Censo Agro 2017)** — Nova pesquisa no módulo IBGE:
  4 temas (efetivo_rebanho, uso_terra, lavoura_temporaria, lavoura_permanente) via tabelas
  SIDRA 6907/6881/6957/6956. API pública `ibge.censo_agro()` com filtros por tema/UF/nível
  e `ibge.temas_censo_agro()`. Dataset `censo_agropecuario` com contrato `ibge.censo_agro v1.0`,
  schema JSON, golden data. Long format (variável/valor por linha). Cache 30 dias.
  52 testes. Docs: `api/ibge.md`, `sources/ibge.md`, `contracts/censo_agropecuario.md`, licenses
  atualizado
- **IBGE Abate Trimestral**: abate bovino, suíno e frango por UF desde 1997 — 54 testes, contrato, golden data
- **IBGE PPM — Pesquisa da Pecuária Municipal (roadmap 2.8)** — Nova pesquisa no módulo IBGE:
  efetivo de rebanhos (10 espécies, tabela SIDRA 3939) e produção de origem animal (6 produtos,
  tabela 74). API pública `ibge.ppm()` com filtros por espécie/ano/UF/nível e `ibge.especies_ppm()`.
  Dataset `pecuaria_municipal` com contrato `IBGE_PPM_V1`, schema JSON, golden data. Cache 7 dias.
  60 testes. Docs: `api/ibge.md`, `sources/ibge.md`, `contracts/pecuaria_municipal.md`, licenses
  atualizado
- **CONAB Progresso de Safra (roadmap 2.0.5)** — Nova fonte: progresso semanal de plantio
  e colheita por cultura x UF. Modulo `agrobr/conab/progresso/` com client (Plone CMS
  pagination, XLSX download via sub-links), parser (block-based state machine para XLSX
  com blocos repetidos por cultura/operacao), models (6 culturas, 27 UFs, normalizacao
  estado→UF). API publica `conab.progresso_safra()` com filtros por cultura/estado/operacao
  e `conab.semanas_disponiveis()` para listar semanas. Contrato `CONAB_PROGRESSO_V1` com
  PK `[cultura, safra, operacao, estado, semana_atual]`. Golden data, 67 testes. Docs:
  `api/conab_progresso.md`, `sources/conab_progresso.md`, licenses atualizado
- **MapBiomas (roadmap 2.7)** — Nova fonte: cobertura e uso da terra por municipio/ano
  (1985-presente). Modulo `agrobr/mapbiomas/` com client (download XLSX do Google Cloud
  Storage), parser (multi-sheet com classes de cobertura MapBiomas Collection 9), models
  (classes de cobertura, biomas, transicoes). API publica `mapbiomas.cobertura()` com
  filtros por municipio/UF/bioma/classe/ano e `mapbiomas.transicao()`. Contrato
  `MAPBIOMAS_COBERTURA_V1`. Golden data, 66 testes. Docs: `api/mapbiomas.md`,
  `sources/mapbiomas.md`, licenses atualizado
- **Desmatamento PRODES/DETER (roadmap 2.2)** — Nova fonte: dados de desmatamento via
  TerraBrasilis GeoServer (WFS). Modulo `agrobr/desmatamento/` com client (WFS+CSV, CQL_FILTER
  por UF/ano/data), parser (PRODES anual + DETER alertas), models (workspaces por bioma,
  classes DETER, mapeamento UF/estado). API publica `desmatamento.prodes()` (5 biomas: Cerrado,
  Caatinga, Mata Atlantica, Pantanal, Pampa) e `desmatamento.deter()` (Amazonia, Cerrado)
  com filtros por bioma/UF/ano/classe e suporte a `return_meta`. Contratos
  `DESMATAMENTO_PRODES_V1` e `DESMATAMENTO_DETER_V1`. Schemas JSON, golden data (PRODES 10
  registros x 9 UFs, DETER 10 registros x 5 UFs x 4 classes), 56 testes. Export em
  `__init__.py` e sync wrapper. Docs: `api/desmatamento.md`, `sources/desmatamento.md`,
  licenses atualizado
- **Queimadas/INPE (roadmap 2.1)** — Nova fonte: focos de calor detectados por satelite via
  BDQueimadas/INPE. Modulo `agrobr/queimadas/` com client (CSV diario/mensal), parser
  (UTF-8 + latin-1 fallback), models (6 biomas, 27 UFs, 13 satelites), API publica
  `queimadas.focos()` com filtros por UF/bioma/satelite e suporte a `return_meta`.
  Contrato `FOCOS_QUEIMADAS_V1` com PK `[data, lat, lon, satelite, hora_gmt]`.
  Schema JSON, golden data (8 registros x 6 biomas), 43 testes. Export em `__init__.py`
  e sync wrapper. Docs: `api/queimadas.md`, `sources/queimadas.md`, licenses atualizado
- **Schemas JSON formais (roadmap 1.2)** — Contratos Python agora geram schemas JSON em
  `agrobr/schemas/`. 8 contratos com primary_key, min/max constraints, validação automática
  via `_validate_contract()` em todos os 8 datasets. Novos contratos: `credito_rural`,
  `exportacao`, `fertilizante`, `custo_producao`. Registry centralizado com
  `register_contract()` / `get_contract()` / `validate_dataset()`. 60 testes dedicados.
  `Contract.to_json()` / `from_json()` para serialização roundtrip
- **Normalização transversal (roadmap 1.3)** — Dois novos módulos em `agrobr/normalize/`:
  - `municipalities.py` — Mapeamento nome→código IBGE para 5571 municípios brasileiros.
    Busca accent/case insensitive. `municipio_para_ibge()`, `ibge_para_municipio()`,
    `buscar_municipios()`. Dados da API IBGE Localidades (livre para uso)
  - `crops.py` — Dicionário unificado de 140+ variantes→35 culturas canônicas.
    `normalizar_cultura()` resolve "SOJA", "soja em grão", "soybean" → "soja".
    `listar_culturas()`, `is_cultura_valida()`. Substitui aliases dispersos
  - 464 testes novos (100 municípios x 3 variações + culturas). Total suite: 2128 testes
- **Politica de versionamento datasets (roadmap 1.4)** — `docs/contracts/semver.md` expandido
  com tabela detalhada de bump rules (major/minor/patch), principio de `schema_version`
  independente de `lib_version`, criterios de breaking change para datasets
- **Metadados no registry (roadmap 1.9)** — `DatasetInfo` expandido com `source_url`,
  `source_institution`, `min_date`, `unit`, `license`. 8 datasets preenchidos com metadados
  reais (instituição, URL, licença, data mínima, unidade). Registry ganha
  `describe(name)` e `describe_all()` para exibição formatada
- **Testes de integracao formalizados (roadmap 1.8)** — Distinção clara entre unit/golden
  (todo push), integration (cron semanal) e benchmark (manual). Markers `@pytest.mark.integration`
  e `@pytest.mark.benchmark` registrados em `pyproject.toml`. CI padrao exclui ambos:
  `pytest -m "not integration and not benchmark"`
- **CI health check semanal (roadmap 1.5)** — `.github/workflows/integration_tests.yml`:
  cron segunda 08:00 UTC, `pytest -m integration --tb=short --timeout=120`, issue automatica
  com label `source-changed` em caso de falha, alertas Discord/Slack, artefato de resultados
  (30 dias). Nao bloqueia release — apenas alerta
- **Cobertura CLI/alerts/health** — 107 testes novos: `test_cli.py` (51), `test_alerts/test_notifier.py` (17),
  `test_health/test_checker.py` (15), `test_health/test_reporter.py` (24). Total suite: 1640 testes. Closes #11
- **Golden tests com dados reais** para 5 fontes: BCB, IBGE, ComexStat, DERAL, ABIOVE
  (substituindo dados sintéticos). Script `scripts/update_golden.py` expandido com
  captura automatizada para 6 fontes (5 novas + CEPEA existente). Closes #10
- **Audit de licenças** — `docs/licenses.md` com tabela completa das 13 fontes,
  classificação (`livre`, `nc`, `zona_cinza`, `restrito`) e URLs dos termos
- **Aviso CC BY-NC 4.0** no módulo CEPEA (docstrings em `__init__.py` e `api.py`)
  e na documentação (`docs/sources/cepea.md`)
- **Avisos de licença** nos módulos IMEA (`restrito`), Notícias Agrícolas
  (`restrito`, deprecação pendente), ANDA e ABIOVE (`zona_cinza`, autorização
  solicitada fev/2026)
- **Runtime warnings** — `warnings.warn()` no primeiro uso de IMEA e Notícias
  Agrícolas alertando sobre restrições de redistribuição
- **Warning box no README** apontando para `docs/licenses.md`
- **Cache key versionada** — `build_cache_key()` em `agrobr/cache/keys.py`:
  formato `{dataset}|{params_hash}|v{lib_version}|sv{schema_version}`,
  garante invalidação automática entre versões da lib e mudanças de schema
- **Cache versionado completo** — migração automática de keys legacy para formato
  versionado (`legacy_cache_migrated`), strict mode via `AGROBR_CACHE_STRICT=1`
  (rejeita cache de versão divergente), `parse_cache_key()` / `is_legacy_key()`
  / `legacy_key_prefix()` em `cache/keys.py`. 16 testes novos (migração, strict,
  concorrência 3 threads)
- **HTTP settings centralizados** — `agrobr/http/settings.py` com `get_timeout()`,
  `get_rate_limit()`, `get_client_kwargs()`. `rate_limit_default` (1 req/s) no
  `HTTPSettings`. Env vars `AGROBR_HTTP_TIMEOUT_*` e `AGROBR_HTTP_RATE_LIMIT_*`
  configuram tudo. 14 testes novos

### Fixed
- **Pydantic `class Config` → `model_config`** — 4 Settings classes em `constants.py`
  migradas de `class Config` (deprecated) para `model_config = SettingsConfigDict(...)`.
  Elimina 4 `PydanticDeprecatedSince20` warnings em toda importação do agrobr
- **MapBiomas sync wrapper** — `_SyncMapBiomas` adicionado ao `sync.py`. Antes:
  `from agrobr.sync import mapbiomas` lançava `ImportError`
- **Warnings zona_cinza ANDA/ABIOVE** — `_WARNED` + `warnings.warn()` adicionados
  em `anda/api.py` e `abiove/api.py` (padrão já existente em B3, CEASA, IMEA, NA)
- **Schemas JSON desatualizados** — `generate_json_schemas()` regenerou 19 schemas
  (3 novos: mapbiomas_cobertura, mapbiomas_transicao, conab_progresso; 16 atualizados)
- **Pre-commit limpo** — SIM117 (nested `with` combinados), mypy `untyped-decorator`
  no cli.py, erros pré-existentes em `scripts/` e `examples/` corrigidos (27 erros mypy)
- **Parser ABIOVE** — suporte a formato single-sheet multi-seção (meses na coluna 1,
  seções por produto: grão, farelo, óleo, milho, total). Layout novo de 2024/2025.
- **Parser DERAL** — suporte a formato multi-produto por sheet (sheets nomeadas por
  data: "Atual", "Anterior", "10-02-2025"). Layout atual do PC.xls com tabela
  Condição/Fase por cultura em cada sheet.
- **7 clients legados migrados para `retry_on_status()`** — deral, imea, usda,
  abiove, bcb, comexstat, anda. ~445 linhas de retry duplicado removidas.
  Timeout/ConnectError propagam imediatamente (sem retry).
- **`indicadores_upsert` 7x mais rápido** — temp table + INSERT SELECT
  substitui INSERT row-by-row. 10k: 34s→4.8s, 50k: 187s→25.9s.
  Scaling agora linear (ratio 50k/10k ≈ 5.4x).

### Changed
- Retry loops dos 7 clients restantes migrados para `http/retry.py` centralizado
  (todos 13 clients agora usam `retry_on_status()`)
- `indicadores_upsert` usa chunks de 5000 via temp table `_ind_staging`
  com fallback row-by-row para isolamento de erros

## [0.9.0] - 2026-02-11

### Added
- **1529 testes** (era 949), cobertura **~75%** (era 57.5%) — atualizado para 1640 no Unreleased
- **Golden tests** para todas as 13 fontes de dados (era 2/13)
- **Benchmark de escalabilidade** — memory, volume, cache, async, rate limiting, sync, golden
- **Suporte a token INMET** — `AGROBR_INMET_TOKEN` via env var
- `retry_on_status()` e `retry_async()` centralizados em `http/retry.py`
- **Retry-After header** respeitado em respostas HTTP 429
- **Testes de resiliência HTTP** para todos os 13 clients (timeout, 429, 500, 403, resposta vazia)
- **Testes de API pública**: `cepea.indicador()`/`ultimo()`, `conab.safras()`/`balanco()`/`brasil_total()`/`levantamentos()`
- Pre-commit hooks atualizados (ruff v0.15, mypy v1.19)

### Fixed
- **Cache DuckDB** — `history_entries.id` sem autoincrement: histórico permanente nunca salvava dados
- **normalize/dates** — `normalizar_safra()` não fazia strip no input
- **6 clients sem retry para HTTP 429**: inmet, nasa_power, conab_custo, conab_serie, conab main, ibge
- **Graceful degradation silenciosa** trocada por `SourceUnavailableError` quando retry esgota
- **except Exception genérico** em `duckdb_store.py` restringido para exceções específicas
- **INMET** — endpoint `/estacao/dados/` atualizado para `/estacao/` (API mudou)
- **INMET** — tratamento de HTTP 204 (No Content) retorna DataFrame vazio

### Changed
- Retry loops de 5 clients migrados para `http/retry.py` centralizado
- Testes de datasets refatorados: 98 funções duplicadas → 27 parametrizadas (115 cenários)
- mypy override para `tests.*` (`ignore_errors = true`, strict mantido no core)

### Known Issues
- 4 golden tests com dados sintéticos (INMET, USDA, NA, ANDA) — `needs_real_data`
  (BCB, IBGE, ComexStat, DERAL, ABIOVE migrados para dados reais na issue #10)
- DuckDB 1.4.4 incompatível com coverage no Python 3.14

## [0.8.0] - 2026-02-09

### Added
- **ABIOVE** (`agrobr.abiove`) — Exportação do complexo soja
  - `abiove.exportacao()` — Volume e receita mensal de grão, farelo, óleo e milho
  - Parser Excel com detecção dinâmica de header
- **USDA PSD** (`agrobr.usda`) — Estimativas internacionais de oferta/demanda
  - `usda.psd()` — Dados PSD por commodity/país/ano via API FAS OpenData v2
  - Suporte a pivot, filtro por atributos, mapeamento PT-BR
  - Requer API key gratuita (api.data.gov)
- **IMEA** (`agrobr.imea`) — Cotações e indicadores Mato Grosso
  - `imea.cotacoes()` — Preços, progresso de safra, comercialização (6 cadeias)
  - API REST pública (api1.imea.com.br), sem autenticação
- **DERAL** (`agrobr.deral`) — Condição das lavouras Paraná
  - `deral.condicao_lavouras()` — Condição semanal (boa/média/ruim) + progresso plantio/colheita
  - Parser Excel (PC.xls) com detecção dinâmica de abas e produtos
- **CONAB série histórica** (`agrobr.conab.serie_historica`) — Sub-módulo de safras 2010+
  - `conab.serie_historica()` — Série histórica de safras por UF com filtros
  - Parser Excel com detecção dinâmica de header row
- **BCB BigQuery fallback** — `pip install agrobr[bigquery]`
  - Base dos Dados como fallback quando API OData retorna 500
  - `asyncio.to_thread()` para wrapping do SDK síncrono
- **5 novos datasets semânticos** (camada semântica):
  - `datasets.credito_rural()` — BCB/SICOR com fallback BigQuery
  - `datasets.exportacao()` — ComexStat → ABIOVE (fallback automático)
  - `datasets.fertilizante()` — ANDA (entregas por UF)
  - `datasets.custo_producao()` — CONAB custos de produção
  - Total: 8 datasets (era 4)
- 949 testes passando (era ~804)

### Fixed
- **BCB/SICOR** — Endpoints atualizados para API reestruturada (~2024)
  - `CusteioMunicipio` → `CusteioRegiaoUFProduto`
  - `InvestimentoMunicipio` → `InvestRegiaoUFProduto`
  - `ComercializacaoMunicipio` → `ComercRegiaoUFProduto`
  - `industrializacao` removida (sem endpoint equivalente)
- **BCB parser** — `COLUNAS_MAP` expandido para colunas da API nova (`VlCusteio`→`valor`, `nomeUF`→`uf`, `AreaCusteio`→`area_financiada`, etc.)
- **BCB parser** — Limpeza de aspas embarcadas em `nomeProduto` (`"\"SOJA\""` → `soja`)

### Changed
- **BCB client** — Server-side filter via `contains()` (unico operador suportado pelo Olinda v2); filtragem por ano/UF client-side
- **BCB client** — `MAX_RETRIES` 4→6, `timeout.read` 60→120s, `User-Agent` header adicionado
- **13 fontes** integradas (era 8): +ABIOVE, +USDA PSD, +IMEA, +DERAL, +Notícias Agrícolas
- `agrobr/constants.py` — Fonte enum +4, URLS +4, CacheSettings +4 TTLs, HTTPSettings +4 rate limits
- `agrobr/sync.py` — 4 novas classes _SyncModule (abiove, deral, imea, usda)
- `agrobr/http/rate_limiter.py` — 4 novas entradas no delays dict

## [0.7.1] - 2026-02-07

### Added
- **NASA POWER** (`agrobr.nasa_power`) — Dados climaticos globais como substituto do INMET
  - `nasa_power.clima_ponto()` — Dados diarios/mensais por coordenada (lat/lon)
  - `nasa_power.clima_uf()` — Dados climaticos por UF (ponto central)
  - 7 parametros agroclimaticos: temp (media/max/min), precipitacao, umidade, radiacao, vento
  - API REST pura (NASA LaRC), sem autenticacao, cobertura global desde 1981
  - Chunking automatico para periodos > 365 dias
  - 34 testes unitarios (models, parser, api)
- **NASAPowerCollector** no agrobr-collector (substitui INMETCollector)
- **Alertas automaticos** — health_check.yml e structure_monitor.yml enviam alertas Discord/Slack quando fontes degradam
- **Health checks reais** — CONAB (HTTP HEAD), IBGE (SIDRA API query com validacao de dados)
- **NASA POWER cache policy** dedicada (TTL 7d, stale 30d) em `policies.py`
- **Notebook demo** (`examples/agrobr_demo.ipynb`) — 14 secoes cobrindo todas as fontes, MetaInfo, fallback, cache, pipeline com graficos e modo async
- **Landing page** atualizada — text-shadow para legibilidade, copyright 2026, icone monocromatico, botao Colab no CTA

### Changed
- INMET desabilitado no collector (config.yaml `enabled: false`) — API dados retornando 404
- Docs atualizados: INMET referencia NASA POWER como alternativa
- `docs/index.md` atualizado com 8 fontes (era 3), NASA POWER no uso rapido
- `alert_on_anomaly` habilitado por padrao em `constants.py`
- `CacheSettings.ttl_nasa_power` corrigido de 24h para 7d (consistente com `policies.py`)
- `SOURCE_POLICY_MAP` corrigido: NASA_POWER aponta para `"nasa_power"` (era `"bcb"`)

### Fixed
- **sync.py** — `_SyncNasaPower` adicionado (nasa_power nao funcionava no modo sincrono)
- **Notebook cell 17** — PAM defensivo: detecta `"producao"` ou `"Quantidade produzida"` (SIDRA rename)
- **README** — Colab badge corrigido de `demo_colab.ipynb` para `agrobr_demo.ipynb`
- **cepea/client.py** — Variavel nao usada `produto_key` removida (ruff lint)

## [0.7.0] - 2026-02-07

### Added
- **INMET** (`agrobr.inmet`) — Dados meteorologicos de 600+ estacoes automaticas
  - `inmet.estacoes()` — Listar estacoes por tipo e UF
  - `inmet.estacao()` — Dados horarios/diarios de uma estacao
  - `inmet.clima_uf()` — Clima mensal agregado por UF
- **BCB/SICOR** (`agrobr.bcb`) — Credito rural por municipio e cultura
  - `bcb.credito_rural()` — Dados de credito de custeio por safra
- **ComexStat** (`agrobr.comexstat`) — Exportacoes brasileiras por NCM
  - `comexstat.exportacao()` — Exportacoes mensais com 19 produtos mapeados
  - Filtro por NCM usa prefix match (subposicoes capturadas automaticamente)
- **ANDA** (`agrobr.anda`) — Entregas de fertilizantes por UF/mes
  - `anda.entregas()` — Dados de entregas de fertilizantes
  - Parser suporta multiplas orientacoes de tabela PDF + layout "Principais Indicadores"
  - Requer `pip install agrobr[pdf]` (pdfplumber)
- **CONAB custo_producao** (`agrobr.conab.custo_producao`) — Custos de producao por hectare
  - `conab.custo_producao()` — Dados detalhados de custo por cultura/UF/safra
  - `conab.custo_producao_total()` — Totais COE/COT/CT

### Fixed
- **ComexStat**: NCM algodao corrigido de `52010000` (inexistente) para prefixo `520100` (captura `52010020` + `52010090`)
- **ComexStat**: `verify=False` no httpx para contornar certificado SSL invalido do `balanca.economia.gov.br`
- **ComexStat**: Filtro NCM no parser mudou de match exato (`==`) para prefix match (`str.startswith()`)
- **ANDA**: URL atualizada de `/estatisticas/` para `/recursos/` (reorganizacao do site)
- **ANDA**: Parser expandido com `_expand_newline_cells()` e `_parse_indicadores()` para PDFs com meses/valores concatenados
- **INMET**: User-Agent header adicionado ao client HTTP (previne 403 Forbidden)
- **custo_producao**: URLs migradas de `conab.gov.br` para `gov.br/conab/` com scraping multi-tab (3 abas de planilhas)
- **custo_producao**: `parse_links_from_html()` reescrito com BeautifulSoup + dedup de URLs

### Known Issues
- **INMET**: API de dados (`/estacao/dados/`) retornando 404 em todos endpoints — API fora do ar externamente
- **BCB/SICOR**: API OData retornando 503 Service Unavailable — indisponibilidade temporaria
- **custo_producao**: Graos (soja, milho, cafe, algodao) nao disponiveis como xlsx no gov.br — conteudo carregado via JavaScript dinamico

## [0.6.3] - 2026-02-06

### Fixed
- `__version__` atualizado para `0.6.3` (estava travado em `0.6.0` desde o v0.6.0)
- `.gitignore` corrompido — linhas garbled reescritas, adicionados roadmap v4 e insights
- README: parâmetro inexistente `periodo=` corrigido para `inicio=` na API CEPEA
- README: `cepea.produtos()` agora com `await` (função é async)
- `ruff>=0.14.0` corrigido para `ruff>=0.4.0` (versão 0.14 não existe)
- `site_url` corrigido no mkdocs.yml e pyproject.toml (era `agrobr.dev`, agora aponta para GitHub Pages)
- Testes CEPEA API marcados como `@pytest.mark.integration` (chamavam API real sem mock)

### Changed
- `playwright` movido de dependência core para extra `[browser]` (~50MB a menos no install padrão)
- Notícias Agrícolas client reescrito — Playwright removido, agora usa httpx puro (página é server-side rendered, não precisa de JS)
- `docs/sources/` (4 páginas órfãs) adicionadas ao nav do mkdocs.yml
- `docs/index.md` reescrito — agora reflete estado atual do projeto (datasets, 20 indicadores, features v0.6)
- Documentação atualizada: 20 produtos CEPEA, LSPA aliases, algodão cBRL/lb, troubleshooting sem Playwright
- Arquivo `nul` (artefato Windows) removido do repositório

## [0.6.2] - 2026-02-05

### Fixed
- URLs do Notícias Agrícolas corrigidas para milho, boi, café, algodão e trigo (retornavam 404)
- Unidade do algodão corrigida de `BRL/@` para `cBRL/lb` (centavos de real por libra-peso)
- Parser trigo ajustado para tabela com 4 colunas (Data, Região, R$/t, Variação)
- `wait_for_selector("table.cot-fisicas")` substituído por `wait_for_selector("table td")` — classe CSS não existe mais no site
- IBGE LSPA aceita nomes genéricos de produto (`milho` → `milho_1` + `milho_2`, `feijao` → `feijao_1` + `feijao_2` + `feijao_3`)
- Playwright cleanup no Windows — `atexit` handler evita `ValueError: I/O operation on closed pipe`
- Circuit breaker no CEPEA httpx — pula tentativa direta (403 Cloudflare) por 10min após primeira falha, eliminando ~2s de latência

### Added
- 11 novos produtos CEPEA via Notícias Agrícolas: arroz, açúcar cristal, açúcar refinado, etanol hidratado, etanol anidro, frango congelado, frango resfriado, suíno, leite, laranja indústria, laranja in natura
- Total de 20 indicadores CEPEA/ESALQ disponíveis via fallback Notícias Agrícolas
- Parser v2 com suporte a tabelas multi-região (trigo: Paraná + RS)
- Aliases LSPA: `milho`, `feijao`, `amendoim`, `batata` expandem para sub-safras automaticamente

## [0.6.1] - 2026-02-05

### Fixed
- Playwright graceful degradation — import com try/except, não crasha em Python 3.14+
- Parser Notícias Agrícolas levanta `ParseError` ao invés de retornar lista vazia silenciosamente
- Cache fallback automático com `StaleDataWarning` quando todas as fontes falham

## [0.6.0] - 2026-02-05

### Added
- **Camada Semântica** - 4 datasets padronizados com fallback automático entre fontes
  - `datasets.preco_diario()` - Preços diários (CEPEA → cache)
  - `datasets.producao_anual()` - Produção anual (IBGE PAM → CONAB)
  - `datasets.estimativa_safra()` - Estimativas safra corrente (CONAB → IBGE LSPA)
  - `datasets.balanco()` - Balanço oferta/demanda (CONAB)
  - `datasets.list_datasets()` / `datasets.list_products()` / `datasets.info()`
- **Contratos Públicos** - Garantias formais de schema versionado
  - Documentação em `docs/contracts/` para cada dataset
  - Colunas estáveis, tipos só alargam, breaking changes só em major
- **Modo Determinístico Aprimorado** - Context manager async com contextvars
  - `async with datasets.deterministic("2025-12-31"):` - Isolado por task
  - `@deterministic_decorator("2025-12-31")` - Decorator para funções
  - `is_deterministic()` / `get_snapshot()` - Verificar estado atual
- **Hierarquia de Exceções Expandida**
  - `NetworkError` - Erros de rede (timeout, HTTP error, DNS)
  - `ContractViolationError` - DataFrame não atende contrato do dataset
- **MetaInfo Expandido** - Novos campos de proveniência
  - `dataset` - Nome do dataset
  - `contract_version` - Versão do contrato
  - `snapshot` - Data de corte (modo determinístico)
- **Documentação Avançada**
  - `docs/advanced/reproducibility.md` - Guia de reprodutibilidade
  - `docs/advanced/pipelines.md` - Integração Airflow, Prefect, Dagster
- **Notebook Demo** - Google Colab com exemplos executáveis

### Changed
- `agrobr.sync.datasets` - API síncrona para datasets
- README atualizado com seção de datasets e status das fontes

## [0.5.0] - 2026-02-04

### Added
- **Plugin System** - Arquitetura extensível para fontes e validadores
  - `SourcePlugin` - Interface para novas fontes de dados
  - `ParserPlugin` - Interface para parsers customizados
  - `ExporterPlugin` - Interface para exportadores customizados
  - `ValidatorPlugin` - Interface para validadores customizados
  - `register()`, `get_plugin()`, `list_plugins()` - Gerenciamento de plugins
- **API Stability Decorators** - Marcadores de estabilidade de API
  - `@stable(since="x.y.z")` - Marca API como estável
  - `@experimental(since="x.y.z")` - Marca API como experimental
  - `@deprecated(since, removed_in, replacement)` - Marca API como deprecated
  - `@internal` - Marca API como interna (não pública)
  - `list_stable_apis()`, `list_experimental_apis()`, `list_deprecated_apis()`
- **SLA Documentado** - Contratos de nível de serviço por fonte
  - `SourceSLA` - Definição de SLA com tier, freshness, latency, availability
  - `CEPEA_SLA` - Tier CRITICAL, atualização diária 18h, 99% uptime
  - `CONAB_SLA` - Tier STANDARD, atualização mensal, 98% uptime
  - `IBGE_SLA` - Tier STANDARD, varia por pesquisa
  - `get_sla()`, `list_slas()`, `get_sla_summary()`
- **Certificação de Qualidade** - Sistema de certificação de dados
  - `QualityLevel` - GOLD, SILVER, BRONZE, UNCERTIFIED
  - `QualityCheck` - Check individual com status e detalhes
  - `QualityCertificate` - Certificado completo com score e validade
  - `certify(df)` - Executa checks (completeness, duplicates, schema, freshness, range)
  - `quick_check(df)` - Retorna (level, score) rapidamente

## [0.4.0] - 2026-02-04

### Added
- **Modo Determinístico** - Reprodutibilidade absoluta para backtests
  - `agrobr.set_mode("deterministic", snapshot="2025-01-01")`
  - `agrobr.configure()` para opções globais
  - `agrobr.get_config()` para consultar configuração atual
  - `agrobr.reset_config()` para resetar ao padrão
- **Sistema de Snapshots** - Gerenciamento de versões de dados
  - `create_snapshot()` - Cria snapshot dos dados atuais
  - `load_from_snapshot()` - Carrega dados de um snapshot
  - `list_snapshots()` / `delete_snapshot()` - Gerenciamento
  - CLI: `agrobr snapshot create/list/delete/use`
- **Export Auditável** - Formatos com metadados de proveniência
  - `export_parquet()` - Parquet com metadata embutido
  - `export_csv()` - CSV com arquivo sidecar .meta.json
  - `export_json()` - JSON com metadados opcionais
  - `verify_export()` - Verificação de integridade

## [0.3.0] - 2026-02-04

### Added
- **Stability Contracts** - Garantias formais de schema para todas as fontes
  - `CEPEA_INDICADOR_V1` - Contrato para indicadores de preço CEPEA
  - `CONAB_SAFRA_V1` - Contrato para dados de safra CONAB
  - `CONAB_BALANCO_V1` - Contrato para balanço oferta/demanda CONAB
  - `IBGE_PAM_V1` - Contrato para dados PAM do IBGE
  - `IBGE_LSPA_V1` - Contrato para dados LSPA do IBGE
  - `contract.validate(df)` - Validação automática contra contrato
  - `contract.to_markdown()` - Documentação automática
- **Validação Semântica** - Verificações avançadas de qualidade
  - Validação de preços positivos
  - Validação de faixas de produtividade por cultura
  - Detecção de anomalias em variação diária (>20%)
  - Consistência de sequência de datas
  - Consistência de áreas (colhida <= plantada)
  - Validação de formato de safra
  - `validate_semantic(df)` - Executa todas as regras
  - `get_validation_summary(df)` - Resumo das violações
- **Benchmark Suite** - Ferramentas para medição de performance
  - `benchmark_async()` / `benchmark_sync()` - Benchmark de funções
  - `run_api_benchmarks()` - Benchmark das APIs
  - `run_contract_benchmarks()` - Benchmark de validação de contratos
  - `run_semantic_benchmarks()` - Benchmark de validação semântica

### Changed
- Changelog reestruturado seguindo Keep a Changelog

## [0.2.0] - 2026-02-04

### Added
- **`agrobr doctor`** - Comando CLI para diagnóstico do sistema
  - Verificação de conectividade das fontes
  - Estatísticas do cache (tamanho, registros, por fonte)
  - Status de configuração
  - Output JSON (`--json`) e formatado Rich
- **Parâmetro `return_meta`** - Suporte a data lineage em todas as APIs
  - `cepea.indicador(return_meta=True)` retorna `(DataFrame, MetaInfo)`
  - `conab.safras(return_meta=True)` retorna `(DataFrame, MetaInfo)`
  - `ibge.pam(return_meta=True)` retorna `(DataFrame, MetaInfo)`
  - `ibge.lspa(return_meta=True)` retorna `(DataFrame, MetaInfo)`
- **Classe `MetaInfo`** - Metadados de proveniência e rastreabilidade
  - Informações da fonte (nome, URL, método)
  - Timing (duração fetch, duração parse)
  - Status do cache (from_cache, cache_key, expires_at)
  - Integridade do conteúdo (hash SHA256, tamanho)
  - Versões (agrobr, parser, schema, python)
  - `to_dict()` / `to_json()` para serialização
  - `verify_hash(df)` para verificação de integridade
- **Documentação** - Guias de proveniência e resiliência
  - `docs/sources/cepea.md` - Documentação da fonte CEPEA
  - `docs/sources/conab.md` - Documentação da fonte CONAB
  - `docs/sources/ibge.md` - Documentação da fonte IBGE
  - `docs/advanced/resilience.md` - Documentação de resiliência

### Changed
- `MetaInfo` exportado do pacote principal

## [0.1.2] - 2026-02-04

### Changed
- **Smart TTL** para cache CEPEA - expira às 18:00 (horário de atualização CEPEA)
- Reduz requests desnecessários em ~90%

## [0.1.1] - 2026-02-04

### Fixed
- Browser fallback desabilitado para CEPEA (Cloudflare bloqueia)
- CEPEA agora vai direto para Notícias Agrícolas, evitando timeout

## [0.1.0] - 2026-02-04

### Added
- **CEPEA**: Indicadores de preços agrícolas (soja, milho, boi, café, algodão, trigo)
  - Fallback automático para Notícias Agrícolas quando CEPEA bloqueado
  - Acumulação progressiva de histórico no DuckDB
- **CONAB**: Dados de safras e balanço oferta/demanda
  - Parser para planilhas XLSX do boletim de safras
  - Suporte a todos os produtos principais (soja, milho, arroz, feijão, etc.)
- **IBGE**: Integração com API SIDRA
  - PAM (Produção Agrícola Municipal) - dados anuais
  - LSPA (Levantamento Sistemático) - estimativas mensais
- **Cache**: Sistema de cache com DuckDB
  - Separação entre cache volátil e histórico permanente
  - TTL configurável por fonte
  - Acumulação progressiva de dados
- **HTTP**: Cliente robusto com resiliência
  - Retry com exponential backoff
  - Rate limiting por fonte
  - User-agent rotativo
  - Fallback para Playwright quando necessário
- **CLI**: Interface de linha de comando completa
  - Comandos para CEPEA, CONAB e IBGE
  - Exportação em CSV, JSON e Parquet
- **Validação**: Sistema de validação multinível
  - Pydantic v2 para validação de tipos
  - Validação estatística (sanity checks)
  - Fingerprinting de layout para detecção de mudanças
- **Monitoramento**: Health checks e alertas
  - Health check por fonte
  - Alertas multi-canal (Slack, Discord, Email)
  - Monitoramento de estrutura
- **Suporte Polars**: Todas as APIs suportam `as_polars=True`
- **Testes**: 96 testes passando (~80% cobertura)
- **CI/CD**: GitHub Actions configurados
  - Testes automatizados
  - Health check diário
  - Monitoramento de estrutura

### Technical Details
- Python 3.11+ required
- Async-first design com sync wrapper
- Type hints completos
- Logging estruturado com structlog

[Unreleased]: https://github.com/bruno-portfolio/agrobr/compare/v0.11.1...HEAD
[0.11.1]: https://github.com/bruno-portfolio/agrobr/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.10.1...v0.11.0
[0.10.1]: https://github.com/bruno-portfolio/agrobr/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.7.1...v0.8.0
[0.7.1]: https://github.com/bruno-portfolio/agrobr/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.6.3...v0.7.0
[0.6.3]: https://github.com/bruno-portfolio/agrobr/compare/v0.6.2...v0.6.3
[0.6.2]: https://github.com/bruno-portfolio/agrobr/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/bruno-portfolio/agrobr/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/bruno-portfolio/agrobr/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/bruno-portfolio/agrobr/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/bruno-portfolio/agrobr/releases/tag/v0.1.0
