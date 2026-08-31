# agrobr

> Brazilian agricultural data in one line of code

**🇧🇷 [Leia em Português](README.pt-BR.md)**

[![PyPI version](https://img.shields.io/pypi/v/agrobr)](https://pypi.org/project/agrobr/)
[![Downloads](https://static.pepy.tech/badge/agrobr)](https://pepy.tech/project/agrobr)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/agrobr)](https://pypi.org/project/agrobr/)
[![Tests](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml)
[![Daily Health Check](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml)
[![Docs](https://github.com/bruno-portfolio/agrobr/actions/workflows/docs.yml/badge.svg)](https://www.agrobr.dev/docs/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/bruno-portfolio/agrobr/blob/main/examples/agrobr_demo.ipynb)

<p align="center">
  <a href="https://htmlpreview.github.io/?https://github.com/bruno-portfolio/agrobr/blob/main/docs/canopy.html">
    <img src="docs/canopy.svg" width="100%" />
  </a>
</p>

Python infrastructure for Brazilian agricultural data with a semantic layer over **40 public sources** — market prices, production and crop seasons, foreign trade, rural credit, climate, environmental monitoring, land registries and regulatory data.

Brazil is one of the world's largest agricultural producers, but its public data is scattered across dozens of government portals, each with its own format, encoding and quirks. agrobr turns all of that into clean, validated DataFrames.

**v1.1.0** — 6,400+ passing tests, 92% coverage, golden tests with reference fixtures per source, centralized retry in every HTTP client.

## Demo
![Animation](https://github.com/user-attachments/assets/40e1341e-f47b-4eb5-b18e-55b49c63ee97)

## Installation

```bash
pip install agrobr
```

With optional extras:
```bash
pip install agrobr[pdf]             # pdfplumber for ANDA, Lista Suja, Rio Verde
pip install agrobr[polars]          # Polars support
pip install agrobr[browser]         # Playwright (optional, for JS-heavy sources)
pip install agrobr[bigquery]        # Base dos Dados (BCB/SICOR fallback)
pip install agrobr[geo]             # GeoPandas — enables _geo variants (PRODES, DETER, SICAR, FUNAI, ICMBio, INCRA, IBAMA, fire hotspots, MapBiomas Alerta, ANA, SFB, EMBRAPA Soils, INCRA land registry)
pip install agrobr[all]             # Everything included
```

### Docker

```bash
docker build -t agrobr .
docker run -it --rm agrobr
```

```python
>>> from agrobr.sync import cepea
>>> df = cepea.indicador('soja', inicio='2024-01-01')
```

```bash
# CLI
docker run --rm agrobr agrobr cepea indicador boi

# Persist cache between runs
docker run -it --rm -v agrobr-cache:/home/agrobr/.agrobr agrobr

# With additional extras (EXTRAS overrides the "browser,pdf" default)
docker build --build-arg EXTRAS="browser,pdf,polars" -t agrobr:extras .

# Run a local script
docker run --rm -v "$(pwd)":/work agrobr python /work/analysis.py
```

> The default image ships Playwright + Chromium and pdfplumber. See the [Docker guide](https://www.agrobr.dev/docs/guides/docker/) for additional extras.

## Usage by category

The examples below use the `async` form. For the equivalent without `async/await`, see [Sync mode](#sync-mode). Functions returning DataFrames accept `as_polars=True` and `return_meta=True` (provenance).

> **Note on naming:** function and parameter names follow Brazilian Portuguese domain terms (`indicador` = indicator, `safra` = crop season, `cultura` = crop, `inicio`/`fim` = start/end, `ano` = year, `mes` = month, `uf` = state code). They are part of the public API and stay stable.

### Prices and markets

CEPEA (daily spot), B3 (agricultural futures), IMEA (Mato Grosso), CONAB CEASA/PROHORT (wholesale produce), ANP Diesel.

```python
from agrobr import cepea

# CEPEA daily indicators — soybean, corn, coffee, cattle, wheat, cotton, rice, etc.
df = await cepea.indicador('soja', inicio='2024-01-01')
ultimo = await cepea.ultimo('soja')
print(f"Soybean: R$ {ultimo.valor}/60kg bag on {ultimo.data}")

print(await cepea.produtos())       # 21 products available
print(await cepea.pracas('soja'))   # trading locations per product
```

| Source | Flagship function | Doc |
|--------|-------------------|-----|
| **B3** agri futures | `b3.ajustes(data="13/02/2025")`, `b3.posicoes_abertas(data=...)`, `b3.historico(contrato="boi", inicio=..., fim=...)` | [docs/sources/b3.md](docs/sources/b3.md) |
| **CFTC COT** fund positioning (Chicago/NY) | `cftc.cot("soja", start="2026-05-01")` | [docs/sources/cftc.md](docs/sources/cftc.md) |
| **IMEA** Mato Grosso | `imea.cotacoes("soja", safra="24/25")` | [docs/sources/imea.md](docs/sources/imea.md) |
| **CONAB CEASA** | `conab.ceasa_precos(produto="tomate", ceasa="SAO PAULO")` | [docs/sources/conab_ceasa.md](docs/sources/conab_ceasa.md) |
| **ANP Diesel** | `alt.anp_diesel.precos_diesel(uf="MT")`, `alt.anp_diesel.vendas_diesel(uf="MT")` | [docs/sources/anp_diesel.md](docs/sources/anp_diesel.md) |

### Production and crop seasons

CONAB (crop surveys, supply/demand balance, production costs, historical series, planting/harvest progress), IBGE (PAM, LSPA, PPM, slaughter, PEVS, milk, GDP, agricultural census), DERAL, USDA PSD, ABIOVE, ANEC, UNICA, Rio Verde.

```python
from agrobr import conab, ibge

# CONAB — current season + supply/demand balance
df = await conab.safras('soja', safra='2024/25')
df = await conab.balanco('soja')
df = await conab.serie_historica('soja', inicio=2010, fim=2024)
df = await conab.progresso_safra(cultura='Soja', estado='MT', operacao='Colheita')
df = await conab.custo_producao(cultura='soja', uf='MT', safra='2024/25')

# IBGE — Municipal Agricultural Production (annual)
df = await ibge.pam('soja', ano=2023, nivel='uf')
df = await ibge.pam('cafe', ano=2023, nivel='municipio', uf='MG')
df = await ibge.lspa('soja', ano=2024, mes=6)        # monthly systematic survey
df = await ibge.ppm('bovino', ano=2023)               # municipal livestock
df = await ibge.abate('frango', trimestre='202303', uf='PR')

# Agricultural Census — 1995/2006/2017 + historical series 1920-2006 + 1985 municipal
df = await ibge.censo_agro('efetivo_rebanho')
df = await ibge.censo_agro_historico('estabelecimentos_area')
df = await ibge.censo_agro_municipal_1985('bovinos', uf='SP')
```

| Source | Flagship function | Doc |
|--------|-------------------|-----|
| **IBGE PEVS** | `ibge.silvicultura('madeira_tora', ano=2023)`, `ibge.extracao_vegetal('acai', ano=2023)` | [docs/sources/ibge.md](docs/sources/ibge.md) |
| **IBGE Milk** | `ibge.leite_trimestral(trimestre='202303', uf='MG')` | [docs/sources/ibge.md](docs/sources/ibge.md) |
| **IBGE Agri GDP** | `ibge.pib_agro(trimestre='202501', setor='agropecuaria')` | [docs/sources/ibge.md](docs/sources/ibge.md) |
| **DERAL** Paraná crop conditions | `deral.condicao_lavouras('soja')` | [docs/sources/deral.md](docs/sources/deral.md) |
| **USDA PSD** international | `usda.psd('soja', country='BR', market_year=2024)` (requires `AGROBR_USDA_API_KEY`) | [docs/sources/usda.md](docs/sources/usda.md) |
| **ABIOVE** soybean complex | `abiove.exportacao(ano=2024, produto='grao')` | [docs/sources/abiove.md](docs/sources/abiove.md) |
| **ANEC** weekly shipments | `anec.embarques(ano=2026)`, `anec.destinos(ano=2026)` | [docs/sources/anec.md](docs/sources/anec.md) |
| **UNICA** Center-South sugarcane crush | `unica.moagem_quinzenal('cana')`, `unica.safra_resumo()`, `unica.producao_historica('acucar')` | [docs/sources/unica.md](docs/sources/unica.md) |
| **Rio Verde** cultivar trials (MT) | `rio_verde.ensaio_soja(safra='2023/24')` | [docs/sources/rio_verde.md](docs/sources/rio_verde.md) |

### Trade and logistics

ComexStat (Brazil), UN Comtrade (bilateral worldwide), ANTAQ (ports), ANTT (highway toll traffic).

```python
from agrobr import comexstat, comtrade

# Brazilian exports/imports by NCM code and state, monthly
df = await comexstat.exportacao('soja', ano=2024, agregacao='mensal')
df = await comexstat.importacao('fertilizante', ano=2024)

# Worldwide bilateral trade (UN Comtrade)
df = await comtrade.comercio('soja', reporter='BR')
df = await comtrade.trade_mirror('soja', reporter='BR')   # exporter/importer cross-validation
```

| Source | Flagship function | Doc |
|--------|-------------------|-----|
| **ANTAQ** ports | `antaq.movimentacao(ano=2024)` | [docs/sources/antaq.md](docs/sources/antaq.md) |
| **ANTT** toll traffic | `alt.antt_pedagio.fluxo_pedagio(ano=2024)`, `alt.antt_pedagio.pracas_pedagio(uf='SP')` | [docs/sources/antt_pedagio.md](docs/sources/antt_pedagio.md) |

### Credit, FX and insurance

BCB — Brazil's central bank (SICOR + SGS + PTAX + Focus), MAPA PSR.

```python
from agrobr import bcb, alt

# BCB SICOR — rural credit
df = await bcb.credito_rural('soja', safra='2024/25')
df = await bcb.credito_rural('soja', safra='2024/25', programa='Pronamp')

# BCB SGS — time series (Selic rate, IPCA inflation, agri PPI, FX, etc.)
df = await bcb.sgs('selic', ultimos=12)
df = await bcb.sgs('ipa_agropecuario', data_inicial='2020-01-01')
df = await bcb.sgs('pib_agropecuaria')                    # also: ipca, igpm, cdi, tjlp, dolar_ptax_venda...

# BCB PTAX — official USD/BRL rate
df = await bcb.ptax(data_inicial='2024-01-01', data_final='2024-12-31')

# BCB Focus — market expectations survey
df = await bcb.focus('PIB Agropecuária')

# MAPA PSR — rural insurance policies and claims
df = await alt.mapa_psr.apolices(cultura='soja', ano=2023)
df = await alt.mapa_psr.sinistros(cultura='soja', uf='MT')
```

### Climate and water

NASA POWER (global climatology), INMET (Brazilian weather stations, requires token), ANA/SNIRH (hydrography, irrigation).

```python
from agrobr import nasa_power, inmet, ana

# NASA POWER — climatology by point or state (no auth)
df = await nasa_power.clima_uf('MT', ano=2024)
df = await nasa_power.clima_ponto(-12.6, -56.1, '2024-01-01', '2024-12-31')

# INMET — observational stations (requires AGROBR_INMET_TOKEN)
df = await inmet.estacao('A001', '2024-01-01', '2024-01-31')
df = await inmet.clima_uf('SP', ano=2024)

# ANA/SNIRH — center-pivot irrigation by state
df = await ana.pivos_irrigacao(uf='MT')
gdf = await ana.pivos_irrigacao_geo(uf='MT')   # requires agrobr[geo]
```

> INMET raises `SourceUnavailableError` (HTTP 403) without a token. Set it with `export AGROBR_INMET_TOKEN=your_token`. For tokenless climate data, use NASA POWER.

### Environmental

Fire hotspots (INPE), deforestation (PRODES + DETER), MapBiomas (land cover/transitions), MapBiomas Alerta, IBAMA (embargoes), ICMBio (protected areas), SFB (public forests).

```python
from agrobr import queimadas, desmatamento, mapbiomas

# Fire hotspots by satellite (6 biomes, 13 satellites)
df = await queimadas.focos(ano=2024, mes=9, uf='MT', bioma='Amazonia')

# Deforestation — PRODES (annual consolidated) + DETER (near-real-time alerts)
df = await desmatamento.prodes(bioma='Cerrado', ano=2022, uf='MT')
df = await desmatamento.deter(
    bioma='Amazônia', uf='PA',
    data_inicio='2024-01-01', data_fim='2024-06-30',
)

# MapBiomas — land use and land cover (1985-present)
df = await mapbiomas.cobertura(estado='MT', ano=2022)
df = await mapbiomas.transicao(estado='PA')

# Geo variants (require agrobr[geo])
gdf = await desmatamento.prodes_geo(bioma='Cerrado', ano=2022, uf='MT')
gdf = await queimadas.focos_geo(ano=2024, mes=9, uf='MT')
```

| Source | Flagship function | Doc |
|--------|-------------------|-----|
| **MapBiomas Alerta** | `mapbiomas_alerta.alertas(start_date='2024-01-01')` (requires `AGROBR_MAPBIOMAS_ALERTA_TOKEN`) | [docs/sources/mapbiomas_alerta.md](docs/sources/mapbiomas_alerta.md) |
| **IBAMA** embargoes | `ibama.embargos(uf='PA')` | [docs/sources/ibama.md](docs/sources/ibama.md) |
| **ICMBio** federal protected areas | `icmbio.ucs(uf='AM', grupo='PI')` | [docs/sources/icmbio.md](docs/sources/icmbio.md) |
| **SFB** public forests | `sfb.cnfp(uf='AM')`, `sfb.concessoes(uf='AM')`, `sfb.ifn_conglomerados(uf='MT')` | [docs/sources/sfb.md](docs/sources/sfb.md) |

### Land registries

SICAR (rural environmental registry), INCRA land registry (SIGEF/SNCI/settlements), FUNAI (indigenous lands), INCRA (quilombola territories), EMBRAPA Soils (PronaSolos + SiBCS).

```python
from agrobr import alt, acervo_fundiario, funai, incra, embrapa_solos

# SICAR — rural environmental registry (rural properties by state)
df = await alt.sicar.imoveis('DF')
df = await alt.sicar.resumo('MT', municipio='Sorriso')

# INCRA land registry — certified parcels and settlements
df = await acervo_fundiario.sigef('MT')
df = await acervo_fundiario.snci('PA')
df = await acervo_fundiario.assentamentos(uf='PA')

# FUNAI — indigenous lands
df = await funai.terras_indigenas(uf='AM', fase='Regularizada')

# INCRA — quilombola territories
df = await incra.quilombolas(uf='BA')

# EMBRAPA Soils — PronaSolos soil profiles + SiBCS soil map
df = await embrapa_solos.perfis(uf='SP')
df = await embrapa_solos.mapa_solos(ordem='LATOSSOLO')

# Geo variants (require agrobr[geo])
gdf = await alt.sicar.imoveis_geo('DF')
gdf = await funai.terras_indigenas_geo(uf='AM')
gdf = await acervo_fundiario.sigef_geo('MT')
gdf = await embrapa_solos.mapa_solos_geo(ordem='LATOSSOLO')
```

### Inputs and regulatory

ANDA (fertilizers), Agrofit (registered pesticides), RNC (plant cultivars), Lista Suja (slave labor registry), ZARC (climate risk zoning).

```python
from agrobr import anda, defensivos, rnc, lista_suja, zarc

# ANDA — fertilizer deliveries (requires agrobr[pdf])
df = await anda.entregas(ano=2024, uf='MT')

# Agrofit — pesticides registered in Brazil
df = await defensivos.formulados(ingrediente_ativo='glifosato')
df = await defensivos.tecnicos(titular='Bayer')
df = await defensivos.autorizacoes(cultura='soja')

# RNC/CultivarWeb — registered (~37K) and protected (~5K) cultivars
df = await rnc.registradas(especie='Soja')
df = await rnc.protegidas(titular='Embrapa')

# Lista Suja — employers caught using slave-like labor
df = await lista_suja.empregadores(uf='PA')

# ZARC — agricultural climate risk zoning (planting windows)
df = await zarc.zoneamento(cultura='soja', uf='MT')
print(zarc.culturas())   # 40+ crops available
```

## Semantic layer — datasets

When you want the data and don't care about the source, use `datasets`. Each dataset orchestrates an automatic fallback chain and returns tracked provenance.

```python
from agrobr import datasets

# Daily price (CEPEA → fallback)
df = await datasets.preco_diario('soja')

# Annual production (IBGE PAM → CONAB)
df = await datasets.producao_anual('soja', ano=2023)

# Current season estimate (CONAB → IBGE LSPA)
df = await datasets.estimativa_safra('soja', safra='2024/25')

# Rural credit (BCB SICOR → BigQuery via basedosdados)
df = await datasets.credito_rural('soja', safra='2024/25')

# Climate (INMET → NASA POWER)
df = await datasets.clima(uf='SP', ano=2024)

# With provenance: meta includes which source was used and which were attempted
df, meta = await datasets.preco_diario('soja', return_meta=True)
print(meta.selected_source, meta.attempted_sources, meta.contract_version)

# List all available datasets
print(datasets.list_datasets())
```

36 datasets available. See the [full list](#available-datasets) below.

## Reproducibility — snapshots and deterministic mode

Snapshots capture local data as parquet for full reproducibility — ideal for papers, audits and CI pipelines.

```python
from agrobr import datasets
from agrobr.snapshots import create_snapshot, list_snapshots, delete_snapshot

# Create a snapshot (saves current data under ~/.agrobr/snapshots/)
info = await create_snapshot("2025-Q4")
info = await create_snapshot(sources=["cepea", "conab"])

# List and remove
for s in list_snapshots():
    print(s.name, s.file_count, f"{s.size_bytes/1024/1024:.1f} MB")
delete_snapshot("2025-Q4")

# Deterministic mode — queries read only from the active snapshot, no network
async with datasets.deterministic("2025-12-31"):
    df = await datasets.preco_diario("soja")
```

Via CLI:

```bash
agrobr snapshot create 2025-Q4 --sources cepea,conab,ibge
agrobr snapshot list
agrobr snapshot use 2025-Q4   # validates the snapshot and shows how to activate it in code
agrobr snapshot delete 2025-Q4
```

## Sync mode

```python
from agrobr.sync import cepea, conab, ibge, datasets, alt

# Same API, no async/await
df = cepea.indicador('soja', inicio='2024-01-01')
df = conab.safras('soja', safra='2024/25')
df = ibge.pam('soja', ano=2023)
df = datasets.preco_diario('soja')
df = alt.sicar.imoveis('DF')
```

Every top-level source plus `alt` is available under `agrobr.sync` with the same signatures.

## Polars support

```python
df = await cepea.indicador('soja', as_polars=True)
df = await datasets.preco_diario('soja', as_polars=True)
df = await ibge.pam('soja', ano=2023, as_polars=True)
```

Supported across all source APIs and datasets.

## CLI

```bash
# Sources
agrobr cepea indicador soja --ultimo
agrobr cepea indicador milho --inicio 2024-01-01 --formato csv
agrobr conab safras soja --safra 2024/25
agrobr conab balanco milho
agrobr ibge pam soja --ano 2023 --nivel uf
agrobr ibge lspa milho --ano 2024 --mes 6

# Diagnostics and configuration
agrobr health
agrobr health --source cepea --deep
agrobr doctor --verbose
agrobr config show

# Snapshots
agrobr snapshot create 2025-Q4 --sources cepea,conab,ibge
agrobr snapshot list
agrobr snapshot use 2025-Q4   # validates the snapshot and shows how to activate it in code
```

## Available datasets

| Dataset | Description | Sources |
|---------|-------------|---------|
| `abate_trimestral` | Cattle, hog and poultry slaughter by state | IBGE |
| `balanco` | Supply/demand balance | CONAB |
| `cadastro_rural` | Rural environmental registry (properties by state) | SICAR/GeoServer WFS |
| `censo_agropecuario` | Agricultural census 1995/2006/2017 (10 themes) | IBGE |
| `censo_agropecuario_historico` | Census historical series 1920-2006 (9 themes) | IBGE SIDRA |
| `censo_agropecuario_legado` | 1995/96 census — 6 legacy themes (FTP) | IBGE FTP |
| `censo_agropecuario_municipal_1985` | 1985 municipal census — 53 themes via OCR (22 states) | IBGE PDFs |
| `clima` | Monthly/daily climate data by state or station | INMET → NASA POWER |
| `comercio_internacional` | Bilateral international trade by HS code | UN Comtrade |
| `condicao_lavouras` | Weekly crop conditions in Paraná | DERAL |
| `credito_rural` | Rural credit by crop (program, insurance, modality) | BCB/SICOR → BigQuery |
| `custo_producao` | Production costs | CONAB |
| `desmatamento` | PRODES/DETER deforestation — consolidated + alerts | INPE TerraBrasilis |
| `embarques_anec` | Weekly shipments by port (soybean, meal, corn, DDGS, sorghum, wheat) | ANEC |
| `estimativa_safra` | Current season estimates | CONAB → IBGE LSPA |
| `exportacao` | Agricultural exports | ComexStat → ABIOVE |
| `extrativismo_vegetal` | Non-timber forest products (açaí, Brazil nut, yerba mate) | IBGE PEVS |
| `fertilizante` | Fertilizer deliveries | ANDA |
| `futuros_agricolas` | B3 agricultural futures (settlements, history, open interest) | B3 |
| `importacao` | Agricultural imports | ComexStat |
| `leite_industrial` | Quarterly milk intake and processing by state | IBGE |
| `movimentacao_portuaria` | Port cargo throughput (bulk, general, container) | ANTAQ |
| `oferta_demanda_global` | Global commodity supply/demand | USDA PSD |
| `pecuaria_municipal` | Municipal livestock (herds and animal products) | IBGE PPM |
| `posicionamento_fundos` | Weekly trader positioning in Chicago/NY agri futures (COT) | CFTC |
| `pib_agro` | Agricultural GDP by sector and quarter | IBGE SIDRA |
| `preco_atacado` | Wholesale produce prices at CEASA hubs | CONAB CEASA/PROHORT |
| `preco_diario` | Daily spot prices | CEPEA → cache |
| `producao_anual` | Consolidated annual production | IBGE PAM → CONAB |
| `progresso_safra` | Weekly planting/harvest progress | CONAB |
| `queimadas` | Satellite fire hotspots (6 biomes) | INPE |
| `seguro_rural` | Rural insurance policies and claims | MAPA PSR |
| `serie_historica_safra` | Crop season historical series — 32 crops since 1976 | CONAB |
| `silvicultura` | Forestry production (eucalyptus, pine, charcoal) | IBGE PEVS |
| `uso_do_solo` | Annual land use and cover by state/municipality | MapBiomas |
| `zoneamento_agricola` | Agricultural climate risk zoning (ZARC) | MAPA/Embrapa |

## Supported sources

Availability is monitored automatically. Run `agrobr health` to check locally (or `agrobr health --source <name> --deep` for a source-specific check with parsing).

| Source | Data | Golden Test | Status |
|--------|------|:-----------:|--------|
| CEPEA | Price indicators (21 products) | ✅ | Working |
| CONAB | Crop surveys, supply/demand, costs, historical series, weekly progress, CEASA wholesale prices | ✅ | Working |
| IBGE | PAM, LSPA, PPM, slaughter, PEVS, milk, GDP, agricultural census (1985/1995-96/2006/2017 + historical series) | ✅ | Working |
| NASA POWER | Daily/monthly climatology (0.5° grid) | ✅ | Working |
| BCB/SICOR | Rural credit by crop + SGS series + PTAX + Focus | ✅¹ | Working |
| ComexStat | Exports and imports by NCM code/state | ✅¹ | Working |
| ANDA | Fertilizer deliveries | ✅ | Working |
| ABIOVE | Soybean complex exports (volume/revenue) | ✅ | Working |
| ANEC | Weekly shipments by port (soybean, meal, corn, DDGS, sorghum, wheat) | ✅ | Working |
| USDA PSD | International estimates (production/supply/demand) | ✅¹ | Working |
| IMEA | Mato Grosso quotes and indicators | ✅ | Working |
| DERAL | Paraná crop conditions | ✅ | Working |
| INMET | Weather (600+ stations) | ✅¹ | Requires `AGROBR_INMET_TOKEN` |
| Notícias Agrícolas | Quotes (CEPEA fallback, internal use) | ✅¹ | Working |
| Fire hotspots/INPE | Satellite fire detection (6 biomes, 13 satellites) | ✅ | Working |
| Deforestation PRODES/DETER | Consolidated deforestation + alerts (TerraBrasilis WFS) | ✅ | Working |
| MapBiomas | Land use and cover (1985-present), state and municipality level | ✅ | Working |
| B3 Agri Futures | Daily settlements + open interest (7 agri contracts) | ✅ | Working |
| UN Comtrade | Bilateral trade + trade mirror (~200 countries, HS codes) | ✅¹ | Working |
| ANTAQ | Port cargo throughput (bulk, general, container) | ⚠️ | Source offline since 2026-06-23 |
| ANP Diesel | Retail prices + diesel volumes by state/municipality | ✅ | Working |
| MAPA PSR | Rural insurance policies and claims (2006+, 27 states) | ✅ | Working |
| ANTT | Toll plaza vehicle traffic (2010+, 200+ plazas) | ✅ | Working |
| SICAR | Rural environmental registry — properties by state (7.4M+ records, WFS) | ✅ | Working |
| ZARC | Climate risk zoning (planting windows by municipality/crop/soil) | ✅ | Working |
| Agrofit/MAPA | Registered pesticides — formulations, authorizations, technical products (~8K) | ✅ | Working |
| MapBiomas Alerta | Deforestation alerts via GraphQL (500K+ alerts) | ✅ | Requires `AGROBR_MAPBIOMAS_ALERTA_TOKEN` |
| Lista Suja | Slave-labor employer registry via XLSX | ✅ | Working |
| ANA/SNIRH | Hydrography, center-pivot irrigation, water demand/availability (ArcGIS REST) | ✅ | Working |
| SFB | Public forests (CNFP), forest concessions, national forest inventory (ArcGIS REST) | ✅ | Working |
| FUNAI | Indigenous lands (WFS) — ~740 territories, state/phase/bbox filters | ✅ | Working |
| IBAMA | Environmental embargoes (SIFISC CSV dump + WKT geometries) — ~114K records, state/bbox filter | ✅ | Working |
| ICMBio | Federal protected areas (WFS) — 344 units | ✅ | Working |
| INCRA | Quilombola territories (WFS) — ~426 territories | ✅ | Working |
| INCRA land registry | SIGEF certified parcels (15 states) + SNCI (10 states) + settlements — shapefile ZIP | ✅ | Working |
| RNC/CultivarWeb | Registered (~37K) and protected (~5K) cultivars — MAPA/SNPC | ✅ | Working |
| EMBRAPA Soils | PronaSolos soil profiles (34K+) + SiBCS soil map (2.8K polygons) | ✅ | Working |
| Fundação Rio Verde | Soybean cultivar trials in MT — ~97 cultivars × 4 planting dates (PDF) | ✅ | Working |
| CFTC COT | Weekly trader positioning — managed money, producers, swaps (12 agri contracts, 2006+) | ✅ | Working |
| UNICA | Center-South sugarcane crush, sugar/ethanol production, mix and ATR (biweekly PDF + historical XLSX) | ✅ | Working |

> ¹ Golden test with synthetic data — `needs_real_data` for validation against the live API.
>
> Several sources have restrictive or gray-area licenses — CEPEA `nc`, IMEA `restrito`, INCRA land registry `nc`, B3/ABIOVE/ANDA/ANEC/UNICA/Notícias Agrícolas `zona_cinza`. They emit `warnings.warn` on first call. See [docs/licenses.md](docs/licenses.md) for the full table.

## Contracts & Schemas

Every dataset has a formal contract with automatic validation. JSON schemas are generated under `agrobr/schemas/`:

```python
from agrobr.contracts import get_contract, list_contracts, validate_dataset

# List registered contracts
list_contracts()

# Inspect a contract
contract = get_contract("preco_diario")
print(contract.primary_key)   # ['data', 'produto']
print(contract.to_json())     # full JSON schema

# Explicit validation (automatic on every fetch)
validate_dataset(df, "preco_diario")  # raises ContractViolationError
```

Global guarantees: stable column names (additions only), types only widen (int→float ok, float→int never), ISO-8601 dates, breaking changes only on major versions. See [docs/contracts/](https://www.agrobr.dev/docs/contracts/) for per-dataset details.

## Cross-source normalization

Helpers to standardize data across sources:

```python
from agrobr.normalize import (
    normalizar_cultura, municipio_para_ibge, coordenada_para_municipio,
    normalizar_uf, normalizar_safra,
)

normalizar_cultura("Soja em Grão")        # "soja"
normalizar_cultura("milho 2ª safra")      # "milho_2"
normalizar_cultura("coffee")              # "cafe"

municipio_para_ibge("Sorriso", "MT")      # 5107925
municipio_para_ibge("SAO PAULO", "SP")    # 3550308

coordenada_para_municipio(-12.74, -55.68)
# {'codigo_ibge': 5107925, 'nome': 'Sorriso', 'uf': 'MT'}

normalizar_uf("São Paulo")                # "SP"
normalizar_safra("24/25")                 # "2024/25"
```

5,571 IBGE municipalities with centroids (offline reverse geocoding), 35 canonical crops, 27 states. Data from the IBGE Localities and Meshes APIs (free to use).

## Highlights

- **Golden tests with reference fixtures per source** — automated validation against real or documented synthetic data
- **Full HTTP resilience** — centralized retry in every client, 429 handling, Retry-After
- **6,400+ passing tests, 92% coverage** — including scalability benchmarks (memory, volume, async)
- **Semantic layer** — standardized datasets with automatic fallback and tracked provenance
- **Formal contracts** — versioned schemas with automatic validation, primary keys and constraints
- **JSON schemas** exported under `agrobr/schemas/`
- **Deterministic mode + snapshots** — full reproducibility for papers/audits
- **Cross-source normalization** — IBGE municipalities, crops, states, crop seasons standardized
- **Async-first** with a sync wrapper for pipelines (Airflow, Prefect, Dagster)
- **pandas + polars support** in every API and dataset
- **Validation** — Pydantic v2 + statistical sanity checks + layout fingerprinting
- **CEPEA cache with smart TTL** (local DuckDB, expires at 6pm official CEPEA time)
- **Multi-channel alerts** (Slack, Discord, Email)
- **Full CLI** for debugging and automation

## How it works

agrobr is a **fetch + normalize** library, not a storage framework. Every call goes to the source (with retry, layout fingerprinting and contract validation). There is no automatic history accumulation:

- **CEPEA indicator cache** — the only source with a persistent local cache. DuckDB with smart TTL: expires at 6pm (official CEPEA publish time), avoiding redundant calls during the day.
- **Optional snapshots** — you create them explicitly via `create_snapshot()` for reproducibility.
- **Permanent history is the consumer's responsibility** — sources that serve historical series (CONAB `serie_historica`, IBGE PAM/PPM, BCB SGS, etc.) already return the full range in a single request. To accumulate daily sources, use a scheduler + parquet (next section).

## Keeping data up to date

To accumulate history or run scheduled collection, integrate with Airflow, Prefect or Dagster using the sync API:

```python
from datetime import date

# Airflow task
@task
def extract_daily_soybean():
    from agrobr.sync import datasets
    df = datasets.preco_diario("soja")
    df.to_parquet(f"/data/soja/{date.today()}.parquet")
```

See the [pipelines guide](https://www.agrobr.dev/docs/advanced/pipelines/) and the [async ergonomics guide](https://www.agrobr.dev/docs/guides/async/).

## Documentation

[Full documentation](https://www.agrobr.dev/docs/) (in Portuguese; English version planned)

- [Quickstart](https://www.agrobr.dev/docs/quickstart/)
- [Datasets](https://www.agrobr.dev/docs/contracts/) — contracts and guarantees
- [Sources](https://www.agrobr.dev/docs/sources/) — all 40 sources documented
- [API Reference](https://www.agrobr.dev/docs/api/cepea/)
- [Resilience](https://www.agrobr.dev/docs/advanced/resilience/)
- [Porting](https://www.agrobr.dev/docs/porting/) — guide for porting agrobr to R, Julia or other languages

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## Data licenses

> **Important:** agrobr itself is MIT-licensed, but the **data** it accesses
> belongs to the respective sources and carries their own licenses.
> CEPEA/ESALQ data, for example, is CC BY-NC 4.0 (commercial use requires
> authorization). See **[docs/licenses.md](docs/licenses.md)** for the full
> table of sources, licenses and classifications.

## License

MIT — see [LICENSE](LICENSE) for details.
