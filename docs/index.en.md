# agrobr

**Brazilian agricultural data in one line of code**

[![PyPI version](https://badge.fury.io/py/agrobr.svg)](https://pypi.org/project/agrobr/)
[![Tests](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml)
[![Health Check](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## What is agrobr?

Python infrastructure for Brazilian agricultural data with a **semantic layer** over 40 public sources.

**v1.0.5** — 6000+ tests | 92% coverage | golden tests with per-source fixtures | centralized retry across all HTTP clients

- **CEPEA/ESALQ**: 21 price indicators (soybean, corn, live cattle, arabica coffee, robusta coffee, cotton, wheat, rice, sugar, ethanol, poultry, hog, milk, orange)
- **CONAB**: Crop surveys, supply/demand balance, production costs, historical series, weekly planting/harvest progress, and wholesale produce prices (CEASA/PROHORT)
- **IBGE/SIDRA**: PAM (annual), LSPA (monthly), PPM, Slaughter, PEVS (silviculture + plant extraction), Quarterly Milk, Agricultural GDP, Agricultural Census
- **NASA POWER**: Daily gridded climatology (temperature, precipitation, radiation, humidity, wind)
- **BCB/SICOR**: Rural credit by crop and state + SGS time series (Selic, IPCA, agri GDP) + PTAX exchange rate + Focus expectations
- **ComexStat**: Agricultural exports by NCM code
- **ANDA**: Fertilizer deliveries by state
- **ABIOVE**: Soy complex exports (monthly volume and revenue)
- **USDA PSD**: International production/supply/demand estimates
- **IMEA**: Mato Grosso quotes and indicators (6 production chains)
- **DERAL**: Paraná crop conditions (weekly)
- **INMET**: Weather data by station (requires `AGROBR_INMET_TOKEN`)
- **Notícias Agrícolas**: Agricultural quotes (CEPEA fallback)
- **Queimadas/INPE**: Satellite fire hotspots (6 biomes, 13 satellites)
- **Deforestation PRODES/DETER**: Consolidated deforestation + real-time alerts + geometry (GeoDataFrame)
- **MapBiomas**: Land cover and use by municipality (1985-present)
- **B3 Agri Futures**: Daily settlements + open interest for agri futures and options
- **UN Comtrade**: Bilateral trade + trade mirror (exports vs imports by HS code, ~200 countries)
- **ANTAQ**: Port cargo movement (solid/liquid bulk, general cargo, containers, 2010+)
- **ANP Diesel**: Diesel resale prices and sales volumes by state/municipality (mechanized-activity proxy)
- **ANTT Tolls**: Vehicle flow at highway toll plazas (ANTT Open Data, CC-BY, 2010+)
- **MAPA PSR**: Federally subsidized rural insurance policies and claims (SISSER/MAPA, 2006+)
- **SICAR**: Rural Environmental Registry — rural property records by state via WFS (7.4M+ properties, 27 states)
- **ZARC**: Agricultural Climate Risk Zoning — planting windows by municipality/crop/soil/cycle (MAPA/Embrapa, CC-BY)
- **Agrofit/MAPA (Pesticides)**: Pesticides registered in Brazil — formulated products, use authorizations and technical products (CC-BY)
- **FUNAI**: Indigenous lands via WFS (~740 territories, CC BY-ND 3.0)
- **ICMBio**: Federal conservation units via WFS (344 units)
- **INCRA**: Quilombola territories via WFS (~426 territories)
- **IBAMA**: Environmental embargoes via WFS (~89K embargoes, ODbL)
- **MapBiomas Alerta**: Deforestation alerts via GraphQL (citation required)
- **Lista Suja**: Employer "dirty list" (slave labor) via PDF (Freedom of Information Act)
- **ANA/SNIRH**: Hydrography, irrigation pivots, water demand and availability via ArcGIS REST
- **SFB**: Public forests, forest concessions and IFN via ArcGIS REST
- **RNC/CultivarWeb**: National Cultivar Registry — ~37K registered + ~5K protected (MAPA, public data)
- **EMBRAPA Solos**: PronaSolos soil profiles (34K+ points) + SiBCS pedological map (2.8K polygons) via WFS (CC BY-NC 3.0 BR)
- **Fundação Rio Verde**: Soybean cultivar trials — ~97 cultivars x 4 sowing windows (PDF, pdfplumber)

## Datasets — Semantic Layer

Ask for what you want; the source is an internal detail:

| Dataset | Description | Sources (automatic fallback) |
|---------|-------------|------------------------------|
| `abate_trimestral` | Slaughter of cattle, hogs and poultry by state | IBGE Slaughter |
| `balanco` | Supply/demand balance | CONAB |
| `cadastro_rural` | Rural Environmental Registry (rural properties) | SICAR/GeoServer WFS |
| `censo_agropecuario` | Agricultural Census 1995/2006/2017 (10 themes) | IBGE Agri Census |
| `censo_agropecuario_historico` | Agricultural Census historical series 1920-2006 (9 themes) | IBGE SIDRA |
| `censo_agropecuario_legado` | Agricultural Census 1995/96 — 6 legacy themes | IBGE FTP |
| `censo_agropecuario_municipal_1985` | 1985 municipal census — 53 themes via OCR (22 states) | IBGE PDFs |
| `clima` | Monthly/daily climate data by state or station | INMET → NASA POWER |
| `comercio_internacional` | Bilateral international trade by HS code | UN Comtrade |
| `condicao_lavouras` | Weekly Paraná crop conditions | DERAL |
| `credito_rural` | Rural credit by crop (program, insurance, modality) | BCB/SICOR → BigQuery |
| `custo_producao` | Production costs | CONAB |
| `desmatamento` | PRODES/DETER deforestation — consolidated + alerts | INPE TerraBrasilis |
| `estimativa_safra` | Current-season estimates | CONAB → IBGE LSPA |
| `exportacao` | Agricultural exports | ComexStat → ABIOVE |
| `extrativismo_vegetal` | Plant extraction output (açaí, brazil nut, yerba mate) | IBGE PEVS |
| `fertilizante` | Fertilizer deliveries | ANDA |
| `futuros_agricolas` | B3 agri futures (settlements, history, positions) | B3 |
| `importacao` | Agricultural imports | ComexStat |
| `leite_industrial` | Quarterly milk acquisition and processing by state | IBGE Milk |
| `movimentacao_portuaria` | Port cargo movement (bulk, general, container) | ANTAQ |
| `oferta_demanda_global` | Global commodity supply/demand (USDA PSD) | USDA |
| `pecuaria_municipal` | Herds and animal production | IBGE PPM |
| `pib_agro` | Agricultural GDP by sector and quarter | IBGE SIDRA |
| `preco_atacado` | Wholesale produce prices at CEASAs | CONAB CEASA/PROHORT |
| `preco_diario` | Daily spot prices | CEPEA → Notícias Agrícolas → cache |
| `producao_anual` | Consolidated annual output | IBGE PAM → CONAB |
| `progresso_safra` | Weekly sowing/harvest progress | CONAB |
| `queimadas` | Satellite fire hotspots (6 biomes) | INPE |
| `seguro_rural` | Rural insurance policies and claims | MAPA PSR |
| `serie_historica_safra` | Crop historical series — 32 crops since 1976 | CONAB |
| `silvicultura` | Silvicultural output (eucalyptus, pine, charcoal) | IBGE PEVS |
| `uso_do_solo` | Annual land cover and use by state/municipality | MapBiomas |
| `zoneamento_agricola` | Agricultural climate risk zoning (ZARC) | MAPA/Embrapa |

```python
from agrobr import datasets

df = await datasets.preco_diario("soja")
df = await datasets.producao_anual("soja", ano=2023)
df = await datasets.estimativa_safra("soja", safra="2024/25")
df = await datasets.balanco("soja")
```

## Installation

```bash
pip install agrobr

# With Playwright (for sources that require JavaScript)
pip install agrobr[browser]
playwright install chromium
```

## Quick Start

```python
from agrobr import cepea, conab, ibge, nasa_power

# CEPEA - price indicators
df = await cepea.indicador('soja', inicio='2024-01-01')

# CONAB - crop surveys
df = await conab.safras('soja', safra='2024/25')

# IBGE - PAM
df = await ibge.pam('soja', ano=2023, nivel='uf')

# NASA POWER - climate
df = await nasa_power.clima_uf('MT', ano=2025)
```

### Synchronous Version

```python
from agrobr.sync import cepea, nasa_power

df = cepea.indicador('soja')
df = nasa_power.clima_uf('MT', ano=2025)
```

## Why agrobr

| Problem | agrobr solution |
|---------|-----------------|
| Manual spreadsheet downloads | A single line of code |
| Inconsistent layouts | Robust parsing with fallback |
| Scripts that break | Fingerprinting detects changes |
| No history | DuckDB cache with accumulation |
| Chaotic encoding | Automatic fallback chain |
| Picking a source | Datasets abstract the source |

## Quality & Reliability

| Metric | Value |
|--------|-------|
| Tests | 6000+ passing |
| Coverage | 92% |
| Golden tests | per-source fixtures (real or synthetic data) |
| HTTP resilience | Centralized retry + 429/Retry-After |
| Benchmarks | Memory, volume, cache, async, rate limiting |

## Features

- **40 public sources** — CEPEA, CONAB, IBGE, NASA POWER, BCB/SICOR, ComexStat, ANDA, ABIOVE, ANEC, USDA, IMEA, DERAL, INMET, Notícias Agrícolas, Queimadas/INPE, Deforestation, MapBiomas, B3 Agri Futures, UN Comtrade, ANTAQ, ANP Diesel, MAPA PSR, ANTT Tolls, SICAR, ZARC, Agrofit/MAPA (Pesticides), FUNAI, ICMBio, INCRA, IBAMA, MapBiomas Alerta, Lista Suja, ANA/SNIRH, SFB, RNC/CultivarWeb, EMBRAPA Solos, Fundação Rio Verde, Acervo Fundiário/INCRA, CFTC COT, UNICA
- **Golden tests** — per-source reference fixtures (real or documented synthetic data)
- **HTTP resilience** — centralized `retry_on_status()`/`retry_async()`, Retry-After, 429 handling
- **Semantic layer** — datasets with automatic fallback across sources
- **Public contracts** — versioned schema with stability guarantees
- **Deterministic mode + snapshots** — full reproducibility for papers/audits ([guide](guides/snapshots.md))
- **Async-first** with a sync wrapper for simple use
- **DuckDB cache** with permanent history
- **pandas + polars support** (`as_polars=True`)
- **Full CLI** (`agrobr cepea indicador soja --formato csv`)
- **Validation** — Pydantic v2 + statistical sanity checks + fingerprinting
- **Monitoring** — daily health checks + Discord/Slack alerts

## Next Steps

- [Quick Start](quickstart.md) — Full tutorial
- [Datasets](contracts/index.md) — Contracts and guarantees
- [API Reference](api/cepea.md) — Detailed documentation
- [Sources](sources/index.md) — Provenance and traceability
- [Examples](https://github.com/bruno-portfolio/agrobr/tree/main/examples) — Example scripts
- [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/bruno-portfolio/agrobr/blob/main/examples/agrobr_demo.ipynb) — Interactive notebook with every source

## License

MIT License — see [LICENSE](https://github.com/bruno-portfolio/agrobr/blob/main/LICENSE)
