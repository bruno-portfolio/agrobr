# Data Sources

agrobr integrates data from 41 agricultural data sources.
Source data functions that return a table accept `return_meta=True` (`cepea.ultimo`, which returns an `Indicador`, does not); Notícias Agrícolas, the CEPEA fallback, carries its provenance in `cepea.indicador(..., return_meta=True)`.
What has a SemVer guarantee in each source is in the [public API](../api/index.md).

## Overview

| Source | Type | Update | Coverage |
|--------|------|--------|----------|
| [CEPEA/ESALQ](cepea.md) | Prices | Daily | Agricultural commodities |
| [CONAB](conab.md) | Crops, costs, historical series (crops and sugarcane industry), [weekly progress](conab_progresso.md), [CEASA/PROHORT](conab_ceasa.md) | Monthly/Weekly/Daily | National production |
| [IBGE/SIDRA](ibge.md) | Statistics | Annual/Monthly/Quarterly | Official data (PAM, LSPA, PPM, Slaughter, PEVS, Milk, GDP, Census), municipal mesh and urbanized areas (WFS geo) |
| [NASA POWER](nasa_power.md) | Daily and monthly weather | Daily | Global, 0.5 degree grid |
| [BCB](bcb.en.md) | Rural credit, time series, exchange-rate bulletins, and forecasts | Varies by service | Crop/state, series, currency/bulletin, or indicator |
| [ComexStat](comexstat.md) | Exports | Weekly | NCM/state |
| [ANDA](anda.md) | Fertilizers | Monthly | State/month |
| [ABIOVE](abiove.md) | Soybean complex exports | Monthly | Volume/revenue |
| [ANEC](anec.md) | Weekly shipments by port | Weekly | 19 ports, 6 products |
| [USDA PSD](usda.md) | International supply/demand | Monthly | Global commodities |
| [IMEA](imea.md) | MT quotes and indicators | Daily | Mato Grosso |
| [DERAL](deral.md) | PR crop condition | Weekly | Paraná |
| [INMET](inmet.md) | Meteorology | Daily | 600+ stations (observational API requires a token; historical ZIPs are public) |
| [Notícias Agrícolas](noticias_agricolas.md) | Quotes (CEPEA fallback) | Daily | Commodities |
| [Queimadas/INPE](queimadas.md) | Fire hotspots | Daily | 6 biomes, 13 satellites |
| [Deforestation PRODES/DETER](desmatamento.md) | Deforestation + alerts | Annual/Daily | Amazônia, Cerrado, Pantanal |
| [MapBiomas](mapbiomas.md) | Land cover and use | Annual | Municipalities (1985-present) |
| [B3 Agricultural Futures](b3.md) | Daily settlements + open interest | Daily | 7 agricultural contracts |
| [UN Comtrade](comtrade.md) | Bilateral 2.1 and mirror 2.0, counts and partitions | Monthly/Annual | HS, World or published partners |
| [ANTAQ](antaq.md) | Port cargo movement | Annual | ⚠️ Source offline since 2026-06-23 |
| [ANP Diesel](anp_diesel.md) | Resale prices + diesel volumes | Weekly/Monthly | States, municipalities, 2013+ |
| [ANTT Pedagio](antt_pedagio.md) | Vehicle traffic at toll plazas | Monthly | 200+ plazas, 2010+ |
| [MAPA PSR](mapa_psr.md) | Rural insurance policies and claims | Annual | 27 states, 2006+ |
| [SICAR](sicar.md) | Rural Environmental Registry (CAR) | Continuous | 27 states, 7.4M+ properties |
| [ZARC](zarc.md) | Agricultural Climate Risk Zoning | Weekly | 107 crops in the catalogue; municipalities of each publication |
| [Agrofit/MAPA](defensivos.md) | Registered pesticides | Continuous | ~8K formulated products, ~267K authorizations |
| [FUNAI Indigenous Lands](funai.md) | Indigenous lands (WFS geo) | Continuous | 665 territories, all states |
| [ICMBio Federal Conservation Units](icmbio.md) | Federal conservation units (WFS geo) | Continuous | 347 federal units, without private reserves |
| [CNUC Conservation Units](cnuc.md) | Federal, state and municipal units, including private reserves (WFS geo) | Continuous | 3,450 units |
| [INCRA Quilombola Territories](incra.md) | Quilombola perimeters (WFS geo), process progress (PDF) and NUP links | Continuous | 445 perimeters and 649 processes (2026-09-22) |
| [Land Registry/INCRA](acervo_fundiario.md) | Certified parcels + settlements (shapefile ZIP) | Continuous | SIGEF (27 states, public and private) + SNCI (24 states on 2026-10-01) + settlements Brazil |
| [IBAMA Environmental Embargoes](ibama.md) | Environmental embargoes (open-data CSV + WKT) | Daily | ~116K terms |
| [MapBiomas Alerta](mapbiomas_alerta.md) | Deforestation alerts (GraphQL) | Weekly | National |
| [Lista Suja](lista_suja.md) | MTE registry (CSV/TXT; PDF alternative) | Periodic publication with interim updates | National |
| [ANA/SNIRH](ana.md) | Hydrography, irrigation, water availability, water bodies (ArcGIS REST) | Variable | National |
| [SFB](sfb.md) | Public forests, concessions, IFN (ArcGIS REST) | Annual | National |
| [RNC/CultivarWeb](rnc.md) | Registered/protected cultivars | Continuous | ~37K registered, ~5K protected |
| [EMBRAPA Solos](embrapa_solos.md) | Soil profiles and soil map | Continuous | 34K profiles, 2.8K polygons |
| [Fundação Rio Verde](rio_verde.md) | MT soybean cultivar trials | Annual | 3 seasons (2023/24 to 2025/26), up to 4 sowing windows |
| [CFTC COT](cftc.md) | Fund positioning in agricultural futures | Weekly | 12 Chicago/NY contracts, 2006+ |
| [UNICA](unica.md) | Center-South sugar/ethanol crushing and production | Biweekly | Current crop year + history 1980-2021 |

## Provenance and Traceability

All information returned by agrobr can be traced back to its origin.
Use the `return_meta=True` parameter to obtain full provenance metadata.

```python
import asyncio
from agrobr import cepea

async def main():
    # Basic usage (unchanged)
    df = await cepea.indicador('soja')

    # With provenance metadata
    df, meta = await cepea.indicador('soja', return_meta=True)

    print(f"Source: {meta.source}")
    print(f"URL: {meta.source_url}")
    print(f"Fetched at: {meta.fetched_at}")
    print(f"From cache: {meta.from_cache}")
    print(f"Records: {meta.records_count}")

asyncio.run(main())
```

## MetaInfo Structure

The `MetaInfo` object includes, among others, the fields below; the full list, with `validation_warnings`, `source_details`, `schema_version`, `attempted_sources`, `selected_source` and `fetch_timestamp`, is in [Contracts](../contracts/index.md#metainfo). `cache_key` and `cache_expires_at` may be null; when filled, they do not mean the data came from the cache: use `from_cache` for that.

| Field | Type | Description |
|-------|------|-------------|
| `source` | str | Source name (cepea, conab, ibge) |
| `source_url` | str | Exact URL accessed |
| `source_method` | str | Access method (httpx, cache) |
| `fetched_at` | datetime | Collection timestamp |
| `from_cache` | bool | Whether it came from the local cache |
| `cache_key` | str \| None | Cache key |
| `cache_expires_at` | datetime \| None | When the cache expires |
| `records_count` | int | Number of records |
| `columns` | list | Returned columns |
| `fetch_duration_ms` | int | Fetch time in ms |
| `parse_duration_ms` | int | Parsing time in ms |
| `agrobr_version` | str | agrobr version |
| `parser_version` | int | Parser version used |

## Export for auditing

`MetaInfo` exports the metadata; `raw_content_hash` is the SHA-256 of the body received from the source (see [Contracts](../contracts/index.md#metainfo)):

```python
meta_json = meta.to_json()
meta_dict = meta.to_dict()
```

## Diagnostics

Use the `doctor` command to check system health:

```bash
agrobr doctor
```

Returns:
- Source connectivity status
- Cache statistics
- Latest collections
- Current configuration
