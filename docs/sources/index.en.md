# Data Sources

agrobr integrates data from 40 agricultural data sources.
All sources support `return_meta=True` for full traceability.

## Overview

| Source | Type | Update | Coverage |
|--------|------|--------|----------|
| [CEPEA/ESALQ](cepea.md) | Prices | Daily | Agricultural commodities |
| [CONAB](conab.md) | Crops, costs, historical series, [weekly progress](conab_progresso.md), [CEASA/PROHORT](conab_ceasa.md) | Monthly/Weekly/Daily | National production |
| [IBGE/SIDRA](ibge.md) | Statistics | Annual/Monthly/Quarterly | Official data (PAM, LSPA, PPM, Slaughter, PEVS, Milk, GDP, Census) |
| [NASA POWER](nasa_power.md) | Climatology | Daily | Global, 0.5 degree grid |
| [BCB/SICOR](bcb.md) | Rural credit, time series, exchange rates, forecasts | Monthly | Crop/state (+ BigQuery) |
| [ComexStat](comexstat.md) | Exports | Weekly | NCM/state |
| [ANDA](anda.md) | Fertilizers | Monthly | State/month |
| [ABIOVE](abiove.md) | Soybean complex exports | Monthly | Volume/revenue |
| [ANEC](anec.md) | Weekly shipments by port | Weekly | 19 ports, 6 products |
| [USDA PSD](usda.md) | International supply/demand | Monthly | Global commodities |
| [IMEA](imea.md) | MT quotes and indicators | Daily | Mato Grosso |
| [DERAL](deral.md) | PR crop condition | Weekly | Paraná |
| [INMET](inmet.md) | Meteorology | Daily | 600+ stations (data requires token) |
| [Notícias Agrícolas](noticias_agricolas.md) | Quotes (CEPEA fallback) | Daily | Commodities |
| [Queimadas/INPE](queimadas.md) | Fire hotspots | Daily | 6 biomes, 13 satellites |
| [Deforestation PRODES/DETER](desmatamento.md) | Deforestation + alerts | Annual/Daily | Amazônia, Cerrado, Pantanal |
| [MapBiomas](mapbiomas.md) | Land cover and use | Annual | Municipalities (1985-present) |
| [B3 Agricultural Futures](b3.md) | Daily settlements + open interest | Daily | 7 agricultural contracts |
| [UN Comtrade](comtrade.md) | Bilateral trade + trade mirror | Monthly/Annual | ~200 countries, HS codes |
| [ANTAQ](antaq.md) | Port cargo movement | Annual | ⚠️ Source offline since 2026-06-23 |
| [ANP Diesel](anp_diesel.md) | Resale prices + diesel volumes | Weekly/Monthly | States, municipalities, 2013+ |
| [ANTT Pedagio](antt_pedagio.md) | Vehicle traffic at toll plazas | Monthly | 200+ plazas, 2010+ |
| [MAPA PSR](mapa_psr.md) | Rural insurance policies and claims | Annual | 27 states, 2006+ |
| [SICAR](sicar.md) | Rural Environmental Registry (CAR) | Continuous | 27 states, 7.4M+ properties |
| [ZARC](zarc.md) | Agricultural Climate Risk Zoning | Weekly | 40+ crops, all municipalities |
| [Agrofit/MAPA](defensivos.md) | Registered pesticides | Continuous | ~8K formulated products, ~267K authorizations |
| [FUNAI Indigenous Lands](funai.md) | Indigenous lands (WFS geo) | Continuous | ~740 TIs, all states |
| [ICMBio Federal Conservation Units](icmbio.md) | Federal conservation units (WFS geo) | Continuous | 344 federal UCs |
| [INCRA Quilombola Territories](incra.md) | Quilombola territories (WFS geo) | Continuous | ~426 territories |
| [Land Registry/INCRA](acervo_fundiario.md) | Certified parcels + settlements (shapefile ZIP) | Continuous | SIGEF (15 states) + SNCI (10 states) + settlements Brazil |
| [IBAMA Environmental Embargoes](ibama.md) | Environmental embargoes (SIFISC CSV + WKT) | Monthly | ~114K embargoes |
| [MapBiomas Alerta](mapbiomas_alerta.md) | Deforestation alerts (GraphQL) | Weekly | National |
| [Lista Suja](lista_suja.md) | Forced-labor registry (PDF) | Semiannual | National |
| [ANA/SNIRH](ana.md) | Hydrography, irrigation, water availability (ArcGIS REST) | Variable | National |
| [SFB](sfb.md) | Public forests, concessions, IFN (ArcGIS REST) | Annual | National |
| [RNC/CultivarWeb](rnc.md) | Registered/protected cultivars | Continuous | ~37K registered, ~5K protected |
| [EMBRAPA Solos](embrapa_solos.md) | Soil profiles and soil map | Continuous | 34K profiles, 2.8K polygons |
| [Fundação Rio Verde](rio_verde.md) | MT soybean cultivar trials | Annual | ~97 cultivars x 4 seasons |
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

The `MetaInfo` object contains the following information:

| Field | Type | Description |
|-------|------|-------------|
| `source` | str | Source name (cepea, conab, ibge) |
| `source_url` | str | Exact URL accessed |
| `source_method` | str | Access method (httpx, cache) |
| `fetched_at` | datetime | Collection timestamp |
| `from_cache` | bool | Whether it came from the local cache |
| `cache_key` | str | Cache key |
| `cache_expires_at` | datetime | When the cache expires |
| `records_count` | int | Number of records |
| `columns` | list | Returned columns |
| `fetch_duration_ms` | int | Fetch time in ms |
| `parse_duration_ms` | int | Parsing time in ms |
| `agrobr_version` | str | agrobr version |
| `parser_version` | int | Parser version used |

## Integrity Verification

MetaInfo lets you verify data integrity:

```python
# Check that the DataFrame was not altered
is_valid = meta.verify_hash(df)

# Export metadata for auditing
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
