# MapBiomas

## Overview

| Field | Value |
|-------|-------|
| **Provider** | MapBiomas Project — multi-institutional network |
| **Data** | Land cover and use, transitions between classes |
| **Access** | Public XLSX download (data.mapbiomas.org Dataverse) |
| **Format** | XLSX (openpyxl, calamine fallback) |
| **Authentication** | None |
| **License** | Public data — free with attribution |
| **Time Series** | 1985-2024 (Collection 10) |

## Data Origin

MapBiomas is a multi-institutional collaborative project that produces annual maps of land cover and use of Brazil from Landsat satellite imagery (30m resolution). The data is generated via automatic classification using Google Earth Engine.

agrobr accesses the **tabular statistics** (areas by class, biome and state) made available as XLSX spreadsheets on the project's statistics page. Geospatial data (rasters) is not included in this version.

## Collections

MapBiomas publishes annual collections with methodological improvements:

| Collection | Date | Period |
|---------|------|---------|
| 10 (current) | August 2025 | 1985-2024 |

agrobr uses the current collection (10). The `colecao` parameter accepts only 10 (or None); other values raise `ValueError`.

## Data Structure

### Coverage (COVERAGE_10)

Data in wide format: one column per year (1985-2024) with area in hectares for each biome x state x class combination.

After parsing, agrobr converts it to long format: one row per biome x state x class x year combination.

### Transition (TRANSITION_10)

Area in hectares of transition between pairs of classes for each time period. Includes annual periods (e.g.: 2019-2020), five-year periods and the 1985-2024 total.

## Usage Example

```python
import agrobr

# Cerrado coverage in 2020
df = await agrobr.mapbiomas.cobertura(bioma="Cerrado", ano=2020)

# Pasture (class 15) in Goias
df = await agrobr.mapbiomas.cobertura(bioma="Cerrado", estado="GO", classe_id=15)

# Forest→pasture transition in Cerrado
df = await agrobr.mapbiomas.transicao(
    bioma="Cerrado",
    classe_de_id=3,   # Forest Formation
    classe_para_id=15, # Pasture
    periodo="2019-2020",
)

# With metadata
df, meta = await agrobr.mapbiomas.cobertura(
    bioma="Cerrado", ano=2020, return_meta=True
)
print(meta.records_count, meta.fetch_duration_ms)
```

## Limitations

- Tabular data only (statistics). Geospatial data (rasters/GEE) is left for a future version
- The state XLSX (~23 MB) is downloaded in full on the first call (parsing selects the filters)
- Municipality level (~660 MB) available via `cobertura(nivel="municipio")` — heavy download, recommended to filter by state/municipality/biome. No integrated local cache yet
- Class names follow the official MapBiomas legend (in Portuguese)

## Cache and Updating

- **TTL**: 7 days (collections are published annually)
- MapBiomas publishes a new collection per year with retroactively recalculated data
- Recommended: specify filters to reduce data volume in the DataFrame

## Links

- [MapBiomas Brasil](https://brasil.mapbiomas.org)
- [Estatisticas](https://brasil.mapbiomas.org/estatisticas/)
- [Legenda](https://brasil.mapbiomas.org/codigos-de-legenda/)
- [Citation (FAQ)](https://brasil.mapbiomas.org/faq/?tema=dados)
