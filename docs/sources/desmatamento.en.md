# Deforestation (PRODES/DETER)

## Overview

| Field | Value |
|-------|-------|
| **Provider** | INPE — Instituto Nacional de Pesquisas Espaciais |
| **Programs** | PRODES (annual) and DETER (daily alerts) |
| **Access** | Public WFS API (TerraBrasilis GeoServer) |
| **Format** | WFS 2.0 GeoJSON (`application/json`) in both modes |
| **Authentication** | None |
| **License** | CC BY-SA 4.0 (INPE), with attribution and ShareAlike for adaptations |
| **Time Series** | PRODES: 2000+, DETER: 2016+ (Amazonia), 2020+ (Cerrado) |

## Data Origin

INPE operates two complementary deforestation monitoring systems:

- **PRODES**: Consolidated annual mapping of clear-cut deforestation. Uses Landsat imagery (30m) to generate deforestation polygons with a minimum area of 6.25 hectares. Official result used by the federal government.

- **DETER**: Daily alert system for enforcement actions. Uses imagery from sensors such as CBERS-4, AMAZONIA-1 and Landsat with variable resolution. Detects deforestation, degradation, mining and burn scars.

## Access via TerraBrasilis

Data is accessed via the TerraBrasilis GeoServer WFS 2.0.0 as JSON (`outputFormat=application/json`), paginated with `startIndex`/`count`, with filters via `CQL_FILTER`.

### PRODES — Workspaces by Biome

| Biome | Workspace | Layer |
|-------|-----------|-------|
| Amazonia | prodes-amazon-nb | yearly_deforestation_biome |
| Cerrado | prodes-cerrado-nb | yearly_deforestation |
| Caatinga | prodes-caatinga-nb | yearly_deforestation |
| Mata Atlantica | prodes-mata-atlantica-nb | yearly_deforestation |
| Pantanal | prodes-pantanal-nb | yearly_deforestation |
| Pampa | prodes-pampa-nb | yearly_deforestation |

### DETER — Workspaces by Biome

| Biome | Workspace | Layer |
|-------|-----------|-------|
| Amazonia | deter-amz | deter_amz |
| Cerrado | deter-cerrado-nb | deter_cerrado |

## Geometry (prodes_geo)

The `prodes_geo()` function returns consolidated PRODES deforestation with geometry polygons as a GeoDataFrame.

| Field | Value |
|-------|-------|
| **Geometry column** | `geom` (uniform across all 6 biomes) |
| **Format** | MultiPolygon EPSG:4326 |
| **max_registros (default)** | 10,000 (tabular: 50,000) |
| **outputFormat** | `application/json` (GeoJSON) |

## Geometry (deter_geo)

The `deter_geo()` function returns DETER alerts with geometry polygons as a GeoDataFrame.

| Field | Value |
|-------|-------|
| **Geometry column (AMZ)** | `geom` |
| **Geometry column (Cerrado)** | `st_multi` |
| **Format** | MultiPolygon EPSG:4326 |
| **Volume per feature** | ~1.1 KB with geometry |
| **max_registros (default)** | 10,000 (tabular: 50,000) |
| **outputFormat** | `application/json` (GeoJSON) |

The geometry column is biome-specific in the GeoServer. The parser normalizes both to `geometry` in the output GeoDataFrame.

## Biome Normalization

The `bioma` parameter accepts variants with/without accents and is case insensitive:

- `"amazonia"` or `"amazônia"` → `"Amazônia"`
- `"cerrado"` → `"Cerrado"`
- `"mata atlantica"` or `"mata atlântica"` → `"Mata Atlântica"`

Normalization is applied automatically in `prodes()`, `prodes_geo()`, `deter()` and `deter_geo()`.

## Usage Example

```python
import agrobr

# PRODES — consolidated annual deforestation
df_prodes = await agrobr.desmatamento.prodes(
    bioma="Cerrado",
    ano=2022,
    uf="MT",
)

# DETER — real-time alerts
df_deter = await agrobr.desmatamento.deter(
    bioma="Amazônia",
    uf="PA",
    inicio="2024-01-01",
    fim="2024-06-30",
)

# With metadata
df, meta = await agrobr.desmatamento.prodes(
    bioma="Cerrado", ano=2022, return_meta=True
)
print(meta.records_count, meta.fetch_duration_ms)

# PRODES with geometry (requires pip install agrobr[geo])
gdf_prodes = await agrobr.desmatamento.prodes_geo(
    bioma="Cerrado",
    ano=2022,
    uf="MT",
)

# DETER with geometry (requires pip install agrobr[geo])
gdf = await agrobr.desmatamento.deter_geo(
    bioma="Amazônia",
    uf="PA",
    inicio="2024-01-01",
    fim="2024-06-30",
)
```

## Limitations

- DETER only available for Amazonia and Cerrado
- For the Amazon, agrobr's PRODES is the biome cut (`yearly_deforestation_biome`), not the Legal Amazon, where INPE publishes its headline rate. In 2024, INPE's technical note gives about 6,288 km² for the Legal Amazon, and agrobr's sum of the biome's states gives 6,068.9 km²
- agrobr paginates the WFS: `tamanho_pagina` features per page (500; 100 in the `_geo` functions; up to 2,000 and 500), with 2 s between requests, up to `max_registros` (50,000; 10,000 in the `_geo` functions). Beyond the limit, the prefix comes out in `fid` (PRODES) or `gid` (DETER) order, with a `UserWarning`. Filter by `ano`, `uf` or dates, which go to the server, or use `max_registros=None` ([migration guide, §84](../guides/migracao-2.en.md#84-desmatamento-pagination-cut-and-cost-of-the-default-call))
- The Source API (`agrobr.desmatamento.*`) returns individual polygons (fine granularity); the `datasets.desmatamento` dataset delivers aggregates according to the contract: annual by uf/class/biome for PRODES and daily by uf/municipality/class/biome for DETER
- After the BiomasBR migration (03/2026), the PRODES layers for Amazonia, Pantanal, Caatinga and Mata Atlantica are temporarily broken in the INPE GeoServer (ServiceException for any client); Cerrado and Pampa operational
- DETER is an alert system, not a consolidation one — there may be overlap
- In DETER Cerrado, `municipio_id` is always null because the source layer does not provide this identifier.
- `prodes_geo()` and `deter_geo()` return geometry (~10x more volume than tabular) — use filters to reduce data

## Cache and Updating

- There is no local cache: every call downloads the data from TerraBrasilis.
- PRODES publishes consolidated annual data, updated approximately once a year.
- DETER publishes daily alerts, updated frequently.
- State and year filters are recommended to reduce data volume.

## Links

- [TerraBrasilis](https://terrabrasilis.dpi.inpe.br)
- [PRODES](https://www.obt.inpe.br/OBT/assuntos/programas/amazonia/prodes)
- [DETER](https://www.obt.inpe.br/OBT/assuntos/programas/amazonia/deter)

## PRODES year range

`prodes()` and `prodes_geo()` accept `ano` only as an integer or `None`; a year after the current one raises `InvalidParameterError` before any request. A year with no feature in the WFS (not yet published or outside the layer's coverage) returns an empty result, with a `UserWarning` and a warning in `meta.validation_warnings`. Polygon coverage should not be confused with the starting year of historical deforestation-rate series.
