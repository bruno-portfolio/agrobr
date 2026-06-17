# Deforestation (PRODES/DETER)

## Overview

| Field | Value |
|-------|-------|
| **Provider** | INPE — Instituto Nacional de Pesquisas Espaciais |
| **Programs** | PRODES (annual) and DETER (daily alerts) |
| **Access** | Public WFS API (TerraBrasilis GeoServer) |
| **Format** | CSV via WFS outputFormat + GeoJSON for geometry |
| **Authentication** | None |
| **License** | Federal government public data |
| **Time Series** | PRODES: 2000+, DETER: 2016+ (Amazonia), 2020+ (Cerrado) |

## Data Origin

INPE operates two complementary deforestation monitoring systems:

- **PRODES**: Consolidated annual mapping of clear-cut deforestation. Uses Landsat imagery (30m) to generate deforestation polygons with a minimum area of 6.25 hectares. Official result used by the federal government.

- **DETER**: Daily alert system for enforcement actions. Uses imagery from sensors such as CBERS-4, AMAZONIA-1 and Landsat with variable resolution. Detects deforestation, degradation, mining and burn scars.

## Access via TerraBrasilis

Data is accessed via the TerraBrasilis GeoServer WFS with `outputFormat=csv` and filters via `CQL_FILTER`.

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
| **maxFeatures default** | 10,000 (tabular: 50,000) |
| **outputFormat** | `application/json` (GeoJSON) |

## Geometry (deter_geo)

The `deter_geo()` function returns DETER alerts with geometry polygons as a GeoDataFrame.

| Field | Value |
|-------|-------|
| **Geometry column (AMZ)** | `geom` |
| **Geometry column (Cerrado)** | `st_multi` |
| **Format** | MultiPolygon EPSG:4326 |
| **Volume per feature** | ~1.1 KB with geometry |
| **maxFeatures default** | 10,000 (tabular: 50,000) |
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
    data_inicio="2024-01-01",
    data_fim="2024-06-30",
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
    data_inicio="2024-01-01",
    data_fim="2024-06-30",
)
```

## Limitations

- DETER only available for Amazonia and Cerrado
- WFS limits 50,000 features per request — filter by state and/or year (warning `desmatamento_*_truncated` when the cap is reached)
- The Source API (`agrobr.desmatamento.*`) returns individual polygons (fine granularity); the `datasets.desmatamento` dataset delivers the annual aggregate by uf/class/biome according to the contract
- After the BiomasBR migration (03/2026), the PRODES layers for Amazonia, Pantanal, Caatinga and Mata Atlantica are temporarily broken in the INPE GeoServer (ServiceException for any client); Cerrado and Pampa operational
- DETER is an alert system, not a consolidation one — there may be overlap
- `prodes_geo()` and `deter_geo()` return geometry (~10x more volume than tabular) — use filters to reduce data

## Cache and Updating

- **PRODES**: TTL 24h (consolidated annual data, updated ~1x/year)
- **DETER**: TTL 24h (daily alerts, updated frequently)
- Recommended: use state and year filters to reduce data volume

## Links

- [TerraBrasilis](https://terrabrasilis.dpi.inpe.br)
- [PRODES](https://www.obt.inpe.br/OBT/assuntos/programas/amazonia/prodes)
- [DETER](https://www.obt.inpe.br/OBT/assuntos/programas/amazonia/deter)
