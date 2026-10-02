# SFB — Servico Florestal Brasileiro

## Overview

| Item | Detail |
|------|---------|
| Provider | SFB (Servico Florestal Brasileiro) |
| Data | Public forests (CNFP), forest concessions, National Forest Inventory (IFN) |
| Access | ArcGIS REST API |
| Format | JSON without geometry (tabular) / GeoJSON (geo) |
| Authentication | None |
| License | Public data |

## Layers

| Layer | Features | Geometry | Filters |
|-------|----------|-----------|---------|
| `cnfp` | 20,829 polygons (2026-09-23) | Polygon | uf, bioma, categoria, bbox |
| `concessoes` | 8 polygons | Polygon | uf, bbox |
| `ifn_conglomerados` | ~14.5K points (not verified: service down) | Point | uf, bioma, bbox |

## Access via ArcGIS REST

| Parameter | Value |
|-----------|-------|
| Base URL | `https://mapas.florestal.gov.br/server/rest/services` |
| CNFP Service | `Hosted/CNFP_v19_03_retificado_17072025/FeatureServer/9` |
| Concessoes Service | `Hosted/unidades_concessoes_florestais/FeatureServer/0` |
| IFN Service | `DadosAbertos-IFN/Conglomerado/FeatureServer/0` |
| Pagination | Keyset on CNFP and concessions (`fid > last`, `orderByFields=fid`), 2,000 features per page; offset on IFN |
| Throttle | 2s delay after 5 pages |

## Usage Example

```python
import asyncio
from agrobr import sfb

async def main():
    # CNFP — Cadastro Nacional de Florestas Publicas
    df = await sfb.cnfp(uf="AM")
    df = await sfb.cnfp(bioma="Amazonia", categoria="FLONA")

    # CNFP with geometry
    gdf = await sfb.cnfp_geo(uf="PA")

    # Forest concessions
    df = await sfb.concessoes()
    gdf = await sfb.concessoes_geo()

    # IFN — National Forest Inventory (conglomerates)
    df = await sfb.ifn_conglomerados(uf="MG")
    df = await sfb.ifn_conglomerados(bioma="Cerrado")

    # IFN with geometry
    gdf = await sfb.ifn_conglomerados_geo(uf="SP")

    # Filter by bbox
    df = await sfb.cnfp(bbox=(-60, -10, -55, -5))

    # With metadata
    df, meta = await sfb.cnfp(uf="AM", return_meta=True)

    # Polars
    df = await sfb.cnfp(as_polars=True)

asyncio.run(main())
```

## Columns by Layer

### cnfp

| Column | Type | Description |
|--------|------|-------------|
| fid | int | Record ID |
| nome | str | Public forest name |
| uf | str | State (abbreviation) |
| bioma | str | Biome |
| categoria | str | Forest category |
| tipo | str | Type |
| governo | str | Government level |
| classe | str | Class |
| area_ha | float | Area in hectares |
| ano_criacao | Int64 | Creation year taken from the published date (see Specifics) |
| ano_criacao_texto | str | Text published in `anocriacao`, as it came from the source (null when the source publishes it empty; a feature without the `anocriacao` field raises `ParseError`) |
| municipio | str | Municipality |

### concessoes

| Column | Type | Description |
|--------|------|-------------|
| fid | int | Record ID |
| nome | str | Unit name |
| uf | str | State (abbreviation) |
| bioma | str | Biome |
| area_ha | float | Area in hectares |
| ano_criacao | Int64 | Creation year (the service publishes the year as an integer) |
| grupo | str | Group |
| categoria | str | Category |

### ifn_conglomerados

| Column | Type | Description |
|--------|------|-------------|
| id | int | Record ID |
| codigo_lote | int | Lot code |
| lote | str | Lot |
| conglomerado | str | Conglomerate |
| uf | str | State (abbreviation) |
| municipio | str | Municipality |
| bioma | str | Biome |

## Specifics

- **CNFP service name**: includes the rectification date in the path (`CNFP_v19_03_retificado_17072025`)
- **Keyset pagination**: on CNFP and concessions, pages follow increasing `fid` (`fid > last` with `orderByFields=fid`). If the pages add up to fewer features than the official count, the query raises `SourceUnavailableError` stating how many are missing, instead of returning a partial result. An HTML response (maintenance or a WAF block, even with status 200) also becomes `SourceUnavailableError`
- **CNFP creation year**: the service's `anocriacao` field is text with the full date (`DD/MM/YYYY`, `DD-MM-YYYY`; rarely `YYYY-MM-DD`, `YYYY/MM/DD` or the year alone). agrobr publishes the year when the text has a single year. It is null when the field is blank or `-`, and when the date is compound with different years (overlapping units, e.g. `22/06/2011 / 10-01-2002` on a "PA / APA"). In that last case a `UserWarning` and `MetaInfo.validation_warnings` report the count and up to three examples of the published text; the `sfb_ano_criacao_ambiguo` log is also retained. In the 2026-09-23 layer: 15,068 of 20,829 records with a year, 4,718 blank or `-` and 1,043 compound with different years. The `ano_criacao_texto` column carries the published text on every row, compound ones included, and the CNFP `MetaInfo.schema_version` is `1.1`
- **Required fields**: every field requested from the service (`outFields`) must be present in every feature of every page in the tabular functions, and in every page in the `_geo` variants. The service sends all of them, nulls included, so a missing field is a layout change and raises `ParseError` naming the field, instead of returning one column fewer
- **Tabular without geometry**: `cnfp()`, `concessoes()` and `ifn_conglomerados()` request `returnGeometry=false` (the first page of the national CNFP drops from 378 MB to 0.5 MB); geometry only comes with the `_geo` functions
- **Units and CRS**: area in hectares as published (`area_ha` on CNFP, `hectares` on concessions), not recomputed from the geometry. Geometry is requested in EPSG:4326 (`outSR=4326`) and reprojected by the server (CNFP is stored in 3857 and concessions in 4674)
- **Parameters**: an unknown argument raises `TypeError` before any request; invalid `uf`, `bioma` and `categoria` raise `InvalidParameterError`; an invalid `bbox` raises `ValueError`
- **Composite filters**: CNFP and IFN accept a bioma filter in addition to uf and bbox

Identifiers, codes and years use nullable `Int64`; areas use `float64`. Text uses the native pandas dtype (`str` on pandas 3, `object` on pandas 2), including empty results.

## Limitations

- On September 2, 2026 and again on September 23, 2026, `ifn_conglomerados()` and `ifn_conglomerados_geo()` were
  unavailable because the IFN ArcGIS service reported `MapServer not started`. Until the
  service is restored, these calls raise `SourceUnavailableError`.
- Data reflects the current state of the SFB ArcGIS Server
- Forest concessions have few records (~8 polygons)
- 2s throttle after 5 pages to avoid overloading the server
