# FUNAI — Indigenous Lands

## Overview

| Item | Detail |
|------|---------|
| Provider | FUNAI (Fundacao Nacional dos Povos Indigenas) |
| Data | Indigenous Lands as polygons |
| Access | OGC WFS (GeoServer) |
| Format | WFS 2.0 GeoJSON (`application/json`) in both modes |
| Authentication | None |
| License | FUNAI term: reproduction with source citation ([details](../licenses.en.md#funai)) |
| Features | 665 Indigenous lands (2026-09-23) |

## Access via WFS

| Parameter | Value |
|-----------|-------|
| Endpoint | `geoserver.funai.gov.br/geoserver/Funai/ows` |
| WFS Version | 2.0.0 |
| Layer | `Funai:tis_poligonais` |
| CRS | EPSG:4674 in the layer; agrobr requests `srsName=EPSG:4326` and returns EPSG:4326 (GeoServer reprojection) |

## Usage Example

```python
import asyncio
from agrobr import funai

async def main():
    # All Indigenous lands
    df = await funai.terras_indigenas()

    # Filter by state
    df = await funai.terras_indigenas(uf="MT")

    # Filter by phase
    df = await funai.terras_indigenas(fase="Regularizada")

    # With geometry (requires geopandas)
    gdf = await funai.terras_indigenas_geo(bbox=(-56, -16, -54, -14))

    # With metadata
    df, meta = await funai.terras_indigenas(return_meta=True)

asyncio.run(main())
```

## Columns

| Column | Type | Description |
|--------|------|-----------|
| codigo | int | Indigenous land code |
| nome | str | Indigenous land name |
| etnia | str | Predominant ethnicity |
| municipio | str | Seat municipality |
| uf | str | State of the land as published; 18 lands list more than one (e.g. "AM, RR") |
| area_ha | float | Area declared by FUNAI in hectares (`superficie_perimetro_ha`), not the polygon's |
| fase | str | Process phase |
| modalidade | str | Indigenous land type |
| data_atualizacao | datetime64[ns] | Update date published as dd/mm/yyyy; null in 146 of the 665 lands |
| feature_id | str | WFS feature identifier (text; may vary between requests) |
| gid | int | Record identifier in the layer |
| reestudo_ti | str | Re-study status as published (empty, "Reestudo" or "Principal") |
| cr | str | FUNAI Regional Coordination |
| faixa_fronteira | str | "Sim"/"Não", as published |
| undadm_codigo | int | Administrative unit code |
| undadm_nome | str | Administrative unit name |
| undadm_sigla | str | Administrative unit acronym |
| dominio_uniao | str | "t"/"f", as published |
| epsg | int | EPSG of the source geometry (4674 for every land) |

State and administrative flags keep the published text, in the installed pandas default dtype (`str` on
pandas 3, `object` on 2); the update date comes as `datetime64[ns]`, and an unreadable date becomes `NaT`
with a `UserWarning` and a warning in `meta.validation_warnings` (`funai.terras_indigenas` 2.0 contract).

`area_ha` is the area declared by FUNAI (`superficie_perimetro_ha`), passed through without recalculation, and it may
differ from the published polygon: for the Mashco do Rio Chandless land (AC), 421 ha declared against 543,430 ha in the
polygon (2026-09-26). In `terras_indigenas_geo`, a land whose declared area differs by more than 5% from the polygon
area (IBGE Albers projection) comes with a warning in `validation_warnings` and `UserWarning`, and the list with both
areas goes to `source_details["area_divergente"]`. `terras_indigenas`, without geometry, does not compare. In AC, 3 of
the 34 lands exceed 5%.

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `uf` | `None` | State abbreviation; matches any state in the published field (lands in more than one state come as "AM, RR") |
| `fase` | `None` | One of the phases below, exact match |
| `bbox` | `None` | (lon_min, lat_min, lon_max, lat_max) in EPSG:4326 |
| `max_registros` | 10,000 (1,000 in `_geo`) | Cap on lands read in code order; `uf` and `fase` filter that prefix locally, and a cut that leaves the selection partial raises a `UserWarning` |
| `tamanho_pagina` | 250 (10 in `_geo` or with `bbox`) | Maximum 1,000 (100 in `_geo` or with `bbox`) |

## Phases

Regularizada, Homologada, Declarada, Delimitada, Em Estudo, Encaminhada RI.

## Raw collection

`agrobr.bruto.coletar("funai", "terras_indigenas", ...)` keeps the original GeoJSON pages of the WFS layer `Funai:tis_poligonais`, in the native CRS (`EPSG:4674`) and with every attribute, without the `propertyName` and `srsName` that `terras_indigenas()` and `terras_indigenas_geo()` use. `agrobr.bruto.coletar("funai", "terras_indigenas_pontos", ...)` does the same for layer `Funai:tis_pontos`, one point per Indigenous land in the `Em Estudo` phase (163 on 2026-10-04), which the tabular API does not read. Both collections are always national (state and bbox refused). Pages use `sortBy=gid`, and the collection requires `gid` to be an integer, present, unique and strictly increasing across pages; since the GeoServer refuses `resultType=hits` (HTTP 403), the counts before and after are the `numberMatched` of a one-feature page. Polygons default to 20 features per page: the largest Indigenous land exceeds 2.9 MB, and 100-feature pages would reach 14 MB, above the 8 MiB limit. See the [raw collection API](../api/bruto.md) and the [manifest contract](../contracts/bruto.md).

## Limitations

- Only polygonal Indigenous lands (points and lines excluded)
- Data reflects the current state of the FUNAI GeoServer
- License: reproduction with source citation under FUNAI's term for geoprocessing and maps; the gov.br portal footer states CC BY-ND 3.0 for site content
