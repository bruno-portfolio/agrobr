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
| `ifn_conglomerados` | 68 points in DF (2026-10-02; national total not verified) | Point | uf, bioma, bbox |

## Access via ArcGIS REST

| Parameter | Value |
|-----------|-------|
| Base URL | `https://mapas.florestal.gov.br/server/rest/services` |
| CNFP Service | `Hosted/CNFP_v19_03_retificado_17072025/FeatureServer/9` |
| Concessoes Service | `Hosted/unidades_concessoes_florestais/FeatureServer/0` |
| IFN Service | `DadosAbertos-IFN/dataset_ifn_tb_pontos_lote/FeatureServer/0` |
| Pagination | Keyset on CNFP and concessions (`fid > last`, `orderByFields=fid`), 2,000 features per page; IFN by `co_pontos_lote` and its auxiliary layer by `co_lote` |
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
| lote | nullable str | Published name from the auxiliary layer, joined on `co_lote` |
| conglomerado | str | Conglomerate |
| uf | str | State (abbreviation) |
| municipio | str | Municipality |
| bioma | str | Biome |
| ciclo | nullable str | Source text from `nu_ciclo_execucao` |

## Specifics

- **IFN, schema 1.1**: retains the seven previous columns and adds nullable text `ciclo`. `lote` comes from `DadosAbertos-IFN/dataset_ifn_tb_lote/FeatureServer/23`, queried without geometry for referenced codes only, in groups of at most 100. The many-to-one join preserves point order and row count. An unmatched code, duplicate code in the auxiliary layer or missing field raises `ParseError`; an auxiliary request failure does not return a partial table. A code or lot name published as null remains null and is recorded in `MetaInfo.validation_warnings`.
- **IFN provenance**: `source_details.resources` lists data pages for points and lots, each with `role`, query URL, `sha256` and `bytes`. Count responses are excluded. `raw_content_hash` is the SHA-256 of this list serialized as UTF-8 JSON with sorted keys, no spaces and Unicode characters preserved (`sort_keys=True`, `separators=(",", ":")`, `ensure_ascii=False`); `raw_content_size` measures this manifest. `source_details.hash_kind` is `resource_manifest_sha256`, and `source_details.resource_bytes` sums the original bodies. An empty selection has the list `[]` and does not query the auxiliary layer.
- **Reconstructing the IFN digest**: `source_details.manifest_encoding="canonical_json_utf8"` and `manifest_fields=["resources"]` identify the serialization and its source. `manifest_root="resources"` indicates that the serialized content is the list in `source_details.resources` itself, without wrapping it in an object. UTF-8 encoding preserves Unicode, sorts keys and uses compact separators, as specified above.
- **IFN biome filter**: uses `UPPER(no_bioma)`; returned text retains the published case, such as `Cerrado` in DF. The geo variant requests EPSG:4326; the points' native CRS is EPSG:4674. Health checks the DF point count without certifying the join; reconciliation checks IDs, attributes and lot names against an independent reading.

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

- IFN uses the active point and lot layers following this migration. The DF capture from October 2, 2026 contains 68 points in lot `DF-01`. Historical goldens from the former service contain only unavailability errors: historical ID or population correspondence has not been established.
- Points and lots are current publications queried separately; the join does not promise a transactional snapshot across both layers.
- Data reflects the current state of the SFB ArcGIS Server
- Forest concessions have few records (~8 polygons)
- 2s throttle after 5 pages to avoid overloading the server

## Raw collection

`agrobr.bruto.coletar("sfb", "cnfp", ...)` stores the original pages of the CNFP layer (`Hosted/CNFP_v19_03_retificado_17072025/FeatureServer/9`) as Esri JSON, in the native CRS (`wkid` 102100, `latestWkid` 3857, recorded as `EPSG:3857`), with every attribute and the geometry; `cnfp_geo` stays in EPSG:4326. Always national: state and bbox are refused. The edition in the manifest is `20250717`, the rectification date in the service name; the layer's last edit comes in the responses' `etag`. Coverage is checked by `returnCountOnly` before and after the pages and by the official `fid` list (`returnIdsOnly`). Pages are ranges of that list, requested with `orderByFields=fid`, and each must bring exactly the range's `fid` values, in ascending order.

On 2026-10-04 the layer had 20,829 features; the largest had 5.52 MB, and 100-feature pages reached 37.7 MB. With the defaults (`tamanho_pagina=100`, 8 MiB `max_bytes_pagina`), the collection stops early, on the 4th page, with a `ResourceLimitError` that states the page, the `fid` range and the limit. No page size fits the default limits: 3 or more features per page exceed 8 MiB, and 2 or fewer exceed `max_paginas`. For the whole country:

```python
from agrobr import bruto

coleta = await bruto.coletar(
    "sfb",
    "cnfp",
    destino="coleta",
    tamanho_pagina=20,
    limites=bruto.LimitesBrutos(max_bytes_pagina=24 * 1024**2),
)
```

The 2026-10-04 collection closed `ok` with 1,042 pages (980 MB), the largest 19.3 MB, in 37.5 minutes. See the [raw collection API](../api/bruto.md) and the [manifest contract](../contracts/bruto.md).
