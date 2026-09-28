# Land Registry — SIGEF, SNCI and Settlements (INCRA)

!!! warning "License `nc` — commercial use prohibited"
    The data from INCRA's Land Registry is public-use data with a commercial-use restriction.
    The first call emits a `UserWarning` reminding you of this restriction.

!!! note "Not reachable from outside Brazil in the environments tested"
    The `certificacao.incra.gov.br` host answered normally from Brazil, but did not answer
    (connection timeout) from GitHub Actions runners nor from any of the 8 international
    nodes tested on 2026-08-31 (Austria, Cyprus, Finland, Iran, Serbia, Ukraine).
    If you run agrobr outside Brazil and get `httpx.ConnectTimeout` on this source,
    that network restriction is the likely cause — not a library bug.
    That is why this source's live tests carry the `integration_br` marker and are excluded from CI.

!!! info "Geospatial dependency"
    Install `pip install agrobr[geo]` before any query. The extra includes
    `geopandas` and `pyogrio`; availability is checked before downloading ZIP
    files that may be hundreds of megabytes.

## Overview

| Item | Detail |
|------|---------|
| Provider | INCRA (Instituto Nacional de Colonizacao e Reforma Agraria) |
| Data | Certified parcels (SIGEF/SNCI) + settlements |
| Access | Static shapefile ZIP download |
| Endpoint | `https://certificacao.incra.gov.br/csv_shp/zip/` |
| CRS | EPSG:4674 (SIRGAS 2000) |
| Encoding | DBF latin1 (cp1252) |
| Update | Continuous (varies by state, exposed via `Last-Modified`) |
| Authentication | None |
| License | Commercial use prohibited — `nc` |

## Coverage by dataset

| Dataset | Available states | Typical size | Granularity |
|---|---|---|---|
| **SIGEF** | 27/27 | 2-766 MB per state | Per state |
| **SNCI** | 27/27 | 0.01-23 MB per state | Per state |
| **Settlements** | Brazil-wide single | 50 MB | Full Brazil, client-side state filter |

INCRA publishes SIGEF and SNCI for all 27 states (2026-09-22). A state without a file on the server raises `SourceUnavailableError` (HTTP 404).

## Public functions

```python
import asyncio
from agrobr import acervo_fundiario

async def main():
    # SIGEF — certified parcels post-2013
    df = await acervo_fundiario.sigef("GO")
    df, meta = await acervo_fundiario.sigef("MG", return_meta=True)
    df_pl = await acervo_fundiario.sigef("SP", as_polars=True)
    gdf = await acervo_fundiario.sigef_geo("GO", bbox=(-50, -16, -49, -15))

    # SNCI — certifications from the system that preceded SIGEF (no date cutoff; records up to 2016 exist)
    df = await acervo_fundiario.snci("GO")
    gdf = await acervo_fundiario.snci_geo("MT")

    # Settlements — Brazil-wide single, uf optional
    df = await acervo_fundiario.assentamentos()             # all states
    df = await acervo_fundiario.assentamentos(uf="GO")      # client-side filter
    gdf = await acervo_fundiario.assentamentos_geo(uf="MG")

asyncio.run(main())
```

## Filesystem cache

Downloaded files are stored in `~/.agrobr/cache/acervo_fundiario/{tema}/{UF}.zip` with a `{UF}.json` alongside containing `last_modified`, `etag`, `sha256`, `size_bytes`, `fetched_at`, `source_url`. Where it lives and how to clean it: [What agrobr writes to disk](../advanced/disco.md).

With `return_meta=True`, the `MetaInfo` states where the file came from. When the HEAD confirms the cache: `from_cache=True`, `fetched_at` = the ZIP's original collection (the `fetched_at` in `{UF}.json`) and `source_details` with `revalidado_em`, `etag` and `last_modified`. On a new download: `from_cache=False`, `fetched_at` = the download and `source_details` with only `etag` and `last_modified`.

Revalidation performs a HEAD request and requires at least one matching,
nonempty validator (`ETag` or `Last-Modified`). Changed validators or file sizes
invalidate the cache; missing validators require a new download. Locks are
local to the event loop, and each write cleans up only its own temporary file.

**Potential cache size:**

- SIGEF full Brazil (27 states) ≈ 3.1 GB (largest: MG=766 MB, SP=356 MB, PR=312 MB)
- SNCI full Brazil (27 states) ≈ 105 MB
- Settlements Brazil = 50 MB

On demand. A casual case of 1-3 states usually stays below 1 GB.

**Opt-out:**

```python
df = await acervo_fundiario.sigef("GO", use_cache=False)
```

```bash
export AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED=1
```

## Schemas

### SIGEF

| Column | Type | Description |
|---|---|---|
| codigo_parcela | str | Parcel UUID |
| rt | str | Technical responsible |
| art | str | Technical responsibility note |
| situacao | str | Reported situation |
| codigo_imovel | str | Rural property code |
| data_submissao | datetime | Submission date |
| data_aprovacao | datetime | Approval date |
| status | str | Certification status |
| nome_area | str | Area/farm name |
| registro_matricula | str | Registry record number |
| registro_data | datetime | Registration date (nullable) |
| cod_municipio | int | Municipality IBGE code |
| uf | str | State abbreviation (mapped from IBGE `uf_id`) |
| geometry | Polygon Z | Geometry with vertex altitude (only in `_geo`) |

### SNCI

| Column | Type | Description |
|---|---|---|
| num_processo | str | Process number |
| sr | str | Regional superintendency |
| num_certificacao | str | Certification number |
| data_certificacao | datetime | Certification date |
| area_peca_tecnica | float | Area in hectares (technical document) |
| cod_profissional | str | Accredited professional code |
| cod_imovel_rural | str | Rural property code |
| nome_imovel | str | Property name |
| uf | str | State abbreviation (from `uf_municip`) |
| geometry | Polygon | Only in `_geo` |

### Settlements

| Column | Type | Description |
|---|---|---|
| codigo_sipra | str | Project SIPRA code |
| nome_projeto | str | Project name |
| municipio | str | Municipality |
| uf | str | State abbreviation |
| area_ha | float | Declared area in hectares |
| capacidade | int | Family capacity |
| num_familias | int | Number of settled families |
| fase | int | Project phase |
| data_criacao | datetime | Creation date |
| forma_obtencao | str | Form of acquisition |
| data_obtencao | datetime | Acquisition date |
| area_calc_ha | float | Calculated area in hectares |
| sr | str | Regional superintendency (nullable) |
| descricao_fase | str | Phase description (nullable) |
| geometry | Polygon | Only in `_geo`; null when the source publishes the record without geometry (1 of 8,216 on 2026-09-22) |

## Filters

### `bbox`

Applied by `pyogrio` while reading the shapefile (pre-read spatial filter, 4-9x less RAM/time than filtering `gdf.cx[]` after loading everything).

```python
gdf = await acervo_fundiario.sigef_geo("MG", bbox=(-44, -18, -43, -17))
```

### `uf` in settlements

The settlements dataset is Brazil-wide single — the `uf` filter is client-side, normalizing the `uf` column (`.str.upper().str.strip()`) and comparing.

If the source brings a state outside the 27 abbreviations, the parser does not drop the row: the `acervo_fundiario_dirty_uf_data` log reports the counts. The `uf="MG"` filter returns only rows with `MG`; the others remain in the DataFrame when `uf=None`. In the 2026-09-22 capture, all 8,216 rows had a valid state.

## Limitations

- **Verified TLS** — downloads and health checks validate certificates and hostnames.
  Certificate errors terminate the connection without disabling verification.
- **No private/public distinction** — the shapefile has no type field (that was a distinction of the legacy WFS)
- **Cache size may accumulate into GB** — see the "Filesystem cache" section
