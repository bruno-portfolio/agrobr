# Land Registry — SIGEF, SNCI and Settlements (INCRA)

!!! info "License: livre"
    Public SIGEF, SNCI and settlement data from INCRA's Land Collection are classified as livre under the access law, Decree 8,777/2016 and INCRA's institutional policy, after a search finding no specific commercial restriction. The earlier claim of a commercial prohibition lacked a substantiated clause. The historical CC BY indication was not recaptured and does not establish a numbered version. Credit INCRA, family, state/scope, file, edition and transformations, preserving express third-party rights. The 2021–2023 plan establishes policy and provenance, not the 2026 status of every service.

!!! note "Not reachable from outside Brazil in the environments tested"
    The `certificacao.incra.gov.br` host answered normally from Brazil, but did not answer
    (connection timeout) from GitHub Actions runners nor from any of the 8 international
    nodes tested on 2026-08-31 (Austria, Cyprus, Finland, Iran, Serbia, Ukraine).
    If you run agrobr outside Brazil and this source raises `SourceUnavailableError`
    with `ConnectTimeout` in the message, that network restriction is the likely cause — not a library bug.
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
| License | Federal public data — `livre`; credit INCRA, layer and extraction |

## Coverage by dataset

| Dataset | Available states | Typical size | Granularity |
|---|---|---|---|
| **SIGEF** | 27/27, in 2 files (public and private) | public 0.4-21 MB, private 1-749 MB per state | Per state |
| **SNCI** | 24/27 on 2026-10-01 (AC, DF and RR without a file) | 0.07-23 MB per state | Per state |
| **Settlements** | Brazil-wide single | 50 MB | Full Brazil, client-side state filter |

INCRA publishes each state's SIGEF in 2 files, `Sigef Público_{UF}.zip` and `Sigef Privado_{UF}.zip`, which partition the state: on 2026-10-01, in all 27 states, the record counts of the two added up to that of `Sigef Brasil_{UF}.zip` (1,848,075 = 159,681 + 1,688,394), and in DF and AP no parcel appeared in both. SNCI is published per state, and the list changes over time: on 2026-10-01, AC, DF and RR had no file (RR's existed on 2026-09-22). A state without a file on the server raises `SourceUnavailableError` (HTTP 404).

## Public functions

```python
import asyncio
from agrobr import acervo_fundiario

async def main():
    # SIGEF — certified parcels post-2013, from the public and private files
    df = await acervo_fundiario.sigef("GO")
    df = await acervo_fundiario.sigef("GO", natureza="publico")  # downloads only the public file
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

Downloaded files are stored in `~/.agrobr/cache/acervo_fundiario/{tema}/{UF}.zip` with a `{UF}.json` alongside containing `last_modified`, `etag`, `sha256`, `size_bytes`, `fetched_at`, `source_url`. The themes are `sigef_publico`, `sigef_privado`, `snci`, `snci_publico`, `snci_privado` and `assentamentos` (the latter in `brasil.zip`). The `sigef/` folder from earlier versions held `Sigef Brasil_{UF}.zip`, which is no longer read: it can be deleted. Where it lives and how to clean it: [What agrobr writes to disk](../advanced/disco.md).

With `return_meta=True`, the `MetaInfo` states where the file came from. When the HEAD confirms the cache: `from_cache=True`, `fetched_at` = the ZIP's original collection (the `fetched_at` in `{UF}.json`) and `source_details` with `revalidado_em`, `etag` and `last_modified`. On a new download: `from_cache=False`, `fetched_at` = the download and `source_details` with only `etag` and `last_modified`. In SIGEF, which may read 2 files, these fields are per file: see [`natureza` in SIGEF](#natureza-in-sigef).

Revalidation performs a HEAD request and requires at least one matching,
nonempty validator (`ETag` or `Last-Modified`). Changed validators or file sizes
invalidate the cache; missing validators require a new download. Locks are
local to the event loop, and each write cleans up only its own temporary file.

**Potential cache size:**

- Full SIGEF (27 states, public + private) ≈ 3.1 GB (0.2 GB public + 2.9 GB private; largest: MG, 749 MB in the private file)
- Full SNCI (24 states published on 2026-10-01) ≈ 104 MB
- Settlements Brazil = 50 MB

On demand. A casual case of 1-3 states usually stays below 1 GB.

**Opt-out:**

```python
df = await acervo_fundiario.sigef("GO", use_cache=False)
```

```bash
export AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED=1
```

With the cache off, the ZIP goes to a system temporary folder and is deleted when the query ends, on success,
error or cancellation; nothing is written to `~/.agrobr/cache/acervo_fundiario/`. The variable accepts
`1`/`true`/`yes`.

## Schemas

Every shapefile field that becomes an output column is required: if INCRA renames or drops one, the parser raises
`ParseError` with the field and the file, instead of returning the DataFrame without the column. A number out of format
(for example, a decimal comma in a text field) becomes null, with a `UserWarning` and an entry in `validation_warnings`.
`cod_municipio`, `capacidade`, `num_familias` and `fase` come out as `Int64`, with or without nulls. In the `_geo`
variants, an invalid geometry is repaired with `shapely.make_valid` (a self-crossing polygon becomes a `MultiPolygon`):
the count goes to `source_details["topology_repaired"]` and to `validation_warnings`, and the delivered polygon differs
from the one INCRA publishes.

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
| natureza | str | `publico` or `privado`: the INCRA file the parcel came from (`Sigef Público_{UF}.zip` or `Sigef Privado_{UF}.zip`) |
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
| natureza | str | Only with `natureza`: `publico` or `privado`, the file the row came from |
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

### `natureza` in SIGEF

The SIGEF shapefile has no field separating public from private parcels: the split is the file INCRA publishes. The `natureza` column states which of the two the row came from.

- `natureza=None` (default) reads both files, with the same volume as the former `Sigef Brasil_{UF}.zip`, and returns the public rows followed by the private ones, each part in file order.
- `natureza="publico"` or `"privado"` downloads only that file. Case and accents are ignored (`"Público"` works); any other value raises `InvalidParameterError` before any network call.
- `MetaInfo`: `source_url` is the URL of the first file read (the public one when both are read); `attempted_sources` lists the files read (`acervo_fundiario_sigef_publico`, `acervo_fundiario_sigef_privado`); `source_details["arquivos"]` holds, per `natureza`, `url`, `from_cache`, `fetched_at`, `sha256`, `size_bytes`, `etag`, `last_modified` and, on a cache hit, `revalidado_em`. `from_cache` is `True` only when every file came from the cache; `fetched_at` is the oldest collection; `raw_content_hash` is filled only for a single file; `raw_content_size` adds up the files; `schema_version` is `1.1`.
- INCRA rewrites the 2 files at different times. If a `codigo_parcela` shows up in both, both rows are kept and `MetaInfo.validation_warnings` says so.

```python
df = await acervo_fundiario.sigef("DF")                      # public + private
publico = await acervo_fundiario.sigef("DF", natureza="publico")
```

### `natureza` in SNCI

Besides `SNCI Brasil_{UF}.zip`, INCRA publishes each state's SNCI as `Imóvel certificado SNCI Público_{UF}.zip` and `Imóvel certificado SNCI Privado_{UF}.zip`, with the same fields.

- `natureza=None` (default) reads the state's Brasil file, as before: same columns, cache in `snci/`, `schema_version` `1.0`.
- `natureza="publico"` or `"privado"` downloads only that file, adds the `natureza` column (also in an empty result) and sets `schema_version` `1.1`; the cache goes to `snci_publico/` or `snci_privado/`. Case and accents are ignored; any other value raises `InvalidParameterError` before the network.
- agrobr does not merge the two files nor promise that their union is the Brasil file: in AL, on 2026-10-03, the sizes did not add up (11,900 + 58,927 bytes against 69,506 for Brasil, published on another date). Whoever needs the partition must check it by records, in the same window.

### `uf` in settlements

The settlements dataset is Brazil-wide single — the `uf` filter is client-side, normalizing the `uf` column (`.str.upper().str.strip()`) and comparing.

If the source brings a state outside the 27 abbreviations, the parser does not drop the row: the `acervo_fundiario_dirty_uf_data` log reports the counts. The `uf="MG"` filter returns only rows with `MG`; the others remain in the DataFrame when `uf=None`. In the 2026-09-22 capture, all 8,216 rows had a valid state.

## Limitations

- **Verified TLS** — downloads and health checks validate certificates and hostnames.
  Certificate errors terminate the connection without disabling verification.
- **Public × private comes from the file, not from a field** — the shapefile has no type field; the `natureza` column states which of INCRA's 2 files the parcel came from (see [`natureza` in SIGEF](#natureza-in-sigef))
- **Cache size may accumulate into GB** — see the "Filesystem cache" section

## Raw collection

`agrobr.bruto.coletar("acervo_fundiario", resource, uf=..., ...)` stores the state's ZIP as INCRA publishes it, with a single GET, no HEAD, cache or shapefile reading: `sigef_publico`, `sigef_privado`, `snci_publico`, `snci_privado` and `snci_brasil`. The manifest records the nature (`publico`, `privado`, or null for `snci_brasil`). A 404 becomes `ausente_na_fonte`, and the next resumption tries again; a 200 response that is not a ZIP is an error. Files per nature are neither merged nor compared to the Brasil file. The `assentamentos` resource stores the national `Assentamento Brasil.zip` (~50 MB) the same way, without a state: state and bbox are refused, the default `nome` is `brasil` and `selecao.edicao` is null (the file date is in `last-modified`). See the [raw collection API](../api/bruto.md) and the [manifest contract](../contracts/bruto.md).
