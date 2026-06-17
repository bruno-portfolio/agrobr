# Land Registry — SIGEF, SNCI and Settlements (INCRA)

!!! warning "License `nc` — commercial use prohibited"
    The data from INCRA's Land Registry is public-use data with a commercial-use restriction.
    The first call emits a `UserWarning` reminding you of this restriction.

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
| **SIGEF** | 15/27 (AC, AL, AM, BA, ES, GO, MA, MG, MS, MT, PA, PR, SC, SP, TO) | 8-687 MB per state | Per state |
| **SNCI** | 10/27 (BA, GO, MG, MS, MT, PA, PI, SC, SP, TO) | 0.6-22 MB per state | Per state |
| **Settlements** | Brazil-wide single | 48 MB | Full Brazil, client-side state filter |

States not listed raise `SourceUnavailableError` with the list of available ones. The absence reflects upstream INCRA data, not an agrobr bug.

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

    # SNCI — certified parcels pre-2013
    df = await acervo_fundiario.snci("GO")
    gdf = await acervo_fundiario.snci_geo("MT")

    # Settlements — Brazil-wide single, uf optional
    df = await acervo_fundiario.assentamentos()             # all states
    df = await acervo_fundiario.assentamentos(uf="GO")      # client-side filter
    gdf = await acervo_fundiario.assentamentos_geo(uf="MG")

asyncio.run(main())
```

## Filesystem cache

Downloaded files are stored in `~/.agrobr/cache/acervo_fundiario/{tema}/{UF}.zip` with a `meta.json` alongside containing `last_modified`, `etag`, `sha256`, `size_bytes`, `fetched_at`, `source_url`.

Revalidation uses the server's `Last-Modified` header: the 2nd call performs a HEAD (~50ms) and reuses the cache if the file has not changed.

**Potential cache size:**

- SIGEF full Brazil (15 states) ≈ 2.4 GB (largest: MG=687 MB, SP=322 MB, PR=287 MB)
- SNCI full Brazil (10 states) ≈ 84 MB
- Settlements Brazil = 48 MB

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
| geometry | Polygon | Geometry (only in `_geo`) |

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
| geometry | Polygon | Only in `_geo` |

## Filters

### `bbox`

Applied by `pyogrio` while reading the shapefile (pre-read spatial filter, 4-9x less RAM/time than filtering `gdf.cx[]` after loading everything).

```python
gdf = await acervo_fundiario.sigef_geo("MG", bbox=(-44, -18, -43, -17))
```

### `uf` in settlements

The settlements dataset is Brazil-wide single — the `uf` filter is client-side, normalizing the `uf` column (`.str.upper().str.strip()`) and comparing.

Upstream INCRA data has known invalid states (`MB`=501 rows, `SM`=200 rows, `12`, `'ma'`). The parser does not silently drop these rows — a `acervo_fundiario_dirty_uf_data` warning log reports the counts. The `uf="MG"` filter returns only rows with valid `MG`; the invalid ones remain in the DataFrame when `uf=None`.

## Limitations

- **12 states lack SIGEF** (AP, CE, DF, PB, PE, PI, RJ, RN, RO, RR, RS, SE) — no upstream record
- **17 states lack SNCI** — only Center-West/Southeast/South/part of the North are covered
- **Invalid SSL cert** on the server — agrobr uses relaxed TLS (`check_hostname=False`, `verify_mode=CERT_NONE`). The server is a public gov.br one.
- **No private/public distinction** — the shapefile has no type field (that was a distinction of the legacy WFS)
- **Cache size may accumulate into GB** — see the "Filesystem cache" section
