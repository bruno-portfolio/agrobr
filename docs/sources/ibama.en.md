# IBAMA — Environmental Embargoes

## Overview

| Item | Detail |
|------|---------|
| Provider | IBAMA (Instituto Brasileiro do Meio Ambiente e dos Recursos Naturais Renovaveis) |
| Data | Embargo terms for environmental infractions (IBAMA enforcement system) |
| Access | CSV of the "Fiscalização - termo de embargo" dataset on IBAMA's open data portal |
| Format | UTF-8 CSV with BOM, `;`, quoted fields, ~208 MB uncompressed (the source publishes no compressed version), WKT geometries |
| Authentication | None |
| License | "Outra (Aberta)" (other, open) in the catalog; federal open data, free use with source credit ([details](../licenses.md#ibama)) |
| Records | 116,332 terms (edition of September 23, 2026), daily update |

> Since 2.0.0 agrobr reads the dataset's current resource. The old ZIP
> (`dadosabertos.ibama.gov.br/dados/SIFISC/termo_embargo/termo_embargo/termo_embargo_csv.zip`) stopped on May 3, 2026
> and is no longer in the catalog. The catalog link to the CSV has a path error (`dados/TERMOS/TERMO_EMBARGO/...`,
> 404); the published file is at the address below.

## Access

| Parameter | Value |
|-----------|-------|
| URL | `stibamadadosabertosprd.blob.core.windows.net/dados-abertos/dados/TERMOS_DE_EMBARGO/TERMO_EMBARGO/termo_de_embargo.csv` |
| Catalog | [dadosabertos.ibama.gov.br/dataset/fiscalizacao-termo-de-embargo](https://dadosabertos.ibama.gov.br/dataset/fiscalizacao-termo-de-embargo) |
| Update | Daily; the edition read (`ULTIMA_ATUALIZACAO_RELATORIO`, Brasília time) goes to `meta.source_details["ultima_atualizacao_relatorio"]` |
| Filters | `uf` and `bbox` applied client-side after the download |

## Usage Example

```python
import asyncio
from agrobr import ibama

async def main():
    # All embargoes in Brazil
    df = await ibama.embargos()

    # Filter by state
    df = await ibama.embargos(uf="MT")

    # With WKT geometry (requires geopandas — [geo] extra)
    gdf = await ibama.embargos_geo(uf="RR")
    gdf = await ibama.embargos_geo(bbox=(-56, -16, -54, -14))

    # With metadata (source edition in meta.source_details)
    df, meta = await ibama.embargos(return_meta=True)

    # Polars
    df = await ibama.embargos(as_polars=True)

    # Skip the cache and download again
    df = await ibama.embargos(use_cache=False)

asyncio.run(main())
```

## Columns

| Column | Type | Description |
|--------|------|-----------|
| seq_tad | str | Term identifier in the enforcement system (empty in the 2,862 AIe terms) |
| numero_tad | str | Embargo Term number (numeric until Oct 7, 2019; alphanumeric in AIe) |
| data_embargo | datetime | Date and time the term was issued, Brasília time without time zone |
| num_processo | str | Administrative process number |
| descricao | str | Embargo description, free text from the source |
| codigo_municipio | str | Municipality IBGE code (7 digits; 2 terms carry `431173 `, 6 digits and a space, as in the source) |
| municipio | str | Municipality |
| uf | str | State (abbreviation) |
| latitude / longitude | float | Reference point of the term, as in the source: 94,655 terms with a point; 4,416 with both zeroed (`0` = not informed) and 17,261 with an empty coordinate; 387 points outside Brazil's bounding rectangle |
| area_embargada_ha | float | Embargoed area in hectares (4 decimal places with a comma in the source; an area with a dot raises `ParseError`) |
| nome_imovel | str | Property name |
| status | str | Term status: Lavrado, Cancelado, Substituído por outro, Excluído (empty in AIe terms) |
| cancelado | bool | `SIT_CANCELADO = S` |
| data_desembargo | datetime | Date the disembargo was recorded; NaT = no disembargo recorded (the source sets `SIT_DESEMBARGO = S` on exactly these terms) |

`embargos_geo` adds `geometry` (Polygon/MultiPolygon) read from the WKT in the CSV itself — only records with a
polygon (58,984 in the September 23, 2026 edition). The CSV declares no SRID; the origin database in IBAMA's GIS
(`adm_embargos_ibama_a`) is in SIRGAS 2000 (EPSG:4674), and agrobr labels EPSG:4326 without reprojecting (the EPSG
SIRGAS 2000 → WGS 84 transformation is a null transformation).

## Particularities

- **Personal data**: the source CSV carries the name and CPF/CNPJ of the embargoed party; agrobr tables do not
  expose those fields (project policy). `descricao` is free text from the source. The 1-hour cache, however, keeps
  the whole file, with those columns, and the first write warns with `UserWarning`. To keep personal data off disk,
  use `use_cache=False`, which writes nothing, or delete the cache's `ibama/` folder after the query. Anyone who
  needs the fields for compliance can download the raw CSV from the source.
- **Dirty dates in the source**: there are dates outside any plausible range (years 1667, 2063, 2080, 2090 and 2925).
  Under agrobr's date rule ([Normalization](../guides/normalizacao.en.md#source-dates)), the same on pandas 2 and 3, a
  date with a year outside 1900–2099 (1667 and 2925) and one on a day after the file's own edition
  (`ULTIMA_ATUALIZACAO_RELATORIO`; in the 23/09/2026 edition, 2063, 2080 and 2090) become `NaT`: a term cannot be dated
  after the file that publishes it. The query warns with a `UserWarning` and in `meta.validation_warnings`, with the column
  and the count. Both date columns come out as `datetime64[ns]`.
- **bbox**: `embargos(bbox=...)` filters by the term's reference point (lat/lon); `embargos_geo(bbox=...)` filters by
  polygon intersection with the box. They can differ: in the example bbox, 9 terms have the point inside and the
  polygon outside, and 4 have the polygon inside and the point outside or missing.
- **Geometries**: 1 unreadable WKT (open ring) is dropped with a log warning; 129 polygons with invalid topology are
  returned as published.
- **1-hour cache**: the CSV (~208 MB) is kept as `ibama/termo_embargo.csv` in the cache folder, with a
  manifest (SHA-256 and collection time), and reused for 1 hour from collection. Consecutive calls with
  different filters download the file only once, even when they run together. `MetaInfo` carries
  `from_cache=True` and, in `fetched_at`, the collection time rather than the call time. `use_cache=False`
  downloads again without reading or writing the cache. A corrupted or expired file is downloaded again. An
  expired file is not deleted: it stays on disk until the next collection overwrites it.
- **Geo with no filter**: `embargos_geo()` without `uf`/`bbox` parses WKT for all of Brazil
  (~4 s); a warning is emitted. With `bbox`, the WKT of the whole selection is read.

## Limitations

- Geometry present in part of the records (embargoes without a polygon are left out of the geo)
- The whole file (~208 MB) is downloaded and filters are local; the 1-hour cache avoids repeating the download
- License: see [Licenses](../licenses.md#ibama)

## Raw collection

`agrobr.bruto.coletar("ibama", "termos_embargo", ...)` stores the whole embargo terms CSV (~209 MB) as IBAMA
publishes it, with no cache and without reading the columns; `embargos()` and `embargos_geo()` keep the 1-hour cache.
IBAMA publishes no edition: `selecao.edicao` is `null`, and the file date is in `cabecalhos` (`last-modified`). State
and bbox are refused.

**Personal data:** the file contains the names and CPF/CNPJ of the embargoed individuals and companies, columns the
table functions do not read. Whoever stores the raw file stores personal data and must process it in accordance with
the LGPD. See the [raw collection API](../api/bruto.md) and the [manifest contract](../contracts/bruto.md).
