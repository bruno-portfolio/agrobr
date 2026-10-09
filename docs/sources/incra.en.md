# INCRA — Quilombola Territories

!!! warning "Breaking change — humanized phases"
    Previous versions accepted phases in a humanized format (`"Titulada"`,
    `"Em Titulacao"`, `"Decreto Publicado"`, `"RTID em Elaboracao"`,
    `"RTID Publicado"`). These values **never matched** the data from the
    CMR/FUNAI server (published in UPPERCASE) and the filter returned an
    empty result with no error. They now raise `InvalidParameterError`
    (a `ValueError` subclass). See the [canonical list](#valid-phases).

## Overview

| Item | Detail |
|------|--------|
| Provider | INCRA (layer published on the CMR/FUNAI GeoServer) and INCRA's quilombola page |
| Data | Quilombola territory perimeters, process progress table (PDF) and the links between them |
| Access | WFS 2.0.0 as JSON (`cmr.funai.gov.br/geoserver/ows`) and a PDF on `gov.br/incra` |
| Authentication | None |
| License | Federal government public data |
| Size | 445 perimeters in the WFS and 649 processes in the PDF (2026-09-22) |

| Function | Returns |
|----------|---------|
| `quilombolas()` | `DataFrame` with 22 columns, one row per published perimeter |
| `quilombolas_geo()` | `GeoDataFrame` with the same 22 columns + `geometry` (EPSG:4326) |
| `andamento_quilombola()` | `DataFrame` with the 15 columns of the "Andamento dos processos" table (requires `agrobr[pdf]`) |
| `vinculos_quilombolas()` | `DataFrame` with 46 columns linking perimeters and processes by NUP (requires `agrobr[pdf]`) |

## Usage Example

```python
import asyncio
from agrobr import incra

async def main():
    df = await incra.quilombolas()
    df = await incra.quilombolas(uf="BA", fase="TITULADO")
    gdf = await incra.quilombolas_geo(bbox=(-42, -15, -40, -13))
    andamento, meta = await incra.andamento_quilombola(return_meta=True)
    vinculos = await incra.vinculos_quilombolas()

asyncio.run(main())
```

## Perimeters (`quilombolas` and `quilombolas_geo`)

Layer `CMR-PUBLICO:lim_quilombolas_a`, queried through WFS 2.0.0/JSON sorted by
`cd_quilomb, nu_processo, no_comunidade`, with a count before and after the acquisition and
pages overlapping by one occurrence to detect changes during pagination.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `uf` | str \| None | None | State code; compared ignoring case and outer whitespace |
| `fase` | str \| None | None | One of the [7 selectors](#valid-phases), literal comparison |
| `bbox` | tuple \| None | None | `(minlon, minlat, maxlon, maxlat)` in **EPSG:4326**; the server preselects and agrobr confirms by intersection |
| `max_registros` | int \| None | 1500 | Cap on perimeters read; `None` removes the cap |
| `tamanho_pagina` | int \| None | 250 (tabular) / 10 (geo or with `bbox`) | At most 1000 (tabular) and 100 (geo or with `bbox`) |

The `uf` and `fase` filters are applied locally after the download: the server does not
honor `CQL_FILTER` on those fields. An invalid parameter raises `InvalidParameterError`
before any request. When `max_registros` cuts the population, a `UserWarning` says the
selection came from a remote prefix. If the complete read matches no community with the
requested `fase`, the result is empty with a `UserWarning` and an entry in `validation_warnings` listing the `ds_fase`
values read (a new INCRA label shows up there). `deterministic()` is not supported (the WFS publishes
no immutable edition).

### Columns

| Column | Source attribute | Type | Note |
|--------|------------------|------|------|
| `codigo` | `cd_quilomb` | Int64 | Null for 64% of the perimeters and 0 for 9 (2026-09-22); not a primary key |
| `nome` | `no_comunidade` | text | |
| `municipio` | `no_municipio` | text | |
| `uf` | `sg_uf` | text | Published text, not normalized |
| `area_ha` | `nu_area_ha` | float64 | Published hectares, not recomputed |
| `familias` | `nu_familia` | Int64 | |
| `fase` | `ds_fase` | text | See [phases](#valid-phases) |
| `titulado` | `st_titulad` | text | `T`/`F` (the source also publishes `t`/`f`), not converted to boolean |
| `data_publicacao` | `dt_publica` | datetime64[ns] | XSD date (`YYYY-MM-DD`) |
| `data_titulo` | `dt_titulo` | datetime64[ns] | Same |
| `feature_id` | feature id | text | Identifier received from the server; stability not proven |
| `regional` | `co_sr` | text | Regional superintendency (`SR-05`, …) |
| `processo` | `nu_processo` | text | NUP as published (may hold more than one or an atypical format) |
| `data_publicacao_2` | `dt_public1` | datetime64[ns] | XSD date |
| `responsavel` | `no_responsavel` | text | Responsible agency (INCRA, ITERPA, …) |
| `esfera` | `no_esfera` | text | Published text (`FEDERAL`, `Federal`, …) |
| `data_cadastro` | `dt_cadastro` | datetime64[ns, UTC] | XSD dateTime, in UTC (read as UTC when it has no offset); the source writes the same load time on every feature and it changes on each reload |
| `codigo_sipra` | `cd_sipra` | text | |
| `descricao` | `ds_descricao` | text | |
| `data_decreto` | `dt_decreto` | datetime64[ns] | XSD date |
| `tipo_levantamento` | `tp_levanta` | text | |
| `escala` | `nr_escalao` | text | Survey scale (`1:15.000`, …) |

agrobr validates each date against the XSD and returns it as `datetime64[ns]`; the registration
time comes as `datetime64[ns, UTC]`. The source uses `0001-01-01` as a "no date" placeholder in
`data_titulo` (3 perimeters) and `data_decreto` (2): it becomes `NaT` with no warning. Any other date
outside 1900–2099 (on 2026-09-08, `0205-01-28` and `2201-02-15` in `data_publicacao_2` and
`0222-11-11` in `data_titulo`, typos at the source) becomes `NaT` with a `UserWarning` and a warning in
`meta.validation_warnings`, with the column and the count; the rule also covers `data_cadastro`. Text
comes in the installed pandas default dtype (`str` on pandas 3, `object` on 2); null, zero, empty text
and the text `NULL` are kept as published.

### Geometry

`quilombolas_geo()` requests `srsName=EPSG:4326` and checks the CRS declared on each page.
Coordinates are returned as received: **no reprojection and no topology repair** — polygons
that are invalid at the source stay invalid. A null geometry becomes `None`; an empty
geometry stays empty. Requires `agrobr[geo]`.

### Valid phases

| Value | Meaning |
|-------|---------|
| `CCDRU` | Concession of Real Right of Use |
| `DECRETO` | Expropriation decree published |
| `PORTARIA` | Recognition ordinance published |
| `RTID` | Technical Identification and Delimitation Report |
| `TITULADO` | Territory with an issued title |
| `TITULO ANULADO` | Title annulled |
| `TITULO PARCIAL` | Partial titling |

On 2026-09-22 the layer also had 15 perimeters with a null phase and 1 with the text `INCRA`.
Those rows are returned by `quilombolas()` without `fase`, but no selector reaches them.

## Process progress (`andamento_quilombola`)

Reads the "Andamento dos processos — Quadro geral" PDF linked on INCRA's quilombola page.
Each row is a table record, in published order (a process may appear on more than one row);
the row count is reconciled with the total declared in the footer ("N processos com algum
tipo de andamento no INCRA").

| Column | Type | Content |
|--------|------|---------|
| `regional` | text | Label of the regional group drawn in the PDF (`SR(05)BA`, …) |
| `numero_publicado` | Int64 | Published position (1…N) |
| `processo`, `comunidade`, `municipio` | text | Cell text; line breaks become `\n` |
| `area_ha_texto`, `familias_texto` | text | Number in the published format (`2.629,0532`), not converted |
| `edital_rtid_1`, `edital_rtid_2`, `retificacao_edital_1`, `retificacao_edital_2`, `portaria`, `retificacao_portaria`, `decreto`, `titulo` | text | Published text: dates, several acts, notes (`Não precisa`, `Em Elaboração`, `**`) or empty |

Text partially clipped by the PDF grid is kept whole.

**Edition.** The edition is the PDF's **internal date** (the date printed above "Fonte:
INCRA-DQ"). The date in the file name is only the download locator. When they differ, a
`UserWarning` and `meta.validation_warnings` quote both dates, and `source_details` keeps
`publication.file_date` and `publication.internal_edition`. `edicao=` (a date or
`"YYYY-MM-DD"`) must match the internal date; an edition that is no longer published raises
`InvalidParameterError` naming the current one. The page links a single PDF: on 2026-09-22
the file named `08_06_2026` carried the content of 2026-09-03 (649 processes). A redirect (3xx) of the page or of
the PDF raises `SourceUnavailableError` ("Redirecionamento administrativo não demonstrado").

The administrative download has local caps of 16 MiB per response and 32 MiB in total.
Exceeding either raises `ResourceLimitError`, with attempt receipts in `resources`,
without retrying the request for that reason.

## Links (`vinculos_quilombolas`)

Combines `quilombolas()` (whole population, no filters) and `andamento_quilombola()` through
the literal NUP reference (`NNNNN.NNNNNN/YYYY-DD`) found in `processo` on both sides. One row
per pair of occurrences (cartesian product when the NUP repeats) plus one row for each
reference without a pair and for each cell without a recognizable NUP.

| `estado_vinculo` | Meaning |
|------------------|---------|
| `vinculo_exato` | The same NUP appears in the perimeter and in the process table |
| `sem_referencia_administrativa` | Perimeter NUP missing from the PDF |
| `sem_referencia_geografica` | PDF NUP missing from the layer |
| `referencia_nao_reconhecida` | Cell text outside the NUP pattern (no punctuation repair) |
| `referencia_ausente` | Empty or null cell |

The 46 columns are the 9 relation columns (`estado_vinculo`, `referencia_tipo`,
`referencia_literal`, occurrence positions and counts, `referencia_repetida`) followed by the
22 perimeter columns prefixed `perimetro_` and the 15 progress columns prefixed
`administrativo_`. A shared NUP does not prove territorial identity. `max_vinculos` (default
50,000) stops with `ResourceLimitError` if the expansion exceeds the cap; `max_vinculos=None` removes
the row cap, and the memory cap remains. The composite's `validation_warnings` carries the link warning followed by
the warnings of both sources, each with its prefix (`incra_geoserver: …`, `incra_andamento_pdf: …`). On 2026-09-22:
817 rows, 286 exact links.

## Raw collection

`agrobr.bruto.coletar("incra", "quilombolas", ...)` keeps the original GeoJSON response of the CMR WFS for layer `CMR-PUBLICO:lim_quilombolas_a`, in the native CRS (`EPSG:4674`) and with every attribute, without the `propertyName`, `srsName` and ordering that `quilombolas()` and `quilombolas_geo()` use. The collection is always national (state and bbox refused) and comes in a single page of up to 1,000 features, because the layer has no unique, sortable field to paginate by; that is why `tamanho_pagina` is not accepted. It closes when the `hits` counts before and after equal the features received and every `feature.id` is present and distinct; a count above 1,000 is a `ParseError`. See the [raw collection API](../api/bruto.md) and the [manifest contract](../contracts/bruto.md).

## Limitations

- Perimeters and the PDF are acquired at different moments, with no joint snapshot.
- `codigo`, `processo` and `feature_id` are not primary keys; repeated occurrences are kept.
- The WFS count and the PDF change without notice; agrobr records hashes and dates of every resource in `MetaInfo`.

## Invalid geometry

Geometry from the `_geo` outputs is returned as the source publishes it, without repair; an invalid geometry raises a warning and a count in `MetaInfo`. See [Published geometries](../guides/normalizacao.en.md#published-geometries).
