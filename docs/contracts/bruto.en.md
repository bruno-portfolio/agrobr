# Raw collection

Raw mode stores the sources' original files and responses, with provenance and integrity
checks. The asynchronous API is `agrobr.bruto.coletar`; the same operation is available
through `agrobr.sync.bruto.coletar`. It does not require the `geo` extra.

The public `manifesto.jsonl` contract is **1.0.0**, independent of the library version
and table contracts. It describes acquisition, not the schema of attributes published
by a source. Original formats are ZIP, CSV, Esri JSON, GML and GeoJSON.

## API and resources

```python
from os import PathLike
from typing import Literal

from agrobr import bruto

async def coletar(
    fonte: str,
    recurso: str,
    *,
    destino: str | PathLike[str],
    nome: str | None = None,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    bbox_crs: Literal["EPSG:4674", "EPSG:4326"] = "EPSG:4674",
    tamanho_pagina: int | None = None,
    compactar: bool = True,
    retomar: bool = False,
    limites: bruto.LimitesBrutos | None = None,
) -> bruto.ColetaBruta: ...
```

`fonte` and `recurso` are the exact identifiers in the table. They are not URLs or free-form
names. `bbox_crs` accepts only `EPSG:4674` and `EPSG:4326`.

| Source | Resource | Selection | Format / pagination identity |
|---|---|---|---|
| `ana` | `massas_dagua` | UF and/or bbox required | `esri_json` / `FID` |
| `cnuc` | `ucs` | UF, bbox, both or Brazil; mandatory `limite=uc` filter | `gml` / `cd_cnuc` |
| `ibge` | `malha_municipal` | UF, bbox, both or Brazil | `geojson` / `cd_mun` |
| `ibge` | `areas_urbanizadas` | bbox or Brazil; UF is not accepted | `geojson` / `fid` |
| `funai` | `terras_indigenas` | Brazil; UF and bbox are not accepted; default `tamanho_pagina` 20 | `geojson` / `gid` |
| `funai` | `terras_indigenas_pontos` | Brazil; UF and bbox are not accepted | `geojson` / `gid` |
| `incra` | `quilombolas` | Brazil; UF, bbox and `tamanho_pagina` are not accepted; single page | `geojson` / `feature.id` |
| `acervo_fundiario` | `sigef_publico` | UF required; bbox is not accepted | `zip` |
| `acervo_fundiario` | `sigef_privado` | UF required; bbox is not accepted | `zip` |
| `acervo_fundiario` | `snci_publico` | UF required; bbox is not accepted | `zip` |
| `acervo_fundiario` | `snci_privado` | UF required; bbox is not accepted | `zip` |
| `acervo_fundiario` | `snci_brasil` | UF required; that state's Brasil file | `zip` |
| `acervo_fundiario` | `assentamentos` | Brazil; UF and bbox are not accepted | `zip` |
| `cnuc` | `cadastro` | Brazil; UF and bbox are not accepted | `csv` |
| `ibama` | `termos_embargo` | Brazil; UF and bbox are not accepted | `csv` |
| `ibge` | `malha_municipal_zip` | Brazil; UF and bbox are not accepted | `zip` |
| `ibge` | `areas_urbanizadas_zip` | Brazil; UF and bbox are not accepted | `zip` |
| `sfb` | `cnfp` | Brazil; UF and bbox are not accepted; the whole country needs `tamanho_pagina=20` and a 24 MiB `max_bytes_pagina` | `esri_json` / `fid` |
| `sicar` | `imoveis` | UF required; bbox optional | `geojson` / `feature.id` |

UF accepts a state abbreviation, normalized to uppercase. Combining UF and bbox means
intersection. The bbox is `(minx, miny, maxx, maxy)`, always longitude/latitude at input,
with finite numbers, `-180 <= minx < maxx <= 180` and `-90 <= miny < maxy <= 90`.
It cannot cross the antimeridian. The adapter serializes the axis order required by the
protocol; it does not reproject the response. Without a bbox, `bbox_crs` is `null`
in the manifest.

`nome` identifies the selection: it defaults to the UF, or `brasil` without a selection.
With a bbox, `nome` is mandatory, including when a UF is also provided. It accepts
`[A-Za-z0-9][A-Za-z0-9_-]{0,63}`; Windows device names such as `CON`, `NUL` and `COM1`
are rejected case-insensitively. Names differing only in case collide on every platform.
The name does not apply a filter.

`tamanho_pagina=None` resolves to **100** for paginated queries, except for `funai`/`terras_indigenas`
(**20**) and `incra`/`quilombolas` (a single page, which does not accept the parameter). An explicit value must
be an integer from 1 to 1,000, never a boolean. For files (ZIP or CSV), it must be `None`. It divides
the acquisition; it does not limit the total feature count. `compactar=True` applies
gzip to pages and controls, with `mtime=0` and no original filename in the header;
files (ZIP or CSV) receive no additional compression. `compactar=False` preserves the same bodies
without gzip.

Municipal boundaries use layer `CGMAT:qg_2025_030_munic`, edition 2025; urbanized areas
use `CGEO:AU_2026_AreasUrbanizadas2022_Brasil`, edition 2022. The edition is recorded in
`selecao.edicao`, not inferred from the collection year. Raw mode requests every
attribute and the geometry. ANA explicitly requests `outSR=4674` and
`returnGeometry=true`; CNUC and IBGE do not request an output CRS and check the
declared native CRS, `EPSG:4674`. An incompatible CRS fails the query. ZIP preserves
its PRJ; `crs=null` means the file's CRS was not verified.

SICAR (`sicar`/`imoveis`) uses the state's layer, `sicar:sicar_imoveis_<uf>`, requests no output CRS and
checks the native CRS, `EPSG:4674`. It keeps every published version of a property and counts by `feature.id`,
not by `cod_imovel`. Because SICAR publishes no sortable identifier, pages use
`sortBy=cod_imovel A,dat_criacao A`, and a collection only closes `ok` when the pair (`cod_imovel`,
`dat_criacao`) strictly increases across the whole collection: two versions with the same `dat_criacao`, a pair
out of order or a date in another format fail the query. An `ok` proves the order of the features received in
that collection, not that the pair is unique across the whole database.

FUNAI's Indigenous lands (`funai`/`terras_indigenas`, layer `Funai:tis_poligonais`, and
`funai`/`terras_indigenas_pontos`, layer `Funai:tis_pontos`) are always national: UF and bbox are rejected and
`selecao.edicao` is `null`, because the layers publish no edition. Pages use `sortBy=gid`, without `srsName` or
`propertyName`, and a collection only closes `ok` when `gid` is an integer that strictly increases across the whole
collection. The GeoServer rejects `resultType=hits` (HTTP 403): the counts before and after are the `numberMatched`
of a GeoJSON GetFeature with `count=1` in the same order, kept as a `json` control. The checked native CRS is
`EPSG:4674`. The polygons are large: on 2026-10-04 there were 665 features and 49.3 MB, the largest at 2.95 MB; with
the default of 20 per page, the largest page had 5.6 MB, and 40 or more per page exceed the 8 MiB page limit. The
points (163 features, about 100 kB) keep the default of 100.

Quilombola territories (`incra`/`quilombolas`) come from layer `CMR-PUBLICO:lim_quilombolas_a`, INCRA's, published
on the CMR GeoServer, always national: UF and bbox are rejected and `selecao.edicao` is `null`. The layer has no
unique or sortable attribute, so it is not paginated: `tamanho_pagina` is not accepted and the collection is a
single page (`count=1000&startIndex=0`, without `sortBy` and without an output CRS), recorded with
`opcoes.tamanho_pagina=1000`. This is the exception to checking order by field: `ok` requires the count
(`resultType=hits`) before and after to equal the features received and the page's `numberMatched`, with every
`feature.id` present and distinct (`cobertura.campo_id="feature.id"`). A count above 1,000 fails the collection with
`ParseError` before the page is requested; a page above `max_bytes_pagina`, with `ResourceLimitError`. The checked
native CRS is `EPSG:4674`; on 2026-10-04 there were 445 features in a 6.7 MB page.

CNFP (`sfb`/`cnfp`) is layer `Hosted/CNFP_v19_03_retificado_17072025/FeatureServer/9` of SFB's ArcGIS, always
national: UF and bbox are rejected. The edition in `selecao.edicao` is `20250717`, the rectification date in the service
name; the layer's last edit only shows in the responses' `etag` (ArcGIS `lastEditDate`, in milliseconds). agrobr
counts, reads the official `fid` list and requests pages by `fid` range, with `orderByFields=fid`, every attribute and
the geometry, without `outSR`. Each page must bring exactly the range's `fid` values, in ascending order, and the
declared native CRS (`wkid` 102100, `latestWkid` 3857), recorded as `EPSG:3857`. The polygons are large: on 2026-10-04
there were 20,829 features, the largest at 5.52 MB, and 100-feature pages reached 37.7 MB. No page size
fits the default limits (3 or more features per page exceed 8 MiB; 2 or fewer exceed `max_paginas`), and the default
call stops early, on the 4th page, with a `ResourceLimitError` that states the page, the `fid` range and
`max_bytes_pagina`. For the whole country, use `tamanho_pagina=20` and
`limites=bruto.LimitesBrutos(max_bytes_pagina=24 * 1024**2)`: the 2026-10-04 collection closed `ok` with 1,042 pages (980 MB),
the largest 19.3 MB, in 37.5 minutes.

The five national files (`acervo_fundiario`/`assentamentos`, `cnuc`/`cadastro`, `ibama`/`termos_embargo`,
`ibge`/`malha_municipal_zip` and `ibge`/`areas_urbanizadas_zip`) come in a single GET with no selection: UF and bbox are rejected and the default
`nome` is `brasil`. The body is stored as the source publishes it, including the CSV encoding, separator and line
endings. Before publishing it, agrobr checks only the beginning: the ZIP signature or, for CSV, the first line with the
columns that identify the table (`Código UC` for CNUC; `SEQ_TAD` and `NUM_TAD` for IBAMA). An error HTML page, an XLSX
spreadsheet labeled as CSV or an empty body in HTTP 200 fail the collection with `ParseError`. The edition is in
`selecao.edicao`: `2025` for municipal boundaries (`BR_Municipios_2025.zip`), `2022` for urbanized areas
(`AreasUrbanizadas2022_Brasil.zip`, the same release as the WFS layer), `202607` (year and month) for the CNUC
registry and `null` for IBAMA embargo terms and INCRA settlements, which publish no edition; the file date is in
`cabecalhos` (`last-modified`). Settlements are the Land Registry's national `Assentamento Brasil.zip`, not a state's
file; the other Land Registry resources remain per state.

The CNUC registry is the CSV of the July 2026 edition in MMA's open data catalog. Before the GET, agrobr checks in the
catalog (`package_show?id=unidadesdeconservacao`) that the `CNUC_2026_07` CSV resource points to the URL pinned in the
library. A different URL (edition republished or withdrawn) fails the collection with `ParseError`, and an unavailable
catalog with `SourceUnavailableError`. The catalog request counts toward the call's budget, deadline and attempts
(capped at `max_bytes_pagina`) and is not stored in the manifest; if it fails, `erro.url` is the catalog's. A new
registry edition ships with a new agrobr version.

The IBAMA embargo terms CSV contains the names and CPF/CNPJ of the embargoed individuals and companies. It is personal
data: store and process the file in accordance with the LGPD.

The `ColetaBruta` return value has three required attributes:

| Attribute | Python type | Example / meaning |
|---|---|---|
| `manifesto` | `pathlib.Path` | Absolute path to `destino/manifesto.jsonl` |
| `entrada` | `bruto.RecursoBruto` | Pydantic model of the entry; `entrada.status` and `entrada.model_dump(mode="json")` |
| `reutilizado` | `bool` | `True` only when `retomar=True` reused a verified `ok` resource without network access |

The function returns for `ok` and `ausente_na_fonte`. Acquisition failures record
`erro` when possible and raise the exception described below. There is no successful
partial return.

## Representation and layout

The manifest uses UTF-8 without BOM, LF and one complete JSON line per resource,
including a final LF. It contains no comments, blank lines, header or study-area entry.
The key is `(fonte, recurso, nome)`, with a single current entry.
Line order has no meaning. The order of `paginas` does: contiguous numbers starting at
1, in acquisition order, without sorting or modifying feature bytes.

```text
coleta/
  manifesto.jsonl
  ana/massas_dagua/AL/<coleta_id>/p000001.esri.json.gz
  ana/massas_dagua/AL/<coleta_id>/controles/c000001.json.gz
  cnuc/ucs/brasil/<coleta_id>/p000001.gml.gz
  ibge/malha_municipal/AL/<coleta_id>/p000001.geojson.gz
  ibge/areas_urbanizadas/area_teste/<coleta_id>/p000001.geojson.gz
  acervo_fundiario/snci_publico/AL/<coleta_id>/original.zip
  cnuc/cadastro/brasil/<coleta_id>/original.csv
  .bruto/lock
  .bruto/<coleta_id>/checkpoint.json
  .bruto/<coleta_id>/diagnostico/
```

Every `arquivo` field contains a path relative to `destino`, using `/`, never an
absolute path, `..` or a link/junction escaping the destination. `coleta_id` is opaque:
do not extract a date from it. The `.gz` suffix exists only with `compressao="gzip"`.
Controls use their body's extension, JSON or XML; the `arquivo`-mode file is `original.zip` or
`original.csv`, matching `formato`. Readers must use `formato` and
`compressao` instead of guessing from extensions.
`.bruto` is private state with no public schema; it is not a source of consumable resources.

Bodies are written to `.part` temporary files and published after closing and verification.
Only then is the manifest replaced atomically. A failure may leave orphan artifacts,
but must never publish an `ok` entry pointing to a partial body.
Replacing an entry preserves the other entries. There is an exclusive destination lock,
effective across threads and processes and released by the operating system when the
process exits. Another writer immediately raises `ResourceLimitError` rather than queuing.

`bytes` and `sha256` describe the **body after HTTP Content-Encoding decoding and before
local gzip**. They are not connection bytes, a hash of extracted ZIP contents or a hash
of local gzip. Text encoding, whitespace, properties, line endings, attributes and
geometries remain unchanged. `bytes_armazenados` is the on-disk artifact size; it may
exceed `bytes` for small compressed bodies. Reading, decompressing when indicated and
computing SHA-256 must reproduce `sha256`. An ETag does not replace this hash.

## Schema 1.0.0: resource entry

**Every key in the tables is required.** `null` is an allowed value only where stated,
not permission to omit the key. Empty objects and lists are `{}` and `[]`.
Integers do not accept booleans; numbers do not accept NaN or infinity.
`UTC` in the tables means an RFC 3339 string ending in `Z`, with an optional fraction of a second of
1 to 6 digits, such as `2026-10-02T20:00:00Z` or `2026-10-02T20:00:00.125Z`. A hash is a SHA-256 string of 64 lowercase hexadecimal digits.

| Field | JSON type | Required | Example / rule |
|---|---|---|---|
| `schema_version` | string | Yes | `"1.0.0"` |
| `tipo` | string literal | Yes | `"recurso"` |
| `fonte` | string | Yes | `"ana"`; registered identifier from the API table |
| `recurso` | string | Yes | `"massas_dagua"`; allowed for that source |
| `nome` | string | Yes | `"AL"` |
| `consulta_id` | hash | Yes | Opaque normalized query identity; not a data hash |
| `coleta_id` | nonempty string | Yes | `"exemplo-ana-001"`; distinguishes whole-resource attempts |
| `status` | enum | Yes | `"ok"`, `"erro"` or `"ausente_na_fonte"` |
| `inicio` | UTC | Yes | Start of the resource attempt |
| `fim` | UTC | Yes | End of the attempt; `inicio <= fim` |
| `registrado_em` | UTC | Yes | Entry publication; `fim <= registrado_em` |
| `url_solicitada` | HTTPS URL string | Yes | File URL; for pagination, endpoint URL without query string |
| `url` | HTTPS URL string | Yes | Final file GET URL; for paginated queries, the same logical URL as `url_solicitada` |
| `parametros` | string → string object | Yes | `{"outFields":"*","outSR":"4674"}`; every logical parameter, excluding pagination; `{}` for files |
| `selecao` | Selection object | Yes | Selection and publication, defined below |
| `opcoes` | Options object | Yes | Effective options and limits for this attempt |
| `modo` | enum | Yes | `"arquivo"` or `"paginado"` |
| `formato` | enum | Yes | `"zip"`, `"csv"`, `"esri_json"`, `"gml"` or `"geojson"`; `zip` and `csv` only for files |
| `crs` | string or null | Yes | `"EPSG:4674"` when verified; `null` when unverified |
| `crs_evidencia` | CRSEvidence object or null | Yes | `null` exactly when `crs=null` |
| `http_status` | integer 100–599 or null | Yes | Last file GET, e.g. `200` or `404`; `null` for paginated queries or no response |
| `http_inicio` | UTC or null | Yes | Start of the last file GET; `null` for paginated queries or a GET not started |
| `http_fim` | UTC or null | Yes | End of that GET; `null` under the same conditions |
| `arquivo` | path string or null | Yes | `"acervo_fundiario/snci_publico/AL/exemplo-001/original.zip"` |
| `sha256` | hash or null | Yes | Complete file hash; `null` for paginated queries or no valid file |
| `bytes` | integer >= 0 or null | Yes | `11900`; original size, not a feature count |
| `bytes_armazenados` | integer >= 0 or null | Yes | Equals `bytes` for files (ZIP or CSV), without additional compression |
| `compressao` | enum | Yes | `"nenhuma"` for files and at the paginated entry root; gzip is recorded on each artifact |
| `cabecalhos` | string → string object | Yes | Headers of the last file GET, including 404; `{}` for paginated queries |
| `paginas` | list of Page | Yes | `[]` for files and zero-feature paginated selections |
| `controles` | list of Control | Yes | Count, ID and/or CRS responses; `[]` if none |
| `feicoes` | integer >= 0 or null | Yes | Declared total before pagination; `null` for files or unknown counts |
| `cobertura` | Coverage object | Yes | Verification result, not a transactional guarantee |
| `erro` | Error object or null | Yes | `null` only for `ok` |
| `avisos` | list of strings | Yes | `[]` when empty; never a substitute for an integrity failure |
| `agrobr_version` | string | Yes | `"2.0.0"`; version that acquired the bodies |

The four fields `arquivo`, `sha256`, `bytes` and `bytes_armazenados` are either all
populated or all `null`. For `modo="arquivo", status="ok"` they are populated and
`http_status=200`. For paginated entries they are always `null`: there is no concatenated
file or implicit aggregate hash. At the paginated entry root, URLs describe the logical
request; executed URLs, including redirects and pagination, are in pages and controls.


### Query identity

`consulta_id` is SHA-256 of a UTF-8 canonical JSON object with exactly these keys:
`fonte`, `recurso`, `nome`, `url_solicitada`, `parametros`, `selecao`, `modo`, `formato`,
`tamanho_pagina` and `compactar`. The first eight values come from the entry; the last two
come from `opcoes`. Serialization uses `ensure_ascii=False`, `sort_keys=True`,
`separators=(",", ":")` and `allow_nan=False`, without a final newline.
Normalization converts UF to uppercase, bbox coordinates to floats (negative zero to
`0.0`), fills selection keys with `null` when inapplicable, and resolves effective options.
Names retain their spelling; a different spelling differing only in case is a collision.
Limits, timestamps, library version and `retomar` are excluded.

For pagination, top-level URLs identify the endpoint without a query string;
`parametros` contains the complete logical query. Page/control URLs include the actual
query string. A page's `parametros` records its original request, including when redirected;
`url` records the final response URL.

### Selection, options and CRS evidence

| Object.field | Type | Required | Example / rule |
|---|---|---|---|
| `selecao.uf` | string or null | Yes | `"AL"` |
| `selecao.bbox` | list of four numbers or null | Yes | `[-48.1,-16.1,-47.9,-15.9]` |
| `selecao.bbox_crs` | enum or null | Yes | `"EPSG:4674"`, `"EPSG:4326"`; `null` without bbox |
| `selecao.camada` | string or null | Yes | `"CGMAT:qg_2025_030_munic"`; `null` for files |
| `selecao.edicao` | integer or null | Yes | `2025`, or `202607` (year and month) for the CNUC registry; `null` when no edition is identified by the source |
| `selecao.natureza` | enum or null | Yes | `"publico"`, `"privado"`; `null` for SNCI Brasil and other sources |
| `opcoes.tamanho_pagina` | integer 1–1000 or null | Yes | `100`; `null` for files |
| `opcoes.compactar` | boolean | Yes | `true`; effective `false` for files |
| `opcoes.limites` | Limits object | Yes | All effective values defined in Limits |
| `crs_evidencia.tipo` | enum | Yes, in object | `"pagina"`, `"controle"` or `"prj"` |
| `crs_evidencia.arquivo` | path | Yes, in object | An artifact belonging to this entry |
| `crs_evidencia.localizador` | nonempty string | Yes, in object | `"/spatialReference/wkid"`, GML/XML XPath or PRJ member name |
| `crs_evidencia.valor` | nonempty string | Yes, in object | `"4674"` or the URN/WKT actually read |

Evidence points to preserved bytes, not an assumption based on the endpoint.
For a query without features, `crs=null` is allowed if no control verified its CRS.
For a query with features, all pages must declare the expected CRS and agree; the
entry may reference the first piece of evidence. Reading PRJ is optional, subject to an
expansion limit and does not extract the ZIP.

### Page and control

Every Page or Control contains **all** of these common fields:

| Field | Type | Required | Example / rule |
|---|---|---|---|
| `numero` | integer >= 1 | Yes | `1`; sequential within its own list |
| `url_solicitada` | HTTPS URL | Yes | Full GET URL, including the query actually sent |
| `url` | HTTPS URL | Yes | Final response URL |
| `parametros` | string → string object | Yes | Effective original request query, including `count`/`startIndex` or FID range |
| `inicio` | UTC | Yes | Start of the request that supplied the body |
| `fim` | UTC | Yes | End of body reading; within the resource interval |
| `http_status` | integer literal | Yes | `200`; error bodies go to private diagnostics |
| `arquivo` | path | Yes | `"ibge/malha_municipal/AL/exemplo-001/p000001.geojson.gz"` |
| `sha256` | hash | Yes | SHA-256 of the complete original body |
| `bytes` | integer >= 0 | Yes | `73038` |
| `bytes_armazenados` | integer >= 0 | Yes | Stored body size, including local gzip |
| `compressao` | enum | Yes | `"gzip"` or `"nenhuma"` |
| `formato` | enum | Yes | Page: `esri_json`/`gml`/`geojson`; control: `json`/`xml` |
| `cabecalhos` | string → string object | Yes | `{"etag":"W/\"abc\""}` or `{}` |

In addition to the common fields, a Page contains:

| Field | Type | Required | Example / rule |
|---|---|---|---|
| `paginacao` | discriminated object | Yes | One of the two forms below |
| `feicoes_recebidas` | integer >= 0 | Yes | Features in the envelope, before any processing |
| `ids_distintos` | integer >= 0 | Yes | Distinct IDs within this page, not the accumulated count |
| `total_declarado` | integer >= 0, literal `"unknown"` or null | Yes | `"unknown"` for CNUC GML; `null` if the envelope declares no total |
| `crs` | string or null | Yes | `"EPSG:4674"`; `null` is only allowed in a page of a failed resource |

The two complete `paginacao` forms are:

| Form | Required fields, types and example |
|---|---|
| WFS | `{"tipo":"offset","inicio":0,"quantidade":100}`; `inicio` integer >= 0, `quantidade` integer >= 1, both from the request |
| ANA | `{"tipo":"fid","min":1,"max":100,"quantidade":100}`; `min`/`max` integers, `min <= max`; `quantidade` counts expected IDs, not numeric range width |

A Control adds `papel` (required, enum `contagem_antes`, `contagem_depois`, `ids` or
`crs`) and `valor_declarado` (required, integer >= 0, `"unknown"` or `null`).
Counts use the literal envelope value; IDs and CRS use `null`. A Control has no
`paginacao`, `feicoes_recebidas`, `ids_distintos`, `total_declarado` or `crs` fields.

`cabecalhos` allows only the lowercase keys `last-modified`, `etag`,
`content-type`, `content-length`, `content-encoding`, `date`, `retry-after` and `location`.
Only received headers appear, with their textual values, preserving ETag quotes and `W/`.
It contains no cookies, Authorization or secrets. Headers belong to the corresponding
GET response; a subsequent HEAD is not attributed to those bytes.
`content-length` is not comparable to `bytes` when Content-Encoding changes the representation.

### Coverage and error

| Object.field | Type | Required | Example / rule |
|---|---|---|---|
| `cobertura.total_antes` | integer >= 0 or null | Yes | Total from the preceding control; `null` when unknown |
| `cobertura.total_depois` | integer >= 0 or null | Yes | Total from the subsequent control |
| `cobertura.recebidas` | integer >= 0 or null | Yes | Sum of features in preserved pages |
| `cobertura.ids_distintos` | integer >= 0 or null | Yes | Cardinality of the union of received IDs |
| `cobertura.ids_repetidos` | integer >= 0 or null | Yes | Excess occurrences, including across pages |
| `cobertura.campo_id` | string or null | Yes | `"FID"`, `"cd_cnuc"`, `"cd_mun"` or `"fid"` |
| `cobertura.estado` | enum | Yes | `"conferida"`, `"divergente"`, `"nao_comprovada"` or `"nao_aplicavel"` |
| `cobertura.completa` | boolean | Yes | `true` only for an `ok` resource |
| `cobertura.snapshot_transacional` | boolean literal | Yes | `false` |
| `cobertura.controles` | list of paths | Yes | References to controls used; all must exist in `controles` |
| `erro.tipo` | enum | Yes, in object | `"HTTP404"`, `"SourceUnavailableError"`, `"ParseError"`, `"ResourceLimitError"`, `"ContractViolationError"`, `"OSError"`, `"CancelledError"` or `"KeyboardInterrupt"` |
| `erro.mensagem` | nonempty string | Yes, in object | `"Contagem mudou durante a coleta"`; in Portuguese |
| `erro.http_status` | integer 100–599 or null | Yes, in object | `404`; `null` if there was no causal HTTP response |
| `erro.url` | HTTPS URL or null | Yes, in object | Failed request; `null` for a purely local failure |

For files, counts and `campo_id` are `null`, state is `nao_aplicavel` and coverage
references are `[]`. `completa=true` proves file acquisition, not thematic completeness
or the validity of every geometry.

For a paginated `ok` entry, the required equality is
`total_antes == total_depois == feicoes == recebidas == ids_distintos`,
with `ids_repetidos=0`, IDs present and `estado="conferida"`. References include the
before/after counts and, for ANA and CNFP, the official ID list. Received IDs must exactly
match the list, both per range and overall. For WFS, order and progress must be checked
using the field in the API table (except the single page of `incra`/`quilombolas`, checked as described
above). Duplicate or null IDs, repeated pages, a short page
before the end, a changed total, unexpected CRS or unproven continuity prevent `ok`.
A numeric total declared in a page must agree with the control count.
`"unknown"` does not mean zero: only controls with a known total allow completion.

Zero confirmed before/after is `ok` with `paginas=[]` and zero counts; ANA and CNFP also verify
an empty ID list. Without pages, `recebidas`, `ids_distintos` and `ids_repetidos` start
at zero for paginated entries. An incomplete attempt uses `nao_comprovada`; an observed
inconsistency uses `divergente`.
Inconsistencies established by the adapter, including FIDs outside the requested range and changed counts, end collection with `ParseError`, `status="erro"` and `cobertura.estado="divergente"`. An unreadable response without enough evidence to compare coverage remains `nao_comprovada`.
Checking counts and IDs does not detect every attribute update made during acquisition.
Therefore `snapshot_transacional=false` remains mandatory.

## Status, errors and limits

| Situation | Manifest / return |
|---|---|
| Intact file or fully verified query | `ok`; returns `ColetaBruta` |
| Original file GET returns 404 | `ausente_na_fonte`, `erro.tipo="HTTP404"`; returns normally |
| 404 in a page or control | `erro`; raises `SourceUnavailableError` |
| 403, timeout, network failure, 429/5xx after attempts | `erro`; raises `SourceUnavailableError` |
| OGC/ArcGIS error inside HTTP 200, invalid envelope, HTTP 200 file in another format or divergent coverage | `erro`; raises `ParseError` |
| Budget exceeded | `erro` if acquisition started; raises `ResourceLimitError` |
| Invalid argument, selection, destination, key collision or write version | `InvalidParameterError` before network access and without changing the manifest |
| Invalid existing-entry integrity or manifest invariant | `ContractViolationError`; preserves the previous manifest |
| Disk, permissions or publication failure | Propagates `OSError`; attempts to record `erro` only if safe publication remains possible |
| Cancellation or cooperative interruption | Propagates `CancelledError`/`KeyboardInterrupt`; best-effort `erro` entry |
| Forced termination or power failure | There may be no error entry; previous manifest and checkpoint remain for resumption |

404 is a timestamped observation, not proof of permanent absence. There is no UF blacklist
or negative cache: the next resumption retries it. HTTP 200 with zero features is an empty
success, never absence.

There is one active request per collection, sequential pages and no process pool. Retry,
user agent, TLS and pauses respect source policy and the shared rate limiter.
The HTTP default is three total attempts per request, configurable through
`HTTPSettings.max_retries` (zero means one attempt); only transient failures are retried.
403/404, inconsistencies, limits and disk errors are not retried automatically.
Retry and rate-limit waits consume the total deadline. There is no fallback to another source.

`LimitesBrutos` contains the fields below; all appear in `opcoes.limites`.
Integers must be positive and the deadline positive and finite; booleans are rejected.

| Field | Type | Default and example | Application |
|---|---|---|---|
| `max_bytes_recurso` | int | `4294967296` (4 GiB) | Separate encoded and decoded HTTP byte counters, both limited, including controls, pages, file, the CNUC catalog request and failed attempts |
| `max_bytes_pagina` | int | `8388608` (8 MiB) | Each page/control, for both counters; does not limit the original file to 8 MiB |
| `max_paginas` | int | `10000` | Logical data pages; controls are excluded; attempts count toward bytes and deadline |
| `max_ids` | int | `500000` | Declared total and retained IDs; also limits ANA's ID list before pagination |
| `max_bytes_ids` | int | `67108864` (64 MiB) | Accounted memory of ID structures, including keys and containers; only the bounded page being processed is additional |
| `max_segundos` | number | `3600.0` | Monotonic operation deadline, including local verification, network, waits and publication |

The effective file (ZIP or CSV) limit is the smaller of `max_bytes_recurso` and 4 GiB. Limits are
enforced during streaming, including HTTP expansion, before retaining blocks beyond the
budget. The ID budget is not a limit on total process RSS. Local gzip is read incrementally
on resumption, limited by declared size and page/resource ceilings.
ZIP members, if read, go through agrobr's expansion helpers and their limits.

The manifest has a **64 MiB** read/write limit, checked before publication.
Limits do not truncate selections or discard pages to manufacture success. For a larger
resource, reduce the selection or configure appropriate limits; the maximum file limit
still applies. Budgets apply per call, not cumulatively to the destination: old-attempt
evidence and compression overhead also occupy disk space. If a limit prevents publishing
even an error entry, the previous manifest is preserved and `ResourceLimitError` propagates.
The deadline is checked around I/O and processing steps; it cannot preempt a blocking
operating-system call at the exact deadline.

## Resumption

1. For a new destination, validate arguments and create the collection. Different resources
   can be added sequentially to the same destination.
2. If the key already exists, `retomar=False` rejects the operation without network access.
   `retomar=True` requires the same normalized query: source, resource, name, selection,
   layer/edition, logical parameters, format, page size and effective compression.
   Limits may change; they are not part of that identity.
3. If the entry is `ok`, verify schema, invariants, paths, stored size, original hash and
   original size of **every** artifact, including controls. Reuse without HEAD or GET,
   retaining IDs and timestamps and returning `reutilizado=True`. The manifest is unchanged.
   This verifies the local collection; it does not confirm the source is still unchanged.
4. Missing files, corruption or incorrect hashes in `ok` raise `ContractViolationError`;
   evidence is not silently replaced. A limit too low for verification raises
   `ResourceLimitError`, also without changing the entry.
5. An error, absence, checkpoint or `.part` restarts the **whole resource**, with a new
   `coleta_id`. Files restart at byte zero and paginated queries at the first page, with
   new controls. The new entry replaces the previous one only when the attempt ends.
   Previous artifacts remain separate; they are neither merged nor automatically deleted.

A new temporal capture uses another destination. Version 1.0.0 has no Range downloads,
remote page resumption, conditional revalidation of completed resources or incremental collection.

## Complete usage

These calls cover every core resource. The urban bbox is only a selection example,
not a guarantee of acquisition volume or duration.

```python
import asyncio
import json
from pathlib import Path

from agrobr import bruto


async def main() -> None:
    destino = Path("coleta-exemplo")
    pedidos = [
        ("ana", "massas_dagua", {"uf": "AL"}),
        ("cnuc", "ucs", {"uf": "AL"}),
        ("ibge", "malha_municipal", {"uf": "AL"}),
        (
            "ibge",
            "areas_urbanizadas",
            {"nome": "area_teste", "bbox": (-48.1, -16.1, -47.9, -15.9)},
        ),
        ("funai", "terras_indigenas", {}),
        ("funai", "terras_indigenas_pontos", {}),
        ("incra", "quilombolas", {}),
        ("acervo_fundiario", "sigef_publico", {"uf": "AL"}),
        ("acervo_fundiario", "sigef_privado", {"uf": "AL"}),
        ("acervo_fundiario", "snci_publico", {"uf": "AL"}),
        ("acervo_fundiario", "snci_privado", {"uf": "AL"}),
        ("acervo_fundiario", "snci_brasil", {"uf": "AL"}),
        ("acervo_fundiario", "assentamentos", {}),
        ("cnuc", "cadastro", {}),
        ("ibama", "termos_embargo", {}),
        ("ibge", "malha_municipal_zip", {}),
        ("ibge", "areas_urbanizadas_zip", {}),
    ]
    for fonte, recurso, selecao in pedidos:
        resultado = await bruto.coletar(
            fonte, recurso, destino=destino, retomar=True, **selecao
        )
        print(recurso, resultado.entrada.status, resultado.reutilizado)
    with (destino / "manifesto.jsonl").open(encoding="utf-8") as arquivo:
        entradas = [json.loads(linha) for linha in arquivo]
    print([(e["fonte"], e["recurso"], e["nome"], e["status"]) for e in entradas])


asyncio.run(main())
```

In a synchronous script, the same call returns the same type:

```python
from agrobr import sync

resultado = sync.bruto.coletar(
    "acervo_fundiario",
    "snci_publico",
    uf="AL",
    destino="coleta-exemplo",
    retomar=True,
)
print(resultado.manifesto, resultado.entrada.status)
```

For example, use `tamanho_pagina=10` with paginated resources to reduce memory per page.
Use another name/destination when changing that value, because the previous collection's
pagination identity differs.

## Complete manifest examples

The following examples are **illustrative and synthetic**: they are not observed responses,
goldens, current counts or hashes of official files. Endpoints and request profiles follow
the adapters; body values, totals, timestamps, IDs and hashes only illustrate the contract.
Each block contains one complete JSON line with no omitted fields.
The ten lines can form a manifest. Their hashes are computed from synthetic example bodies,
not from official files. The usage example above performs real requests and will produce
different counts, hashes and timestamps.

### ana / massas_dagua

```json
{"agrobr_version":"2.0.0","arquivo":null,"avisos":[],"bytes":null,"bytes_armazenados":null,"cabecalhos":{},"cobertura":{"campo_id":"FID","completa":true,"controles":["ana/massas_dagua/AL/exemplo-ana-massas_dagua/controles/c000001.json.gz","ana/massas_dagua/AL/exemplo-ana-massas_dagua/controles/c000002.json.gz","ana/massas_dagua/AL/exemplo-ana-massas_dagua/controles/c000003.json.gz"],"estado":"conferida","ids_distintos":1,"ids_repetidos":0,"recebidas":1,"snapshot_transacional":false,"total_antes":1,"total_depois":1},"coleta_id":"exemplo-ana-massas_dagua","compressao":"nenhuma","consulta_id":"5611a8c3667d66d56bbf8bbb390e2449c408bc376c228661f621be70b6cb44a7","controles":[{"arquivo":"ana/massas_dagua/AL/exemplo-ana-massas_dagua/controles/c000001.json.gz","bytes":11,"bytes_armazenados":31,"cabecalhos":{"content-type":"application/json"},"compressao":"gzip","fim":"2026-10-02T20:00:02Z","formato":"json","http_status":200,"inicio":"2026-10-02T20:00:01Z","numero":1,"papel":"contagem_antes","parametros":{"f":"json","outFields":"*","outSR":"4674","returnCountOnly":"true","returnGeometry":"true","where":"(nmufe = 'ALAGOAS' OR nmufe LIKE 'ALAGOAS, %' OR nmufe LIKE '%, ALAGOAS' OR nmufe LIKE '%, ALAGOAS, %')"},"sha256":"6aea6dfe6561984cdc5c54ead84d47d2cf29e48253ae282aef237404adad4661","url":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query?where=%28nmufe+%3D+%27ALAGOAS%27+OR+nmufe+LIKE+%27ALAGOAS%2C+%25%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%2C+%25%27%29&outFields=%2A&outSR=4674&f=json&returnGeometry=true&returnCountOnly=true","url_solicitada":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query?where=%28nmufe+%3D+%27ALAGOAS%27+OR+nmufe+LIKE+%27ALAGOAS%2C+%25%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%2C+%25%27%29&outFields=%2A&outSR=4674&f=json&returnGeometry=true&returnCountOnly=true","valor_declarado":1},{"arquivo":"ana/massas_dagua/AL/exemplo-ana-massas_dagua/controles/c000002.json.gz","bytes":43,"bytes_armazenados":56,"cabecalhos":{"content-type":"application/json"},"compressao":"gzip","fim":"2026-10-02T20:00:04Z","formato":"json","http_status":200,"inicio":"2026-10-02T20:00:03Z","numero":2,"papel":"ids","parametros":{"f":"json","outFields":"*","outSR":"4674","returnGeometry":"true","returnIdsOnly":"true","where":"(nmufe = 'ALAGOAS' OR nmufe LIKE 'ALAGOAS, %' OR nmufe LIKE '%, ALAGOAS' OR nmufe LIKE '%, ALAGOAS, %')"},"sha256":"7a3c54cbac7a424181b4b1604e2d49ac58188682f239d00b15e3ad754bba4ac2","url":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query?where=%28nmufe+%3D+%27ALAGOAS%27+OR+nmufe+LIKE+%27ALAGOAS%2C+%25%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%2C+%25%27%29&outFields=%2A&outSR=4674&f=json&returnGeometry=true&returnIdsOnly=true","url_solicitada":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query?where=%28nmufe+%3D+%27ALAGOAS%27+OR+nmufe+LIKE+%27ALAGOAS%2C+%25%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%2C+%25%27%29&outFields=%2A&outSR=4674&f=json&returnGeometry=true&returnIdsOnly=true","valor_declarado":null},{"arquivo":"ana/massas_dagua/AL/exemplo-ana-massas_dagua/controles/c000003.json.gz","bytes":11,"bytes_armazenados":31,"cabecalhos":{"content-type":"application/json"},"compressao":"gzip","fim":"2026-10-02T20:00:08Z","formato":"json","http_status":200,"inicio":"2026-10-02T20:00:07Z","numero":3,"papel":"contagem_depois","parametros":{"f":"json","outFields":"*","outSR":"4674","returnCountOnly":"true","returnGeometry":"true","where":"(nmufe = 'ALAGOAS' OR nmufe LIKE 'ALAGOAS, %' OR nmufe LIKE '%, ALAGOAS' OR nmufe LIKE '%, ALAGOAS, %')"},"sha256":"6aea6dfe6561984cdc5c54ead84d47d2cf29e48253ae282aef237404adad4661","url":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query?where=%28nmufe+%3D+%27ALAGOAS%27+OR+nmufe+LIKE+%27ALAGOAS%2C+%25%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%2C+%25%27%29&outFields=%2A&outSR=4674&f=json&returnGeometry=true&returnCountOnly=true","url_solicitada":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query?where=%28nmufe+%3D+%27ALAGOAS%27+OR+nmufe+LIKE+%27ALAGOAS%2C+%25%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%2C+%25%27%29&outFields=%2A&outSR=4674&f=json&returnGeometry=true&returnCountOnly=true","valor_declarado":1}],"crs":"EPSG:4674","crs_evidencia":{"arquivo":"ana/massas_dagua/AL/exemplo-ana-massas_dagua/p000001.esri.json.gz","localizador":"/spatialReference/wkid","tipo":"pagina","valor":"4674"},"erro":null,"feicoes":1,"fim":"2026-10-02T20:00:09Z","fonte":"ana","formato":"esri_json","http_fim":null,"http_inicio":null,"http_status":null,"inicio":"2026-10-02T20:00:00Z","modo":"paginado","nome":"AL","opcoes":{"compactar":true,"limites":{"max_bytes_ids":67108864,"max_bytes_pagina":8388608,"max_bytes_recurso":4294967296,"max_ids":500000,"max_paginas":10000,"max_segundos":3600.0},"tamanho_pagina":100},"paginas":[{"arquivo":"ana/massas_dagua/AL/exemplo-ana-massas_dagua/p000001.esri.json.gz","bytes":179,"bytes_armazenados":158,"cabecalhos":{"content-type":"application/json"},"compressao":"gzip","crs":"EPSG:4674","feicoes_recebidas":1,"fim":"2026-10-02T20:00:06Z","formato":"esri_json","http_status":200,"ids_distintos":1,"inicio":"2026-10-02T20:00:05Z","numero":1,"paginacao":{"max":1,"min":1,"quantidade":1,"tipo":"fid"},"parametros":{"f":"json","outFields":"*","outSR":"4674","returnGeometry":"true","where":"((nmufe = 'ALAGOAS' OR nmufe LIKE 'ALAGOAS, %' OR nmufe LIKE '%, ALAGOAS' OR nmufe LIKE '%, ALAGOAS, %')) AND FID >= 1 AND FID <= 1"},"sha256":"9470996f4054dc75f0bffb7d229836d567f8a30168021f627469baaeda329671","total_declarado":null,"url":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query?where=%28%28nmufe+%3D+%27ALAGOAS%27+OR+nmufe+LIKE+%27ALAGOAS%2C+%25%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%2C+%25%27%29%29+AND+FID+%3E%3D+1+AND+FID+%3C%3D+1&outFields=%2A&outSR=4674&f=json&returnGeometry=true","url_solicitada":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query?where=%28%28nmufe+%3D+%27ALAGOAS%27+OR+nmufe+LIKE+%27ALAGOAS%2C+%25%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%2C+%25%27%29%29+AND+FID+%3E%3D+1+AND+FID+%3C%3D+1&outFields=%2A&outSR=4674&f=json&returnGeometry=true"}],"parametros":{"f":"json","outFields":"*","outSR":"4674","returnGeometry":"true","where":"(nmufe = 'ALAGOAS' OR nmufe LIKE 'ALAGOAS, %' OR nmufe LIKE '%, ALAGOAS' OR nmufe LIKE '%, ALAGOAS, %')"},"recurso":"massas_dagua","registrado_em":"2026-10-02T20:00:10Z","schema_version":"1.0.0","selecao":{"bbox":null,"bbox_crs":null,"camada":"Massa_dagua/MapServer/0","edicao":null,"natureza":null,"uf":"AL"},"sha256":null,"status":"ok","tipo":"recurso","url":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query","url_solicitada":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query"}
```

### cnuc / ucs

```json
{"agrobr_version":"2.0.0","arquivo":null,"avisos":[],"bytes":null,"bytes_armazenados":null,"cabecalhos":{},"cobertura":{"campo_id":"cd_cnuc","completa":true,"controles":["cnuc/ucs/AL/exemplo-cnuc-ucs/controles/c000001.xml.gz","cnuc/ucs/AL/exemplo-cnuc-ucs/controles/c000002.xml.gz"],"estado":"conferida","ids_distintos":1,"ids_repetidos":0,"recebidas":1,"snapshot_transacional":false,"total_antes":1,"total_depois":1},"coleta_id":"exemplo-cnuc-ucs","compressao":"nenhuma","consulta_id":"0bf3f67dfeb6a36c6d111cac85136e18a86aee2c04702752cea1ef117b4bc299","controles":[{"arquivo":"cnuc/ucs/AL/exemplo-cnuc-ucs/controles/c000001.xml.gz","bytes":104,"bytes_armazenados":112,"cabecalhos":{"content-type":"text/xml"},"compressao":"gzip","fim":"2026-10-02T20:00:02Z","formato":"xml","http_status":200,"inicio":"2026-10-02T20:00:01Z","numero":1,"papel":"contagem_antes","parametros":{"FILTER":"<fes:Filter xmlns:fes='http://www.opengis.net/fes/2.0' xmlns:gml='http://www.opengis.net/gml/3.2'><fes:And><fes:PropertyIsEqualTo><fes:ValueReference>limite</fes:ValueReference><fes:Literal>uc</fes:Literal></fes:PropertyIsEqualTo><fes:PropertyIsLike wildCard='%' singleChar='_' escapeChar='!'><fes:ValueReference>uf</fes:ValueReference><fes:Literal>%ALAGOAS%</fes:Literal></fes:PropertyIsLike></fes:And></fes:Filter>","MAP":"/var/www/storage/app/mapfiles/ucs.map","REQUEST":"GetFeature","RESULTTYPE":"hits","SERVICE":"WFS","TYPENAMES":"ms:ucs_selected","VERSION":"2.0.0"},"sha256":"b9eafd2e580456593d4eecd2d6ca89abd174349ffce0a18f2dfb00289ec4bbbf","url":"https://cnuc-mapserv.mma.gov.br/cgi-bin/mapserv?MAP=%2Fvar%2Fwww%2Fstorage%2Fapp%2Fmapfiles%2Fucs.map&SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=ms%3Aucs_selected&FILTER=%3Cfes%3AFilter+xmlns%3Afes%3D%27http%3A%2F%2Fwww.opengis.net%2Ffes%2F2.0%27+xmlns%3Agml%3D%27http%3A%2F%2Fwww.opengis.net%2Fgml%2F3.2%27%3E%3Cfes%3AAnd%3E%3Cfes%3APropertyIsEqualTo%3E%3Cfes%3AValueReference%3Elimite%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3Euc%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsEqualTo%3E%3Cfes%3APropertyIsLike+wildCard%3D%27%25%27+singleChar%3D%27_%27+escapeChar%3D%27%21%27%3E%3Cfes%3AValueReference%3Euf%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3E%25ALAGOAS%25%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsLike%3E%3C%2Ffes%3AAnd%3E%3C%2Ffes%3AFilter%3E&RESULTTYPE=hits","url_solicitada":"https://cnuc-mapserv.mma.gov.br/cgi-bin/mapserv?MAP=%2Fvar%2Fwww%2Fstorage%2Fapp%2Fmapfiles%2Fucs.map&SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=ms%3Aucs_selected&FILTER=%3Cfes%3AFilter+xmlns%3Afes%3D%27http%3A%2F%2Fwww.opengis.net%2Ffes%2F2.0%27+xmlns%3Agml%3D%27http%3A%2F%2Fwww.opengis.net%2Fgml%2F3.2%27%3E%3Cfes%3AAnd%3E%3Cfes%3APropertyIsEqualTo%3E%3Cfes%3AValueReference%3Elimite%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3Euc%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsEqualTo%3E%3Cfes%3APropertyIsLike+wildCard%3D%27%25%27+singleChar%3D%27_%27+escapeChar%3D%27%21%27%3E%3Cfes%3AValueReference%3Euf%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3E%25ALAGOAS%25%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsLike%3E%3C%2Ffes%3AAnd%3E%3C%2Ffes%3AFilter%3E&RESULTTYPE=hits","valor_declarado":1},{"arquivo":"cnuc/ucs/AL/exemplo-cnuc-ucs/controles/c000002.xml.gz","bytes":104,"bytes_armazenados":112,"cabecalhos":{"content-type":"text/xml"},"compressao":"gzip","fim":"2026-10-02T20:00:08Z","formato":"xml","http_status":200,"inicio":"2026-10-02T20:00:07Z","numero":2,"papel":"contagem_depois","parametros":{"FILTER":"<fes:Filter xmlns:fes='http://www.opengis.net/fes/2.0' xmlns:gml='http://www.opengis.net/gml/3.2'><fes:And><fes:PropertyIsEqualTo><fes:ValueReference>limite</fes:ValueReference><fes:Literal>uc</fes:Literal></fes:PropertyIsEqualTo><fes:PropertyIsLike wildCard='%' singleChar='_' escapeChar='!'><fes:ValueReference>uf</fes:ValueReference><fes:Literal>%ALAGOAS%</fes:Literal></fes:PropertyIsLike></fes:And></fes:Filter>","MAP":"/var/www/storage/app/mapfiles/ucs.map","REQUEST":"GetFeature","RESULTTYPE":"hits","SERVICE":"WFS","TYPENAMES":"ms:ucs_selected","VERSION":"2.0.0"},"sha256":"b9eafd2e580456593d4eecd2d6ca89abd174349ffce0a18f2dfb00289ec4bbbf","url":"https://cnuc-mapserv.mma.gov.br/cgi-bin/mapserv?MAP=%2Fvar%2Fwww%2Fstorage%2Fapp%2Fmapfiles%2Fucs.map&SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=ms%3Aucs_selected&FILTER=%3Cfes%3AFilter+xmlns%3Afes%3D%27http%3A%2F%2Fwww.opengis.net%2Ffes%2F2.0%27+xmlns%3Agml%3D%27http%3A%2F%2Fwww.opengis.net%2Fgml%2F3.2%27%3E%3Cfes%3AAnd%3E%3Cfes%3APropertyIsEqualTo%3E%3Cfes%3AValueReference%3Elimite%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3Euc%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsEqualTo%3E%3Cfes%3APropertyIsLike+wildCard%3D%27%25%27+singleChar%3D%27_%27+escapeChar%3D%27%21%27%3E%3Cfes%3AValueReference%3Euf%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3E%25ALAGOAS%25%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsLike%3E%3C%2Ffes%3AAnd%3E%3C%2Ffes%3AFilter%3E&RESULTTYPE=hits","url_solicitada":"https://cnuc-mapserv.mma.gov.br/cgi-bin/mapserv?MAP=%2Fvar%2Fwww%2Fstorage%2Fapp%2Fmapfiles%2Fucs.map&SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=ms%3Aucs_selected&FILTER=%3Cfes%3AFilter+xmlns%3Afes%3D%27http%3A%2F%2Fwww.opengis.net%2Ffes%2F2.0%27+xmlns%3Agml%3D%27http%3A%2F%2Fwww.opengis.net%2Fgml%2F3.2%27%3E%3Cfes%3AAnd%3E%3Cfes%3APropertyIsEqualTo%3E%3Cfes%3AValueReference%3Elimite%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3Euc%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsEqualTo%3E%3Cfes%3APropertyIsLike+wildCard%3D%27%25%27+singleChar%3D%27_%27+escapeChar%3D%27%21%27%3E%3Cfes%3AValueReference%3Euf%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3E%25ALAGOAS%25%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsLike%3E%3C%2Ffes%3AAnd%3E%3C%2Ffes%3AFilter%3E&RESULTTYPE=hits","valor_declarado":1}],"crs":"EPSG:4674","crs_evidencia":{"arquivo":"cnuc/ucs/AL/exemplo-cnuc-ucs/p000001.gml.gz","localizador":"//*[local-name()='Polygon']/@srsName","tipo":"pagina","valor":"urn:ogc:def:crs:EPSG::4674"},"erro":null,"feicoes":1,"fim":"2026-10-02T20:00:09Z","fonte":"cnuc","formato":"gml","http_fim":null,"http_inicio":null,"http_status":null,"inicio":"2026-10-02T20:00:00Z","modo":"paginado","nome":"AL","opcoes":{"compactar":true,"limites":{"max_bytes_ids":67108864,"max_bytes_pagina":8388608,"max_bytes_recurso":4294967296,"max_ids":500000,"max_paginas":10000,"max_segundos":3600.0},"tamanho_pagina":100},"paginas":[{"arquivo":"cnuc/ucs/AL/exemplo-cnuc-ucs/p000001.gml.gz","bytes":582,"bytes_armazenados":324,"cabecalhos":{"content-type":"text/xml"},"compressao":"gzip","crs":"EPSG:4674","feicoes_recebidas":1,"fim":"2026-10-02T20:00:06Z","formato":"gml","http_status":200,"ids_distintos":1,"inicio":"2026-10-02T20:00:05Z","numero":1,"paginacao":{"inicio":0,"quantidade":100,"tipo":"offset"},"parametros":{"COUNT":"100","FILTER":"<fes:Filter xmlns:fes='http://www.opengis.net/fes/2.0' xmlns:gml='http://www.opengis.net/gml/3.2'><fes:And><fes:PropertyIsEqualTo><fes:ValueReference>limite</fes:ValueReference><fes:Literal>uc</fes:Literal></fes:PropertyIsEqualTo><fes:PropertyIsLike wildCard='%' singleChar='_' escapeChar='!'><fes:ValueReference>uf</fes:ValueReference><fes:Literal>%ALAGOAS%</fes:Literal></fes:PropertyIsLike></fes:And></fes:Filter>","MAP":"/var/www/storage/app/mapfiles/ucs.map","REQUEST":"GetFeature","SERVICE":"WFS","SORTBY":"cd_cnuc","STARTINDEX":"0","TYPENAMES":"ms:ucs_selected","VERSION":"2.0.0"},"sha256":"3969b3ffe691dff13fad926492fe18bb1f0499bd38172fd3df8d7f659f400903","total_declarado":"unknown","url":"https://cnuc-mapserv.mma.gov.br/cgi-bin/mapserv?MAP=%2Fvar%2Fwww%2Fstorage%2Fapp%2Fmapfiles%2Fucs.map&SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=ms%3Aucs_selected&FILTER=%3Cfes%3AFilter+xmlns%3Afes%3D%27http%3A%2F%2Fwww.opengis.net%2Ffes%2F2.0%27+xmlns%3Agml%3D%27http%3A%2F%2Fwww.opengis.net%2Fgml%2F3.2%27%3E%3Cfes%3AAnd%3E%3Cfes%3APropertyIsEqualTo%3E%3Cfes%3AValueReference%3Elimite%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3Euc%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsEqualTo%3E%3Cfes%3APropertyIsLike+wildCard%3D%27%25%27+singleChar%3D%27_%27+escapeChar%3D%27%21%27%3E%3Cfes%3AValueReference%3Euf%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3E%25ALAGOAS%25%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsLike%3E%3C%2Ffes%3AAnd%3E%3C%2Ffes%3AFilter%3E&SORTBY=cd_cnuc&COUNT=100&STARTINDEX=0","url_solicitada":"https://cnuc-mapserv.mma.gov.br/cgi-bin/mapserv?MAP=%2Fvar%2Fwww%2Fstorage%2Fapp%2Fmapfiles%2Fucs.map&SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=ms%3Aucs_selected&FILTER=%3Cfes%3AFilter+xmlns%3Afes%3D%27http%3A%2F%2Fwww.opengis.net%2Ffes%2F2.0%27+xmlns%3Agml%3D%27http%3A%2F%2Fwww.opengis.net%2Fgml%2F3.2%27%3E%3Cfes%3AAnd%3E%3Cfes%3APropertyIsEqualTo%3E%3Cfes%3AValueReference%3Elimite%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3Euc%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsEqualTo%3E%3Cfes%3APropertyIsLike+wildCard%3D%27%25%27+singleChar%3D%27_%27+escapeChar%3D%27%21%27%3E%3Cfes%3AValueReference%3Euf%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3E%25ALAGOAS%25%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsLike%3E%3C%2Ffes%3AAnd%3E%3C%2Ffes%3AFilter%3E&SORTBY=cd_cnuc&COUNT=100&STARTINDEX=0"}],"parametros":{"FILTER":"<fes:Filter xmlns:fes='http://www.opengis.net/fes/2.0' xmlns:gml='http://www.opengis.net/gml/3.2'><fes:And><fes:PropertyIsEqualTo><fes:ValueReference>limite</fes:ValueReference><fes:Literal>uc</fes:Literal></fes:PropertyIsEqualTo><fes:PropertyIsLike wildCard='%' singleChar='_' escapeChar='!'><fes:ValueReference>uf</fes:ValueReference><fes:Literal>%ALAGOAS%</fes:Literal></fes:PropertyIsLike></fes:And></fes:Filter>","MAP":"/var/www/storage/app/mapfiles/ucs.map","REQUEST":"GetFeature","SERVICE":"WFS","SORTBY":"cd_cnuc","TYPENAMES":"ms:ucs_selected","VERSION":"2.0.0"},"recurso":"ucs","registrado_em":"2026-10-02T20:00:10Z","schema_version":"1.0.0","selecao":{"bbox":null,"bbox_crs":null,"camada":"ms:ucs_selected","edicao":null,"natureza":null,"uf":"AL"},"sha256":null,"status":"ok","tipo":"recurso","url":"https://cnuc-mapserv.mma.gov.br/cgi-bin/mapserv","url_solicitada":"https://cnuc-mapserv.mma.gov.br/cgi-bin/mapserv"}
```

### ibge / malha_municipal

```json
{"agrobr_version":"2.0.0","arquivo":null,"avisos":[],"bytes":null,"bytes_armazenados":null,"cabecalhos":{},"cobertura":{"campo_id":"cd_mun","completa":true,"controles":["ibge/malha_municipal/AL/exemplo-ibge-malha_municipal/controles/c000001.xml.gz","ibge/malha_municipal/AL/exemplo-ibge-malha_municipal/controles/c000002.xml.gz"],"estado":"conferida","ids_distintos":1,"ids_repetidos":0,"recebidas":1,"snapshot_transacional":false,"total_antes":1,"total_depois":1},"coleta_id":"exemplo-ibge-malha_municipal","compressao":"nenhuma","consulta_id":"bb53d383d933f7bd1789bbe15a80f635980df52b04e9dd29766e88ead1ab423b","controles":[{"arquivo":"ibge/malha_municipal/AL/exemplo-ibge-malha_municipal/controles/c000001.xml.gz","bytes":104,"bytes_armazenados":112,"cabecalhos":{"content-type":"text/xml"},"compressao":"gzip","fim":"2026-10-02T20:00:02Z","formato":"xml","http_status":200,"inicio":"2026-10-02T20:00:01Z","numero":1,"papel":"contagem_antes","parametros":{"CQL_FILTER":"sigla_uf='AL'","request":"GetFeature","resultType":"hits","service":"WFS","typeNames":"CGMAT:qg_2025_030_munic","version":"2.0.0"},"sha256":"b9eafd2e580456593d4eecd2d6ca89abd174349ffce0a18f2dfb00289ec4bbbf","url":"https://geoservicos.ibge.gov.br/geoserverIBGE/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGMAT%3Aqg_2025_030_munic&CQL_FILTER=sigla_uf%3D%27AL%27&resultType=hits","url_solicitada":"https://geoservicos.ibge.gov.br/geoserverIBGE/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGMAT%3Aqg_2025_030_munic&CQL_FILTER=sigla_uf%3D%27AL%27&resultType=hits","valor_declarado":1},{"arquivo":"ibge/malha_municipal/AL/exemplo-ibge-malha_municipal/controles/c000002.xml.gz","bytes":104,"bytes_armazenados":112,"cabecalhos":{"content-type":"text/xml"},"compressao":"gzip","fim":"2026-10-02T20:00:08Z","formato":"xml","http_status":200,"inicio":"2026-10-02T20:00:07Z","numero":2,"papel":"contagem_depois","parametros":{"CQL_FILTER":"sigla_uf='AL'","request":"GetFeature","resultType":"hits","service":"WFS","typeNames":"CGMAT:qg_2025_030_munic","version":"2.0.0"},"sha256":"b9eafd2e580456593d4eecd2d6ca89abd174349ffce0a18f2dfb00289ec4bbbf","url":"https://geoservicos.ibge.gov.br/geoserverIBGE/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGMAT%3Aqg_2025_030_munic&CQL_FILTER=sigla_uf%3D%27AL%27&resultType=hits","url_solicitada":"https://geoservicos.ibge.gov.br/geoserverIBGE/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGMAT%3Aqg_2025_030_munic&CQL_FILTER=sigla_uf%3D%27AL%27&resultType=hits","valor_declarado":1}],"crs":"EPSG:4674","crs_evidencia":{"arquivo":"ibge/malha_municipal/AL/exemplo-ibge-malha_municipal/p000001.geojson.gz","localizador":"/crs/properties/name","tipo":"pagina","valor":"urn:ogc:def:crs:EPSG::4674"},"erro":null,"feicoes":1,"fim":"2026-10-02T20:00:09Z","fonte":"ibge","formato":"geojson","http_fim":null,"http_inicio":null,"http_status":null,"inicio":"2026-10-02T20:00:00Z","modo":"paginado","nome":"AL","opcoes":{"compactar":true,"limites":{"max_bytes_ids":67108864,"max_bytes_pagina":8388608,"max_bytes_recurso":4294967296,"max_ids":500000,"max_paginas":10000,"max_segundos":3600.0},"tamanho_pagina":100},"paginas":[{"arquivo":"ibge/malha_municipal/AL/exemplo-ibge-malha_municipal/p000001.geojson.gz","bytes":337,"bytes_armazenados":232,"cabecalhos":{"content-type":"application/json"},"compressao":"gzip","crs":"EPSG:4674","feicoes_recebidas":1,"fim":"2026-10-02T20:00:06Z","formato":"geojson","http_status":200,"ids_distintos":1,"inicio":"2026-10-02T20:00:05Z","numero":1,"paginacao":{"inicio":0,"quantidade":100,"tipo":"offset"},"parametros":{"CQL_FILTER":"sigla_uf='AL'","count":"100","outputFormat":"application/json","request":"GetFeature","service":"WFS","sortBy":"cd_mun","startIndex":"0","typeNames":"CGMAT:qg_2025_030_munic","version":"2.0.0"},"sha256":"7df21d94f4b8d6bddc13ffd21967fe1a2c9f4601c6be9588c1c900c4c41a13f7","total_declarado":1,"url":"https://geoservicos.ibge.gov.br/geoserverIBGE/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGMAT%3Aqg_2025_030_munic&outputFormat=application%2Fjson&sortBy=cd_mun&CQL_FILTER=sigla_uf%3D%27AL%27&count=100&startIndex=0","url_solicitada":"https://geoservicos.ibge.gov.br/geoserverIBGE/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGMAT%3Aqg_2025_030_munic&outputFormat=application%2Fjson&sortBy=cd_mun&CQL_FILTER=sigla_uf%3D%27AL%27&count=100&startIndex=0"}],"parametros":{"CQL_FILTER":"sigla_uf='AL'","outputFormat":"application/json","request":"GetFeature","service":"WFS","sortBy":"cd_mun","typeNames":"CGMAT:qg_2025_030_munic","version":"2.0.0"},"recurso":"malha_municipal","registrado_em":"2026-10-02T20:00:10Z","schema_version":"1.0.0","selecao":{"bbox":null,"bbox_crs":null,"camada":"CGMAT:qg_2025_030_munic","edicao":2025,"natureza":null,"uf":"AL"},"sha256":null,"status":"ok","tipo":"recurso","url":"https://geoservicos.ibge.gov.br/geoserverIBGE/wfs","url_solicitada":"https://geoservicos.ibge.gov.br/geoserverIBGE/wfs"}
```

### ibge / areas_urbanizadas

```json
{"agrobr_version":"2.0.0","arquivo":null,"avisos":[],"bytes":null,"bytes_armazenados":null,"cabecalhos":{},"cobertura":{"campo_id":"fid","completa":true,"controles":["ibge/areas_urbanizadas/area_teste/exemplo-ibge-areas_urbanizadas/controles/c000001.xml.gz","ibge/areas_urbanizadas/area_teste/exemplo-ibge-areas_urbanizadas/controles/c000002.xml.gz"],"estado":"conferida","ids_distintos":1,"ids_repetidos":0,"recebidas":1,"snapshot_transacional":false,"total_antes":1,"total_depois":1},"coleta_id":"exemplo-ibge-areas_urbanizadas","compressao":"nenhuma","consulta_id":"f55a4460e91eec1c635203ddc587e5eb2b7b67df52e76f0116b721eacbb0bee2","controles":[{"arquivo":"ibge/areas_urbanizadas/area_teste/exemplo-ibge-areas_urbanizadas/controles/c000001.xml.gz","bytes":104,"bytes_armazenados":112,"cabecalhos":{"content-type":"text/xml"},"compressao":"gzip","fim":"2026-10-02T20:00:02Z","formato":"xml","http_status":200,"inicio":"2026-10-02T20:00:01Z","numero":1,"papel":"contagem_antes","parametros":{"CQL_FILTER":"BBOX(geom,-48.1,-16.1,-47.9,-15.9,'EPSG:4674')","request":"GetFeature","resultType":"hits","service":"WFS","typeNames":"CGEO:AU_2026_AreasUrbanizadas2022_Brasil","version":"2.0.0"},"sha256":"b9eafd2e580456593d4eecd2d6ca89abd174349ffce0a18f2dfb00289ec4bbbf","url":"https://geoservicos.ibge.gov.br/geoserverCGEO/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGEO%3AAU_2026_AreasUrbanizadas2022_Brasil&CQL_FILTER=BBOX%28geom%2C-48.1%2C-16.1%2C-47.9%2C-15.9%2C%27EPSG%3A4674%27%29&resultType=hits","url_solicitada":"https://geoservicos.ibge.gov.br/geoserverCGEO/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGEO%3AAU_2026_AreasUrbanizadas2022_Brasil&CQL_FILTER=BBOX%28geom%2C-48.1%2C-16.1%2C-47.9%2C-15.9%2C%27EPSG%3A4674%27%29&resultType=hits","valor_declarado":1},{"arquivo":"ibge/areas_urbanizadas/area_teste/exemplo-ibge-areas_urbanizadas/controles/c000002.xml.gz","bytes":104,"bytes_armazenados":112,"cabecalhos":{"content-type":"text/xml"},"compressao":"gzip","fim":"2026-10-02T20:00:08Z","formato":"xml","http_status":200,"inicio":"2026-10-02T20:00:07Z","numero":2,"papel":"contagem_depois","parametros":{"CQL_FILTER":"BBOX(geom,-48.1,-16.1,-47.9,-15.9,'EPSG:4674')","request":"GetFeature","resultType":"hits","service":"WFS","typeNames":"CGEO:AU_2026_AreasUrbanizadas2022_Brasil","version":"2.0.0"},"sha256":"b9eafd2e580456593d4eecd2d6ca89abd174349ffce0a18f2dfb00289ec4bbbf","url":"https://geoservicos.ibge.gov.br/geoserverCGEO/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGEO%3AAU_2026_AreasUrbanizadas2022_Brasil&CQL_FILTER=BBOX%28geom%2C-48.1%2C-16.1%2C-47.9%2C-15.9%2C%27EPSG%3A4674%27%29&resultType=hits","url_solicitada":"https://geoservicos.ibge.gov.br/geoserverCGEO/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGEO%3AAU_2026_AreasUrbanizadas2022_Brasil&CQL_FILTER=BBOX%28geom%2C-48.1%2C-16.1%2C-47.9%2C-15.9%2C%27EPSG%3A4674%27%29&resultType=hits","valor_declarado":1}],"crs":"EPSG:4674","crs_evidencia":{"arquivo":"ibge/areas_urbanizadas/area_teste/exemplo-ibge-areas_urbanizadas/p000001.geojson.gz","localizador":"/crs/properties/name","tipo":"pagina","valor":"urn:ogc:def:crs:EPSG::4674"},"erro":null,"feicoes":1,"fim":"2026-10-02T20:00:09Z","fonte":"ibge","formato":"geojson","http_fim":null,"http_inicio":null,"http_status":null,"inicio":"2026-10-02T20:00:00Z","modo":"paginado","nome":"area_teste","opcoes":{"compactar":true,"limites":{"max_bytes_ids":67108864,"max_bytes_pagina":8388608,"max_bytes_recurso":4294967296,"max_ids":500000,"max_paginas":10000,"max_segundos":3600.0},"tamanho_pagina":100},"paginas":[{"arquivo":"ibge/areas_urbanizadas/area_teste/exemplo-ibge-areas_urbanizadas/p000001.geojson.gz","bytes":318,"bytes_armazenados":209,"cabecalhos":{"content-type":"application/json"},"compressao":"gzip","crs":"EPSG:4674","feicoes_recebidas":1,"fim":"2026-10-02T20:00:06Z","formato":"geojson","http_status":200,"ids_distintos":1,"inicio":"2026-10-02T20:00:05Z","numero":1,"paginacao":{"inicio":0,"quantidade":100,"tipo":"offset"},"parametros":{"CQL_FILTER":"BBOX(geom,-48.1,-16.1,-47.9,-15.9,'EPSG:4674')","count":"100","outputFormat":"application/json","request":"GetFeature","service":"WFS","sortBy":"fid","startIndex":"0","typeNames":"CGEO:AU_2026_AreasUrbanizadas2022_Brasil","version":"2.0.0"},"sha256":"0b688742a3f7a99e91dae04260370827649a2475790b157b5bf1ca9d1f897f96","total_declarado":1,"url":"https://geoservicos.ibge.gov.br/geoserverCGEO/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGEO%3AAU_2026_AreasUrbanizadas2022_Brasil&outputFormat=application%2Fjson&sortBy=fid&CQL_FILTER=BBOX%28geom%2C-48.1%2C-16.1%2C-47.9%2C-15.9%2C%27EPSG%3A4674%27%29&count=100&startIndex=0","url_solicitada":"https://geoservicos.ibge.gov.br/geoserverCGEO/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGEO%3AAU_2026_AreasUrbanizadas2022_Brasil&outputFormat=application%2Fjson&sortBy=fid&CQL_FILTER=BBOX%28geom%2C-48.1%2C-16.1%2C-47.9%2C-15.9%2C%27EPSG%3A4674%27%29&count=100&startIndex=0"}],"parametros":{"CQL_FILTER":"BBOX(geom,-48.1,-16.1,-47.9,-15.9,'EPSG:4674')","outputFormat":"application/json","request":"GetFeature","service":"WFS","sortBy":"fid","typeNames":"CGEO:AU_2026_AreasUrbanizadas2022_Brasil","version":"2.0.0"},"recurso":"areas_urbanizadas","registrado_em":"2026-10-02T20:00:10Z","schema_version":"1.0.0","selecao":{"bbox":[-48.1,-16.1,-47.9,-15.9],"bbox_crs":"EPSG:4674","camada":"CGEO:AU_2026_AreasUrbanizadas2022_Brasil","edicao":2022,"natureza":null,"uf":null},"sha256":null,"status":"ok","tipo":"recurso","url":"https://geoservicos.ibge.gov.br/geoserverCGEO/wfs","url_solicitada":"https://geoservicos.ibge.gov.br/geoserverCGEO/wfs"}
```

### acervo_fundiario / sigef_publico

```json
{"agrobr_version":"2.0.0","arquivo":"acervo_fundiario/sigef_publico/AL/exemplo-acervo_fundiario-sigef_publico/original.zip","avisos":[],"bytes":1220,"bytes_armazenados":1220,"cabecalhos":{"content-length":"1220","content-type":"application/x-zip-compressed","etag":"\"exemplo-sigef_publico\"","last-modified":"Fri, 02 Oct 2026 20:00:00 GMT"},"cobertura":{"campo_id":null,"completa":true,"controles":[],"estado":"nao_aplicavel","ids_distintos":null,"ids_repetidos":null,"recebidas":null,"snapshot_transacional":false,"total_antes":null,"total_depois":null},"coleta_id":"exemplo-acervo_fundiario-sigef_publico","compressao":"nenhuma","consulta_id":"090ff8e709fb9534ad3e40d8bd925cd7c5ea396043896cd485c87342f31154ef","controles":[],"crs":null,"crs_evidencia":null,"erro":null,"feicoes":null,"fim":"2026-10-02T20:00:09Z","fonte":"acervo_fundiario","formato":"zip","http_fim":"2026-10-02T20:00:08Z","http_inicio":"2026-10-02T20:00:01Z","http_status":200,"inicio":"2026-10-02T20:00:00Z","modo":"arquivo","nome":"AL","opcoes":{"compactar":false,"limites":{"max_bytes_ids":67108864,"max_bytes_pagina":8388608,"max_bytes_recurso":4294967296,"max_ids":500000,"max_paginas":10000,"max_segundos":3600.0},"tamanho_pagina":null},"paginas":[],"parametros":{},"recurso":"sigef_publico","registrado_em":"2026-10-02T20:00:10Z","schema_version":"1.0.0","selecao":{"bbox":null,"bbox_crs":null,"camada":null,"edicao":null,"natureza":"publico","uf":"AL"},"sha256":"7f17928b731c64b486004a94e74194ef6037acf7fe57c0ec70433d4117f27f47","status":"ok","tipo":"recurso","url":"https://certificacao.incra.gov.br/csv_shp/zip/Sigef%20P%C3%BAblico_AL.zip","url_solicitada":"https://certificacao.incra.gov.br/csv_shp/zip/Sigef%20P%C3%BAblico_AL.zip"}
```

### acervo_fundiario / sigef_privado

```json
{"agrobr_version":"2.0.0","arquivo":"acervo_fundiario/sigef_privado/AL/exemplo-acervo_fundiario-sigef_privado/original.zip","avisos":[],"bytes":1220,"bytes_armazenados":1220,"cabecalhos":{"content-length":"1220","content-type":"application/x-zip-compressed","etag":"\"exemplo-sigef_privado\"","last-modified":"Fri, 02 Oct 2026 20:00:00 GMT"},"cobertura":{"campo_id":null,"completa":true,"controles":[],"estado":"nao_aplicavel","ids_distintos":null,"ids_repetidos":null,"recebidas":null,"snapshot_transacional":false,"total_antes":null,"total_depois":null},"coleta_id":"exemplo-acervo_fundiario-sigef_privado","compressao":"nenhuma","consulta_id":"b4babc34fe42b1f367a37f197ad34ea6d9acc79edd78e6705671540e74536a77","controles":[],"crs":null,"crs_evidencia":null,"erro":null,"feicoes":null,"fim":"2026-10-02T20:00:09Z","fonte":"acervo_fundiario","formato":"zip","http_fim":"2026-10-02T20:00:08Z","http_inicio":"2026-10-02T20:00:01Z","http_status":200,"inicio":"2026-10-02T20:00:00Z","modo":"arquivo","nome":"AL","opcoes":{"compactar":false,"limites":{"max_bytes_ids":67108864,"max_bytes_pagina":8388608,"max_bytes_recurso":4294967296,"max_ids":500000,"max_paginas":10000,"max_segundos":3600.0},"tamanho_pagina":null},"paginas":[],"parametros":{},"recurso":"sigef_privado","registrado_em":"2026-10-02T20:00:10Z","schema_version":"1.0.0","selecao":{"bbox":null,"bbox_crs":null,"camada":null,"edicao":null,"natureza":"privado","uf":"AL"},"sha256":"d2d49150be8ec0612161b1a8506ceb3e9c2a96297eec39f69463a3177b7e04fc","status":"ok","tipo":"recurso","url":"https://certificacao.incra.gov.br/csv_shp/zip/Sigef%20Privado_AL.zip","url_solicitada":"https://certificacao.incra.gov.br/csv_shp/zip/Sigef%20Privado_AL.zip"}
```

### acervo_fundiario / snci_publico

```json
{"agrobr_version":"2.0.0","arquivo":"acervo_fundiario/snci_publico/AL/exemplo-acervo_fundiario-snci_publico/original.zip","avisos":[],"bytes":1200,"bytes_armazenados":1200,"cabecalhos":{"content-length":"1200","content-type":"application/x-zip-compressed","etag":"\"exemplo-snci_publico\"","last-modified":"Fri, 02 Oct 2026 20:00:00 GMT"},"cobertura":{"campo_id":null,"completa":true,"controles":[],"estado":"nao_aplicavel","ids_distintos":null,"ids_repetidos":null,"recebidas":null,"snapshot_transacional":false,"total_antes":null,"total_depois":null},"coleta_id":"exemplo-acervo_fundiario-snci_publico","compressao":"nenhuma","consulta_id":"2f24eecf6f90be43d50a64ee4ded68d323d68dc8eefbb30f1624ae5225924d25","controles":[],"crs":null,"crs_evidencia":null,"erro":null,"feicoes":null,"fim":"2026-10-02T20:00:09Z","fonte":"acervo_fundiario","formato":"zip","http_fim":"2026-10-02T20:00:08Z","http_inicio":"2026-10-02T20:00:01Z","http_status":200,"inicio":"2026-10-02T20:00:00Z","modo":"arquivo","nome":"AL","opcoes":{"compactar":false,"limites":{"max_bytes_ids":67108864,"max_bytes_pagina":8388608,"max_bytes_recurso":4294967296,"max_ids":500000,"max_paginas":10000,"max_segundos":3600.0},"tamanho_pagina":null},"paginas":[],"parametros":{},"recurso":"snci_publico","registrado_em":"2026-10-02T20:00:10Z","schema_version":"1.0.0","selecao":{"bbox":null,"bbox_crs":null,"camada":null,"edicao":null,"natureza":"publico","uf":"AL"},"sha256":"85843e54d72b4cd5d6397bda0bda995f9a356668bcc489f5f70ab5815aa34e25","status":"ok","tipo":"recurso","url":"https://certificacao.incra.gov.br/csv_shp/zip/Im%C3%B3vel%20certificado%20SNCI%20P%C3%BAblico_AL.zip","url_solicitada":"https://certificacao.incra.gov.br/csv_shp/zip/Im%C3%B3vel%20certificado%20SNCI%20P%C3%BAblico_AL.zip"}
```

### acervo_fundiario / snci_privado

```json
{"agrobr_version":"2.0.0","arquivo":"acervo_fundiario/snci_privado/AL/exemplo-acervo_fundiario-snci_privado/original.zip","avisos":[],"bytes":1200,"bytes_armazenados":1200,"cabecalhos":{"content-length":"1200","content-type":"application/x-zip-compressed","etag":"\"exemplo-snci_privado\"","last-modified":"Fri, 02 Oct 2026 20:00:00 GMT"},"cobertura":{"campo_id":null,"completa":true,"controles":[],"estado":"nao_aplicavel","ids_distintos":null,"ids_repetidos":null,"recebidas":null,"snapshot_transacional":false,"total_antes":null,"total_depois":null},"coleta_id":"exemplo-acervo_fundiario-snci_privado","compressao":"nenhuma","consulta_id":"6004975a7070092d9c0c32870150bf54196b711b27853db9032be8ce009e5130","controles":[],"crs":null,"crs_evidencia":null,"erro":null,"feicoes":null,"fim":"2026-10-02T20:00:09Z","fonte":"acervo_fundiario","formato":"zip","http_fim":"2026-10-02T20:00:08Z","http_inicio":"2026-10-02T20:00:01Z","http_status":200,"inicio":"2026-10-02T20:00:00Z","modo":"arquivo","nome":"AL","opcoes":{"compactar":false,"limites":{"max_bytes_ids":67108864,"max_bytes_pagina":8388608,"max_bytes_recurso":4294967296,"max_ids":500000,"max_paginas":10000,"max_segundos":3600.0},"tamanho_pagina":null},"paginas":[],"parametros":{},"recurso":"snci_privado","registrado_em":"2026-10-02T20:00:10Z","schema_version":"1.0.0","selecao":{"bbox":null,"bbox_crs":null,"camada":null,"edicao":null,"natureza":"privado","uf":"AL"},"sha256":"0a4ce6e02cb4ef3cb49187accdd78a328066854abaab11fe087c2e062a1af651","status":"ok","tipo":"recurso","url":"https://certificacao.incra.gov.br/csv_shp/zip/Im%C3%B3vel%20certificado%20SNCI%20Privado_AL.zip","url_solicitada":"https://certificacao.incra.gov.br/csv_shp/zip/Im%C3%B3vel%20certificado%20SNCI%20Privado_AL.zip"}
```

### acervo_fundiario / snci_brasil

```json
{"agrobr_version":"2.0.0","arquivo":"acervo_fundiario/snci_brasil/AL/exemplo-acervo_fundiario-snci_brasil/original.zip","avisos":[],"bytes":1180,"bytes_armazenados":1180,"cabecalhos":{"content-length":"1180","content-type":"application/x-zip-compressed","etag":"\"exemplo-snci_brasil\"","last-modified":"Fri, 02 Oct 2026 20:00:00 GMT"},"cobertura":{"campo_id":null,"completa":true,"controles":[],"estado":"nao_aplicavel","ids_distintos":null,"ids_repetidos":null,"recebidas":null,"snapshot_transacional":false,"total_antes":null,"total_depois":null},"coleta_id":"exemplo-acervo_fundiario-snci_brasil","compressao":"nenhuma","consulta_id":"e808c06ffe8258cc91b2093abe6522022ceb697c3d1285db4cb76e157398129f","controles":[],"crs":null,"crs_evidencia":null,"erro":null,"feicoes":null,"fim":"2026-10-02T20:00:09Z","fonte":"acervo_fundiario","formato":"zip","http_fim":"2026-10-02T20:00:08Z","http_inicio":"2026-10-02T20:00:01Z","http_status":200,"inicio":"2026-10-02T20:00:00Z","modo":"arquivo","nome":"AL","opcoes":{"compactar":false,"limites":{"max_bytes_ids":67108864,"max_bytes_pagina":8388608,"max_bytes_recurso":4294967296,"max_ids":500000,"max_paginas":10000,"max_segundos":3600.0},"tamanho_pagina":null},"paginas":[],"parametros":{},"recurso":"snci_brasil","registrado_em":"2026-10-02T20:00:10Z","schema_version":"1.0.0","selecao":{"bbox":null,"bbox_crs":null,"camada":null,"edicao":null,"natureza":null,"uf":"AL"},"sha256":"2427aab938541297a1d359016d1e5c90180ad1879af9e7d0a870417f318f5124","status":"ok","tipo":"recurso","url":"https://certificacao.incra.gov.br/csv_shp/zip/Im%C3%B3vel%20certificado%20SNCI%20Brasil_AL.zip","url_solicitada":"https://certificacao.incra.gov.br/csv_shp/zip/Im%C3%B3vel%20certificado%20SNCI%20Brasil_AL.zip"}
```

### cnuc / cadastro

```json
{"agrobr_version":"2.0.0","arquivo":"cnuc/cadastro/brasil/exemplo-cnuc-cadastro/original.csv","avisos":[],"bytes":70,"bytes_armazenados":70,"cabecalhos":{"content-type":"text/csv","etag":"\"exemplo-cnuc-cadastro\"","last-modified":"Fri, 02 Oct 2026 20:00:00 GMT"},"cobertura":{"campo_id":null,"completa":true,"controles":[],"estado":"nao_aplicavel","ids_distintos":null,"ids_repetidos":null,"recebidas":null,"snapshot_transacional":false,"total_antes":null,"total_depois":null},"coleta_id":"exemplo-cnuc-cadastro","compressao":"nenhuma","consulta_id":"45ae513cfc8f1b6abac604d10e862c323fb575b76025ee67774268263f8e4e75","controles":[],"crs":null,"crs_evidencia":null,"erro":null,"feicoes":null,"fim":"2026-10-02T20:00:09Z","fonte":"cnuc","formato":"csv","http_fim":"2026-10-02T20:00:08Z","http_inicio":"2026-10-02T20:00:01Z","http_status":200,"inicio":"2026-10-02T20:00:00Z","modo":"arquivo","nome":"brasil","opcoes":{"compactar":false,"limites":{"max_bytes_ids":67108864,"max_bytes_pagina":8388608,"max_bytes_recurso":4294967296,"max_ids":500000,"max_paginas":10000,"max_segundos":3600.0},"tamanho_pagina":null},"paginas":[],"parametros":{},"recurso":"cadastro","registrado_em":"2026-10-02T20:00:10Z","schema_version":"1.0.0","selecao":{"bbox":null,"bbox_crs":null,"camada":null,"edicao":202607,"natureza":null,"uf":null},"sha256":"7413cd5659ddc0c54b8dc6fbcad667ba52bcc197fa83da49509a8a98c257f6ff","status":"ok","tipo":"recurso","url":"https://dados.mma.gov.br/dataset/44b6dc8a-dc82-4a84-8d95-1b0da7c85dac/resource/72dd3d2d-3cca-4b97-b382-a1c90531e379/download/cnuc_2026_07.csv","url_solicitada":"https://dados.mma.gov.br/dataset/44b6dc8a-dc82-4a84-8d95-1b0da7c85dac/resource/72dd3d2d-3cca-4b97-b382-a1c90531e379/download/cnuc_2026_07.csv"}
```


## Scope and compatibility

Raw mode does not normalize attributes, reproject or repair geometries, deduplicate
versions or merge public/private ZIPs, and does not promise that their union equals the
Brasil file. It does not compare collections or detect thematic changes: a rewritten
ZIP or a WFS `timeStamp` can change the hash without changing the data relevant to the consumer.

It offers no DataFrame, Parquet, `as_polars`, `return_meta`, free-form SQL/CQL, arbitrary
URL, local filter presented as a remote selection or study-area selection.
External readers must be adapted by their consumers.
It does not use agrobr's semantic snapshot format.

Table APIs retain their contracts and CRS. `acervo_fundiario.snci()` and `snci_geo()`
without `natureza` still read that state's Brasil file. Opting in with
`natureza="publico"` or `"privado"` is separate from this file API.
ANA page hashes in `MetaInfo` are also not a file manifest:
they provide no paths and do not establish that bodies were stored.

Every raw-mode source is `livre` in the [source licenses](../licenses.en.md), so collection emits
no license warning. Preserving a body does not change its license or validate its thematic content.

Names, types, required fields and the meaning of status, hashes, CRS, paths and coverage
are part of the contract. Incompatible changes require a schema major version;
new optional fields, minor; compatible fixes, patch. Source payloads are not versioned
by agrobr. Readers accept additional optional fields within a known major and reject
unknown majors. Writers only modify manifest versions they can preserve in full;
in 1.0.0, other versions are rejected before network access.
See the [versioning policy](semver.en.md).
