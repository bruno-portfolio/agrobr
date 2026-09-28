# Lista Suja — MTE Employer Registry

The source table is also available through [`datasets.empregadores_lista_suja`](../api/empregadores_lista_suja.en.md), which reuses contract 2.0 and preserves CSV/PDF route provenance. This is a national employer registry, not an agricultural-only selection. The examples below call the source API; wrapper signature guards and error wrapping are documented on the dataset page.

## Access

The API reads the main registry published by Brazil's Ministry of Labor and Employment on its [official page](https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/inspecao-do-trabalho/areas-de-atuacao/combate-ao-trabalho-escravo-e-analogo-ao-de-escravo), without authentication. Discovery distinguishes this registry from the separate Cadastro de Empregadores em Ajustamento de Conduta (CEAC).

The default `formato="auto"` uses CSV and verifies the companion TXT to obtain publication context. This route works with the core installation. PDF is an alternative when CSV is not advertised or an eligible transport failure occurs. A successful response containing an invalid file raises `ParseError`. Explicit `formato="csv"` and `formato="pdf"` selections use only that format.

Only PDF requires `pip install agrobr[pdf]`. Polars requires `pip install agrobr[polars]`.

## Usage

```python
from agrobr import lista_suja

df, meta = await lista_suja.empregadores(return_meta=True)
para = await lista_suja.empregadores(uf="PA", formato="csv")
record = await lista_suja.empregadores(id_registro="41")
pdf = await lista_suja.empregadores(formato="pdf")
```

Synchronous usage:

```python
from agrobr.sync import lista_suja

df = lista_suja.empregadores(uf="PA")
```

| Argument | Default | Behavior |
|---|---|---|
| `uf` | `None` | Normalized state abbreviation; local filter after validating the entire file |
| `id_registro` | `None` | Exact textual ID within this export; never converted to a number |
| `formato` | `"auto"` | `"auto"`, `"csv"`, or `"pdf"` |
| `as_polars` | `False` | Converts after contract validation |
| `return_meta` | `False` | Returns `(df, MetaInfo)` |

Filters can be combined. No matches produce an empty frame with the same dtypes. Invalid types, invalid states, empty textual filters, and unknown arguments raise `InvalidParameterError` before warnings or network access. Each call downloads the complete file, without pagination or persistent caching.

## Contract 2.0

Available as `get_contract("lista_suja_empregadores")` and `LISTA_SUJA_EMPREGADORES_V2` from `agrobr.contracts.lista_suja`. All twelve columns are stable.

| Column | pandas dtype | Nullable | Content |
|---|---|---|---|
| `empregador` | text | No | Published name |
| `cpf_cnpj` | text | No | Document retaining punctuation and leading zeros |
| `estabelecimento` | text | Yes | Published establishment |
| `uf` | text | Yes | Reported state |
| `cnae` | text | Yes | Code retaining leading zeros |
| `data_inclusao` | `datetime64[ns]` | Yes | Inclusion when the cell contains a single date |
| `trabalhadores_resgatados` | `Int64` | Yes | Official “Trabalhadores envolvidos” field, retaining the legacy name |
| `ano_acao_fiscal` | `Int64` | Yes | Reported year |
| `id_registro` | text | No | Row ID within the export |
| `data_decisao` | `datetime64[ns]` | Yes | Reported administrative decision date |
| `data_atualizacao` | `datetime64[ns]` | Yes | Registry update verified in the publication body |
| `data_inclusao_texto` | text | No | Original cell, including intervals and multiple dates |

The `[id_registro]` key applies within an export identified by `meta.raw_content_hash`. Repeated documents are not deduplicated. This is not a permanent employer identifier.

Some cells contain text such as `05/04/2024 a 10/05/2024, 09/04/2025`. In these cases, `data_inclusao` is `NaT`, the text is retained, and `source_details.compound_inclusion_ids` identifies affected records. The parser does not choose the first or last date or infer the legal reason for the interval. Invalid non-empty dates and numbers raise errors.

Missing text remains null and is never filled from another field. Counts and years use `Int64`, including empty results. These dtype and sentinel changes require major contract **2.0**; see the [migration guide](../guides/migracao-2.md).

## Publication and provenance

`meta.source_details.publication` distinguishes `periodic_update` from `registry_updated_at`. In the September 6, 2026 capture, these were April 6 and September 4, 2026, respectively. The latter supplies `data_atualizacao`; the page date, query clock, and `Last-Modified` never replace it.

CSV does not declare its edition. To use the TXT date, the parser compares all ten original fields of every row by ID. Invalid or divergent TXT raises an error. Missing or unavailable TXT leaves the date null with a diagnostic. PDF supplies context in its own body. Publication dates are civil dates; acquisition timestamps use UTC.

Metadata includes:

- Routes actually attempted and selected, format, and fallback cause.
- Requested/final URL, hash, size, acquisition time, and headers for the file, page, and received TXT.
- Title, edition text, and publication notes, without assigning a cause to missing fields.
- Original and final row counts, null counts, layout fingerprint, and applied filters.

Discovery errors do not authorize an old fixed URL. Automatic fallback considers HTTP 403, 404, 408, 410, 429, 5xx or transport failures, applying the retry policy when eligible; other errors propagate.

## Scope and license

The implementation delivers the current export. CEAC, historical edition selection, and revision storage are not part of this API. Hash and date document an acquisition but do not provide a historical archive.

The semantic dataset rejects `deterministic` before I/O and returns `snapshot=None`; neither the current file nor its hash reconstructs another edition.

The existing classification is `livre`. The official footer states CC BY-ND 3.0; no separate file license was located. Public access and the internal classification do not establish unrestricted reuse permission: see the [license verification](../licenses.en.md#lista-suja). The existing CPF/CNPJ notice is emitted on the first call.

## Reconciliation on 2026-09-18

The complete CSV/TXT/PDF bodies captured on September 18 retain the same hashes
as the earlier capture: periodic edition dated 2026-04-06, registry updated on
2026-09-04. Recapture does not imply a new edition. The publication contains
579 records, 567 distinct documents and 4,706 workers in the published field,
including ten compound inclusion dates. Nulls and repeated documents remain.

All twelve columns are compared through both source and dataset APIs against
independent oracles: CSV/TXT read with the standard library, and PDF read by
characters and cell coordinates across its 45 pages. Each format has its own
expectations; two establishment names retain line breaks after a hyphen in
PDF and differ from CSV, without implicit repair. PDF reading uses pdfminer,
which is also a dependency of production pdfplumber; independence between
extraction engines is not claimed.

The inventory covers the ten source fields, edition context, notes and portal
links, separating the main registry from CEAC and alternative formats.
`python -m scripts.reconciliar_lista_suja --output result.json` compares
preserved local bodies; its PDF step requires the `[pdf]` extra. HTTP replays
exercise public APIs and real filters, but execute at new times: original
acquisition timestamps remain in the receipts. CSV/TXT/PDF represent the
same publication and do not constitute independent-source reconciliation.

Derived text fields collapse internal whitespace as production does.
This affects six CSV `estabelecimento` cells in the capture: IDs 170, 180, 356,
368, 410 and 525. Original bodies remain byte-for-byte identical; the manifest
declares the transformation and the raw/normalized values.
PDF `data_inclusao_texto` preserves cell line breaks. Exact oracle comparison
has 12 differences: ten compound inclusion texts and the two establishment
names above. Comparing the ten source fields after whitespace normalization
yields two differences; comparing all twelve final columns literally yields
the 12 differences described above.
