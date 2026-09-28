# Lista Suja employer dataset

`datasets.empregadores_lista_suja` exposes the current main MTE registry through the semantic layer, reusing the [`lista_suja.empregadores` source API](../sources/lista_suja.en.md). Coverage is national: inclusion does not automatically identify an agricultural activity. CEAC is a separate registry and is outside this query.

## Query

```python
from agrobr import datasets

df, meta = await datasets.empregadores_lista_suja(
    uf="PA", formato="csv", return_meta=True,
)
record = await datasets.empregadores_lista_suja(id_registro="41")
```

| Parameter | Default | Behavior |
|---|---|---|
| `uf` | `None` | State abbreviation normalized by the source; local filter |
| `id_registro` | `None` | Exact textual ID within the export, without numeric conversion |
| `formato` | `"auto"` | `"auto"`, `"csv"`, or `"pdf"`, retaining source selection |
| `as_polars` | `False` | Strict boolean; conversion after pandas validation |
| `return_meta` | `False` | Strict boolean; returns `(df, MetaInfo)` when true |

Every argument is keyword-only. State and ID filters can be combined. ID matching is literal: surrounding whitespace is not removed from this selector. No matches produce a typed empty frame; this does not establish that a person or company is absent from other publications.

Both flags must be boolean; filters and format follow source validation. Invalid selections raise `InvalidParameterError` before network access. Positional or unknown arguments, including `produto` and `use_cache`, raise `TypeError` from the signature before I/O.

## Formats and errors

Each call reads the official page and downloads the complete file, even with filters. There is no persistent cache or pagination. CSV works with the core installation. The companion TXT verifies publication context; it is not a second returned table.

`formato="auto"` prefers CSV. The source may choose PDF when CSV is not advertised or an eligible availability failure occurs, retaining the cause in metadata. Explicit `formato="csv"` and `formato="pdf"` are exclusive. Malformed CSV in a successful HTTP response or divergent TXT raises an error; neither triggers a silent switch to PDF. Only the PDF route requires `agrobr[pdf]`.

Validation covers the complete publication before filtering. Source acquisition, parsing, and contract failures are wrapped by the base in `SourceUnavailableError`, with classifications in `errors`; a missing PDF dependency also remains identifiable in that diagnostic. The dataset has no fallback to another institution. Format fallback and its cause remain the source's responsibility.

## Contract and identity

The dataset validates the existing `lista_suja_empregadores` **2.0** contract with twelve columns, including when `return_meta=False`. It creates neither a contract alias nor a new schema under its own name. [Columns and guarantees](../sources/lista_suja.en.md#contract-20) remain the same as in the source.

Documents, CNAE, and IDs remain text, retaining leading zeros. The `id_registro` key applies only within the content identified by its hash; it is not a permanent employer identifier. Repeated documents are not deduplicated. Missing fields remain null, counts/years use `Int64`, and civil dates use timezone-naive `datetime64[ns]`.

Inclusions with intervals or multiple dates retain `data_inclusao_texto` and leave the scalar `data_inclusao` null. The legacy `trabalhadores_resgatados` name represents the official “Trabalhadores envolvidos” field; the wrapper does not change its meaning. Empty results retain all twelve columns and their dtypes.

## Provenance and temporal limits

`meta.dataset` identifies `empregadores_lista_suja`; `meta.source` uses `datasets.empregadores_lista_suja/<route>`. `selected_source` and `attempted_sources` retain `lista_suja_csv` or `lista_suja_pdf`, including a single attempt. When a CSV attempt fails and the source uses PDF, both attempts and their cause are preserved. If CSV was not advertised, only PDF appears among attempts, with that cause recorded in fallback details.

`fetched_at` retains the original timezone-aware UTC acquisition. `fetch_timestamp` is the same acquisition, in timezone-aware UTC. Hash, size, and durations describe the source resource and work, rather than the filtered DataFrame or total wrapper duration. `from_cache=False`; no cache key or expiry is invented. `source_details` and warnings are preserved as independent copies.

Periodic update, registry update, and acquisition are distinct references. A date verified in the publication body is not replaced with the page date or local clock. Missing TXT, or TXT unavailable under eligible conditions, leaves context unverified with its diagnostic; invalid or divergent TXT fails.

`update_frequency="semiannual"` is a convention for the maximum six-month interval in article 2, paragraph 5, of [Interministerial Ordinance 18/2024](https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/inspecao-do-trabalho/portaria-interministerial-mte-mdhc-mir-n-18-2024.pdf), which permits updates at any time. It is neither a fixed schedule nor a publication SLA; `typical_latency` retains this qualification.

`datasets.deterministic(...)` is rejected before I/O, and results have `snapshot=None`. This interface does not select historical editions or reconstruct revisions. See the [license and qualifications](../licenses.en.md#lista-suja); the wrapper preserves the source's existing CPF/CNPJ notice and does not automatically classify records as agricultural.

## Sync and Polars

```python
from agrobr.sync import datasets

df = datasets.empregadores_lista_suja(uf="PA", formato="csv")
polars_df = datasets.empregadores_lista_suja(
    id_registro="41", formato="csv", as_polars=True,
)
```

Polars requires `agrobr[polars]`. Conversion follows pandas contract validation. The dataset also appears in `datasets.list_datasets()` and `datasets.info("empregadores_lista_suja")`.

In the 2026-09-18 reconciliation, all twelve columns and 579 published records were checked in full for CSV/TXT and PDF. Each format retains its own text, including line breaks. Both source and dataset APIs preserve `lista_suja_csv` or `lista_suja_pdf` in attempted/selected sources. Edition date, registry update and acquisition instant remain distinct; see the [source reconciliation](../sources/lista_suja.en.md).

PDF `data_inclusao_texto` preserves cell line breaks; other text fields use the whitespace normalization documented for the source.
