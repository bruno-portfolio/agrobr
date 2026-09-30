# RNC/SNPC API

The `rnc` module reads two distinct CultivarWeb/MAPA exports: cultivars registered with RNC and protection records from SNPC. The received population is validated before filtering. Registration and protection are not joined by name.

## Functions

### `registradas`

```python
async def registradas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    grupo: str | None = None,
    situacao: str | None = None,
    mantenedor: str | None = None,
    nr_registro: str | None = None,
    nr_formulario: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]: ...
```

| Filters | Rule |
|---|---|
| `cultivar`, `especie`, `grupo`, `situacao`, `mantenedor` | Case- and accent-insensitive literal substring, without regex (`"feijao"` finds `"Feijão"`); `especie` searches `nome_comum` |
| `nr_registro`, `nr_formulario` | Exact textual equality, preserving leading zeros |

The output has **ten columns**, in this order: `cultivar`, `nome_comum`, `nome_cientifico`, `grupo`, `situacao`, `nr_formulario`, `nr_registro`, `data_registro`, `data_validade`, `mantenedor`.

The key is `nr_registro`, scoped to the export identified by its content hash. Form numbers may repeat or be blank. Published blank cultivar and maintainer names are preserved; they are not filled from other fields.

```python
from agrobr import rnc

soybeans = await rnc.registradas(especie="soja")
registration = await rnc.registradas(nr_registro="42039", use_cache=False)
```

### `protegidas`

```python
async def protegidas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    situacao: str | None = None,
    titular: str | None = None,
    nr_processo: str | None = None,
    nr_certificado: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]: ...
```

| Filters | Rule |
|---|---|
| `cultivar`, `especie`, `situacao`, `titular` | Case- and accent-insensitive literal substring, without regex (`"feijao"` finds `"Feijão"`); `especie` searches `nome_comum` |
| `nr_processo`, `nr_certificado` | Exact textual equality |

The output has **twelve columns**: `cultivar`, `nome_cientifico`, `nome_comum`, `nr_processo`, `situacao`, `nr_certificado`, `inicio_protecao`, `termino_protecao`, `titular`, `representante_legal`, `melhoristas`, `termino_protecao_texto`.

The key is `nr_processo`, scoped to the export identified by its content hash. Certificates may repeat across processes, so a certificate filter can return multiple rows. Cancelled, expired, relinquished or void protection statuses remain as published. Compound person fields are not split or deduplicated.

```python
df, meta = await rnc.protegidas(
    nr_processo="21806.000132/2019",
    return_meta=True,
)
print(df[["termino_protecao", "termino_protecao_texto"]])
```

## Dates and missing values

All four date columns use `datetime64[ns]`: civil dates at midnight, without a timezone. Blank cells remain `NaT`; non-empty text must be a valid `DD/MM/YYYY` date. Impossible dates or unknown text raise `ParseError` instead of silently becoming missing values.

Protection end also accepts the official literal `até a emissão do certificado definitivo` (“until the definitive certificate is issued”). In that case, `termino_protecao` is `NaT`, while `termino_protecao_texto` preserves the condition. The text column contains the published cell with outer whitespace removed, including dates and blanks. A condition can therefore be distinguished from an absent value. No deadline is calculated from this expression, and no administrative status is inferred from it.

Other columns contain text, in the installed pandas default dtype (`str` on pandas 3, `object` on 2). Outer whitespace is removed; blank strings, punctuation and compound content remain. Identifiers are not converted to numbers.

## Validation and selection

Filters are optional and combined by intersection. Outer whitespace is removed before comparison. Blank filters, non-string values, unknown parameters and non-boolean flags raise `InvalidParameterError` before I/O. Numeric IDs are not automatically converted to strings.

Parser 2 requires each family's complete layout, checks record widths, validates cells and rejects duplicate keys before filtering. Contract 1.0 is applied to both the population and the selected output, including with `return_meta=False`. A filter with no matches returns an empty DataFrame with all columns and dtypes preserved; this differs from an invalid or record-free source CSV.

`as_polars=True` converts the validated output and requires the `[polars]` extra. `return_meta=True` returns `(DataFrame, MetaInfo)`, including in Polars mode.

## Cache and provenance

`use_cache=True` reuses a local package per family containing the raw CSV and an acquisition manifest. The TTL is **24 hours from receipt of the CSV**, without renewal on access. Hash, size, family, versions and expiry are checked; cached CSVs are parsed and validated against the contract again. Incompatible, expired or unreadable packages are ignored. Writes are atomic. The old normalized-table cache does not provide the provenance required by this route.

`use_cache=False` neither reads nor writes the cache. It does not select a historical revision.

With `return_meta=True`, metadata includes:

- `selected_source`/`attempted_sources`: `rnc_registradas` or `rnc_protegidas`;
- `raw_content_hash`, `raw_content_size` and the original UTC acquisition time in `fetched_at`, including cache hits;
- `from_cache`, key/expiry where applicable and fetch/parsing durations; filter time is in `source_details.filter_duration_ms`;
- `source_details.acquisition`: search and CSV identities, URLs, timestamps, hashes and public headers;
- `source_details.parser`: layout fingerprint, validated population, missing values, repeated identifiers and date statistics;
- `source_details.selection`: normalized filters and selected row count.

`source_details.coverage.status` is `count_matched` when the CSV row count equals the total reported by its preceding search; a mismatch aborts the acquisition. Without a verified total, the status is `unknown`. Matching counts do not prove a single transaction or immutable revision: `transactional_snapshot` remains `False`.

## Synchronous access and datasets

```python
from agrobr.sync import rnc

df = rnc.registradas(especie="soja")
```

The semantic wrappers `datasets.cultivares_registradas` and `datasets.cultivares_protegidas` reuse these routes and contracts with the same filters. They have one source and reject `deterministic` context before I/O: the current cache does not freeze a historical edition.

## Source and limits

Access follows public search/export forms, using a session and refreshing CSRF before POSTs; no personal credential is required. Tokens and cookies are excluded from public metadata. There is no fallback to another institution. Fields describe the received publication; legal validity and historical revision coverage are not inferred. See the [source description](../sources/rnc.en.md) and [license classification](../licenses.en.md).

## Export of 2026-09-18

The CSVs of the 2026-09-18 export contain 38,325 RNC records and 5,424 SNPC records. The HTML search count that precedes each CSV matches it; this agreement establishes neither a transactional snapshot nor historical completeness.

In RNC, the published 22,946 blank form numbers, 4,256 blank cultivar names and 4,271 blank maintainers remain empty text; both dates are populated in that export. There are 924 repeated form-number groups with 15,142 occurrences. In SNPC, protection end has 5,267 dates, 155 conditions and two blanks; the 155 conditions and two blanks produce null dates while retaining the text. There are 102 certificate numbers repeated across distinct processes, with 204 occurrences. These secondary-identifier repetitions are not deduplicated.

The original acquisition in `fetched_at` retains timezone-aware UTC through cache and dataset calls. At both source and dataset level, `fetch_timestamp` equals acquisition time. Table dates are civil dates independent of these timestamps.
