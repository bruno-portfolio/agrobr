# RNC and SNPC cultivar datasets

`datasets.cultivares_registradas` exposes the current National Cultivar Registry (RNC) export. `datasets.cultivares_protegidas` exposes the current protection records from the National Plant Variety Protection Service (SNPC). Both reuse the [`rnc` source API](rnc.en.md), with separate tables, identifiers and contracts. The SDK does not join the families by cultivar name or interpret their legal status.

## Queries

```python
from agrobr import datasets

registered, meta = await datasets.cultivares_registradas(
    nr_registro="42039", return_meta=True,
)
protected = await datasets.cultivares_protegidas(
    nr_processo="21806.000202/2014",
)
by_certificate = await datasets.cultivares_protegidas(nr_certificado="20190277")
```

All arguments are keyword-only. Combined filters use intersection: each row must satisfy every filter. Filtering is local; a fresh acquisition downloads the complete export before selecting rows.

| Parameter | Registered | Protected | Meaning |
|---|---|---|---|
| `cultivar` | Yes | Yes | Literal substring of cultivar name |
| `especie` | Yes | Yes | Literal substring of `nome_comum`; it does not search the scientific name |
| `grupo` | Yes | No | Literal substring of the species group |
| `situacao` | Yes | Yes | Literal substring of the published status |
| `mantenedor` | Yes | No | Literal substring of the maintainer text |
| `titular` | No | Yes | Literal substring of the holder text |
| `nr_registro` | Yes | No | Full textual equality of the registration number |
| `nr_formulario` | Yes | No | Full textual equality of the form number; not a unique key |
| `nr_processo` | No | Yes | Full textual equality of the application/process number |
| `nr_certificado` | No | Yes | Full textual equality of the certificate; may select several processes |

Filters default to `None`. Values must be nonblank strings after trimming surrounding whitespace; numbers and booleans are not converted to text. IDs are also trimmed before exact comparison: `" 42039 "` selects `"42039"`, whereas `"042039"` remains different. Identifier punctuation, leading zeros and case are preserved. Other filters ignore case and treat punctuation literally, without regular expressions or accent normalization.

| Flag | Default | Behavior |
|---|---|---|
| `use_cache` | `True` | Reuses a valid acquisition; `False` bypasses both cache reads and writes |
| `as_polars` | `False` | Converts after pandas contract validation |
| `return_meta` | `False` | Returns `(df, MetaInfo)` when true |

All three flags require actual booleans. Invalid parameters raise `InvalidParameterError` before cache or network access. Positional, unknown or other-family arguments raise `TypeError` at the public signature. There is no `produto`, date-range or historical selection parameter.

## Contracts and missing values

Registered cultivars reuse [`rnc_registradas` 1.0](../contracts/cultivares_registradas.en.md), with ten columns and `nr_registro` as the key. Protected cultivars reuse [`rnc_protegidas` 1.0](../contracts/cultivares_protegidas.en.md), with twelve columns and `nr_processo` as the key. The wrappers do not create contract aliases.

Contract validation always applies, including empty results and calls without metadata. The source validates the entire population before filtering; a filter cannot hide an invalid row elsewhere. No match produces a typed empty DataFrame, without proving absence from other publications or registries.

Published empty strings remain `""`. Civil dates use `datetime64[ns]` without a timezone or time of day; missing dates remain `NaT`. The twelfth protected column, `termino_protecao_texto`, preserves the trimmed original cell, including dates and empty values. The published phrase `até a emissão do certificado definitivo` remains textual and has a null scalar end date. It is not converted to an assumed expiry or tied automatically to one status.

Repeated certificates across distinct processes are retained. Form numbers may also repeat or be empty. Maintainer, holder, representative and breeder text is not split or deduplicated.

## Cache and provenance

Each family cache stores the raw CSV and an acquisition manifest with its hash, URL, UTC collection time, parser and schema. Its TTL is 24 hours from collection, not from the most recent read. Contents are revalidated before use. Legacy normalized CSV caches are not treated as identified acquisitions. `use_cache=False` neither reads nor updates an existing cache.

`meta.dataset` identifies the wrapper; `meta.source` is `datasets.cultivares_registradas/rnc_registradas` or `datasets.cultivares_protegidas/rnc_protegidas`. `selected_source` and `attempted_sources` retain the family route, including cache hits. `source_method="dataset"` identifies the semantic layer; `from_cache` identifies acquisition reuse.

`fetched_at` retains the original timezone-aware UTC collection time. `fetch_timestamp` is the same acquisition, in timezone-aware UTC. Hash and size describe the raw CSV, not the filtered DataFrame. Cache key/expiry and fetch/parse durations are preserved; cached CSV contents are parsed again when read.

`source_details` includes `acquisition.resource/search`, `parser` diagnostics, filters and rows in `selection`, cache status and `coverage`. A verified total in the public search must match CSV rows (`count_matched`); disagreement fails. Without a verified total, coverage is `unknown`. A matching count does not make search and export a transactional snapshot. Population and selected-row counts remain separate, and metadata is independently copied.

Source transport, parsing and contract failures are wrapped by the base as `SourceUnavailableError`, with classification in `errors`. There is no fallback to the other family. See the [source description](../sources/rnc.en.md) and [licenses](../licenses.en.md); access to the CSV does not establish rights over a cultivar.

## Determinism, sync and Polars

Any active `datasets.deterministic(...)` context is rejected before I/O. Current exports and their cache cannot select arbitrary historical revisions; results have `snapshot=None`. `update_frequency="continuous"` describes a current registry, without promising a publication schedule or SLA.

```python
from agrobr.sync import datasets

df = datasets.cultivares_registradas(nr_registro="42039")
polars_df = datasets.cultivares_protegidas(
    nr_certificado="20190277", as_polars=True,
)
```

Pandas queries work with the core installation. Polars requires `agrobr[polars]`; conversion retains textual IDs and dates, including empty results. Both wrappers appear in `datasets.list_datasets()` and `datasets.info(...)`.

## Export of 2026-09-18

The CSVs of the 2026-09-18 export contain 38,325 RNC records and 5,424 SNPC records. The HTML search count that precedes each CSV matches it; this agreement establishes neither a transactional snapshot nor historical completeness.

In RNC, the published 22,946 blank form numbers, 4,256 blank cultivar names and 4,271 blank maintainers remain empty text; both dates are populated in that export. There are 924 repeated form-number groups with 15,142 occurrences. In SNPC, protection end has 5,267 dates, 155 conditions and two blanks; the 155 conditions and two blanks produce null dates while retaining the text. There are 102 certificate numbers repeated across distinct processes, with 204 occurrences. These secondary-identifier repetitions are not deduplicated.

The original acquisition in `fetched_at` retains timezone-aware UTC through cache and dataset calls. At both source and dataset level, `fetch_timestamp` equals acquisition time. Table dates are civil dates independent of these timestamps.
