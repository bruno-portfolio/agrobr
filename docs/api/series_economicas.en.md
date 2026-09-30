# Economic time series — SGS

`datasets.series_economicas` retrieves one Banco Central SGS time series and enforces the existing [`bcb_sgs` 3.0 contract](../contracts/series_economicas.en.md). It delegates acquisition to [`bcb.sgs`](bcb.en.md), without combining series, converting units or inferring frequency from observation dates.

```python
from agrobr import datasets

history, meta = await datasets.series_economicas(
    1, inicio="01/01/2010", fim="31/12/2024", return_meta=True,
)
ipca = await datasets.series_economicas(
    "ipca", inicio="01/01/2024", fim="31/12/2024",
)
```

## Parameters

```python
async def series_economicas(
    codigo: int | str,
    *,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    ultimos: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
): ...
```

`codigo` accepts a positive Int64 integer or an alias already supported by `bcb.sgs`, such as `"ipca"`. Numeric string `"1"` does not replace integer `1`. The source normalizes alias case and surrounding whitespace; errors list the available aliases. Consult the [SGS catalogue](https://www3.bcb.gov.br/sgspub/) for the meaning, unit and frequency of each series.

| Selection | Source behavior |
|---|---|
| Both dates | Inclusive request interval; ISO, `DD/MM/YYYY`, `date` or `datetime` |
| Start only | Effective end is the current UTC date |
| End only | Start is delegated to the server; remote limits may reject an extensive request |
| Neither dates nor `ultimos` | Start defaults to ten years before the current UTC date; end defaults to today |
| `ultimos` without dates | Remote endpoint for the latest observations |
| Dates and `ultimos` | Acquire every block, reconcile, sort, then apply a local tail |

`ultimos` must be a positive integer. The observed remote limit of twenty values for series 1 is not imposed as a universal SDK cap; a recognized remote rejection raises `InvalidParameterError`. With dates, `ultimos=1300` is valid and does not skip earlier blocks.

Flags require actual booleans. Invalid codes, calendar dates, inverted intervals and selectors fail before I/O. Unknown keywords, including `produto`, `use_cache` and `fonte`, raise `TypeError`. Only `codigo` may be positional. The dataset adds neither caching nor new SGS query options.

## History, missing values and errors

The source partitions long ranges into blocks of at most ten years ending at annual boundaries. A transport, parsing or source contract failure in any block aborts the request. Neither a partial first block nor a local tail hides corruption.

Dates are published reference dates. A monthly series may return `2024-01-01` when the requested start is `02/01/2024`; the SDK retains it with a diagnostic warning, including when metadata is not requested. Saturdays, negative values and explicit JSON nulls are preserved. There is no daily filling, interpolation or replacement of missing values with zero. Series that publish `dataFim` (e.g. TR) gain the optional `data_fim` column with the end of the period.

A duplicate within one response is an error. Repeated dates across blocks are reconciled only when their values agree, with origins retained; conflicts fail. A valid `[]` or the recognized no-values envelope can yield a typed empty frame. A recognized HTTP 404 emits a warning and does not establish that the series code exists.

The dataset base wraps source availability, parsing and contract failures in `SourceUnavailableError`, retaining their classification in `errors`. Invalid parameters retain `InvalidParameterError`. `bcb_sgs` is the only source; there is no fallback. Contract validation also runs on empty results and without metadata.

## Metadata and reproducibility

Metadata identifies `source="datasets.series_economicas/bcb_sgs"`, `dataset="series_economicas"`, `source_method="dataset"` and `selected_source="bcb_sgs"`. Contract and schema versions are 3.0. `fetched_at` retains the source collection time as aware UTC; the dataset's `fetch_timestamp` is the same instant.

`source_details` preserves the effective query, defaults, block resources, reconciliation, diagnostics and coverage. `coverage.completeness="unknown"` means no global expected count has been established; obtaining all requested blocks does not establish historical completeness. Nested details and lists are independently copied.

`raw_content_hash` is the SHA-256 of the canonical `query`/`resources` manifest; `raw_content_size` is that manifest's byte size. Each resource carries its own body hash and size, while `source_details.resource_bytes` sums HTTP bytes. Fetch/parse durations are preserved. This hash is not treated as a frozen historical revision.

Every `datasets.deterministic(...)` context is rejected before I/O, even with explicit dates: reference selection cannot select the revision available at an earlier date. Results have `snapshot=None`. `update_frequency="varies_by_series"`, with no fixed unit or earliest date, describes the heterogeneous catalogue.

```python
from agrobr import sync

latest = sync.datasets.series_economicas(1, ultimos=3)
```

`as_polars=True` converts after pandas contract validation and requires the Polars extra. Core remains independent of optional packages. See [licenses](../licenses.md).

Dates accept ISO, DD/MM/YYYY, `date` and `datetime`, discarding time. `01/02/2024` means February 1. `data_inicial`/`data_final` are no longer accepted; use `inicio`/`fim`.
