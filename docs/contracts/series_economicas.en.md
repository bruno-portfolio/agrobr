# Contract — series_economicas

The [`series_economicas` dataset](../api/series_economicas.en.md) reuses **`bcb_sgs` contract version 3.0**, without a dataset-specific contract alias. Its only source is `bcb.sgs`; the dataset layer does not normalize units, frequency or values.

| Column | pandas dtype | Nullable | Meaning |
|---|---|---|---|
| `data` | `datetime64[ns]` | No | Published civil reference date without time or timezone |
| `valor` | `float64` | Yes | Finite value in the series unit; negative values are allowed |
| `codigo` | `Int64` | No | Positive code up to `2**63 - 1` |
| `nome_serie` | text | Yes | Alias known to the SDK, not a title retrieved from the catalogue |
| `data_fim` | `datetime64[ns]` | Yes | Optional: end of the published period in `dataFim` (e.g. TR); present only when the source publishes the field |

These four stable columns retain their order and are followed by `data_fim` when the series publishes `dataFim`. Primary key: **`codigo, data`**. Rows are sorted by code and reference date; a local `ultimos` tail follows reconciliation. Numeric codes without a known alias keep a null `nome_serie`. Non-null names must be nonempty text.

Explicit null values remain missing, without zero filling or interpolation. Invalid textual NaN/infinity is not equivalent to JSON null. Within-response duplicates and conflicting values across blocks fail. Only matching cross-block observations are reconciled, retaining their origins in metadata.

Monthly or quarterly reference dates may precede a requested daily start. The SDK retains the reference and its diagnostic; it does not assume a daily observation. A Saturday is not a calendar error. Unit and periodicity depend on the code and should be checked in the SGS catalogue.

The contract is mandatory without metadata, before Polars conversion and for empty results. Empty frames use the contract's empty frame: the same four columns and dtypes, plus `data_fim`. A no-values envelope does not establish that a series exists; malformed layout is not treated as a legitimate empty result.

```python
from agrobr import contracts, datasets

df = await datasets.series_economicas("ipca", inicio="01/01/2024", fim="31/12/2024")
contracts.validate_dataset(df, "bcb_sgs")
```

`meta.contract_version` and `schema_version` are 3.0. Raw hash/size identify a query/resource manifest, rather than a single response body; individual bodies are described in `source_details.resources`. Unknown coverage is not upgraded to historical completeness. A reference period is not an as-of revision; `deterministic` is rejected before I/O.
