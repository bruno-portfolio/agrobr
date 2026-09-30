# BCB SGS — contract 3.0

Source-API contract for `bcb.sgs()`, registered as `bcb_sgs` and reused by the [`series_economicas` dataset](series_economicas.en.md). The constant is `agrobr.contracts.bcb_sgs.BCB_SGS_V3`; the exported schema is `agrobr/schemas/bcb_sgs.json`.

| Column | pandas type | Nullable | Meaning |
|--------|-------------|----------|---------|
| `data` | datetime64[ns], timezone-naive | No | Published civil reference date, without time of day |
| `valor` | float64 | Yes | Finite measure in the series' own unit; may be negative |
| `codigo` | Int64 | No | Selected SGS code, a positive integer |
| `nome_serie` | text | Yes | Known agrobr alias; null for other codes |
| `data_fim` | datetime64[ns], timezone-naive | Yes | Optional: end of the published period in `dataFim` (e.g. TR, code 226); present only when the source publishes the field |

The key is `codigo, data` and output is sorted in that order, including after `ultimos`. Empty output retains all four stable columns and the numeric/date dtypes; `data_fim` follows them when any row carries `dataFim`, and other body fields raise a warning. The contract's empty frame (`empty_frame()`, used by the dataset) also declares `data_fim`. Explicit source nulls remain missing; malformed text, infinities, and complete magnitude loss when converting to float64 raise an error. Dates must fit the datetime64[ns] domain.

`data` is neither publication date nor acquisition time. Observation bodies provide no frequency or unit; the contract does not infer either from aliases or spacing between references. Published dates outside requested daily bounds are retained with a warning. Duplicates within one body are invalid; identical values across blocks may be reconciled with every origin retained. Conflicts abort acquisition.

The official missing-values HTTP404 envelope and an empty JSON list yield typed empty output. That 404 does not establish that a code exists, and transport/layout errors are not empty results. The query returns only after every planned block succeeds, but `coverage.completeness="unknown"` records the absence of an independent global count.

`MetaInfo` uses schema/contract 3.0 and parser 2. Selection, each final response, SHA256, status, parameters, UTC acquisition, and reconciliation are in `source_details`. The top hash/size represent a canonical query/resources manifest; bodies have individual hashes. Resource and row indexes in origins are zero-based. Coverage extremes precede the `ultimos` tail; returned count describes final output. Revisions are not frozen.

```python
from agrobr import bcb, contracts

df, meta = await bcb.sgs(
    "ipca", inicio="01/01/2024", fim="31/12/2024", return_meta=True,
)
contracts.validate_dataset(df, "bcb_sgs")
```

See the [API](../api/bcb.en.md#sgs) and [migration changes](../guides/migracao-2.en.md).

Version 3.0 changes `codigo` from `int64` to `Int64`. The `BCB_SGS_V2` constant preserves contract 2.1 for historical validation. Text follows the installed pandas default; dtype-name comparisons may need updating.
