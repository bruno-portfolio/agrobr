# BCB Focus — contract 2.0

Source-API contract for `bcb.focus()`, registered as `bcb_focus`. Its constant is `agrobr.contracts.bcb_focus.BCB_FOCUS_V2`; the exported schema is `agrobr/schemas/bcb_focus.json`. The `expectativas_mercado` dataset reuses this source contract.

| Column | pandas type | Nullable | Meaning |
|--------|-------------|----------|---------|
| `indicador` | text | No | Exact indicator name published by BCB |
| `data` | datetime64[ns], timezone-naive | No | Civil survey date, without time of day |
| `data_referencia` | text | No | Annual YYYY or monthly MM/YYYY forecast horizon |
| `media`, `mediana`, `desvio_padrao`, `minimo`, `maximo` | float64 | Yes | Published finite statistics, without additional rounding |
| `numero_respondentes` | Int64 | Yes | Published integer between 0 and 2³¹−1 |
| `base_calculo` | Int64 | Yes | Published code between 0 and 2³¹−1, without remapping |
| `periodicidade` | text | No | `anual` or `mensal`, from the selected entity |
| `indicador_detalhe` | text | Yes | Published annual detail; null in the monthly entity |

Empty output retains all twelve columns and numeric/date dtypes. The key is `periodicidade, indicador, indicador_detalhe, data, data_referencia, base_calculo`. Null in the key represents a missing dimension; empty detail text differs from null. Balança comercial's Exportações, Importações, and Saldo must remain separate, as must different bases of the same indicator.

`data` identifies the survey date, distinct from UTC acquisition time. `data_referencia` is a forecast horizon and may be in the future; it is not the date of a realized observation. Its format must contain a valid year and month. Output preserves acquisition order: descending survey date, then ascending textual reference and base; annual detail is the final tie-breaker. MM/YYYY is not chronological horizon order.

Required external fields must be present even when nullable. Explicit missing values do not become zero. Statistics require JSON numbers; respondent counts and bases require JSON integers, without booleans or text/fraction coercion. Non-finite values, ambiguous JSON, and dates outside datetime64[ns] raise errors. Finite negative forecasts remain unchanged. Negative deviation, inverted extrema, and means/medians outside available extrema produce diagnostics with origins while retaining published values. This contract does not certify the survey's scientific consistency.

Every page is validated in full before applying `max_registros`. Duplicates, including identical records or nullable dimensions, selection changes, and failed pages abort acquisition. Without an independent count, short pages advance by the actual number received until an empty page. `coverage.completeness` remains `unknown` without an independent total; discarded rows or pending continuation at the local limit allow `partial`. `complete` requires a reconciled count without discarded rows or contradictory continuation. Count and nextLink support was defensively validated offline, without a functional occurrence in this stage's source probes. Revisions are not frozen between pages.

`MetaInfo` uses schema/contract 2.0, parser 2, and source `bcb_focus`. `source_details` contains query, resources, coverage, warnings, and dtype/null diagnostics. Resources retain URLs, parameters, offset, received/retained counts, status, UTC acquisition, body SHA256, and byte size. Top-level hash/size identify a canonical UTF-8 query/resources manifest; total body bytes are reported separately. Warnings are also emitted without `return_meta=True`.

```python
from agrobr import bcb, contracts

df, meta = await bcb.focus(
    "IPCA", periodicidade="mensal", data_inicial="2026-08-28", return_meta=True,
)
contracts.validate_dataset(df, "bcb_focus")
```

See the [API](../api/bcb.en.md#focus), [source limits](../sources/bcb.en.md#focus-market-expectations), and [migration guide](../guides/migracao-2.en.md).
