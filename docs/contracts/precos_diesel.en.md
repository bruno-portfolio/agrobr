# precos_diesel v1.0

Weekly diesel prices and explicit monthly aggregates from ANP.

Source: **ANP**. Contract registry key: `precos_diesel`.

## Schema

| Column | Type | Nullable | Unit |
|---|---|---|---|
| `data` | date | No | — |
| `uf` | str | No | — |
| `municipio` | str | No | — |
| `produto` | str | No | — |
| `preco_venda` | float | No | BRL/litro |
| `preco_compra` | float | Yes | BRL/litro |
| `n_postos` | int | Yes | — |
| `margem` | float | Yes | BRL/litro |
| `periodo_inicio` | date | No | — |
| `periodo_fim` | date | No | — |
| `nivel` | str | No | — |
| `unidade` | str | No | — |
| `agregacao` | str | No | — |
| `n_semanas` | int | No | — |
| `n_postos_media` | float | Yes | — |

**Primary key:** Not defined by the contract. Selection removes entirely identical weekly rows with a warning; a weekly identity with conflicting values is rejected before averages are calculated.

All stable columns must exist, including nullable columns and empty results. Breaking schema changes require a major contract version.

## Semantics and provenance

For a state and for Brazil, `preco_venda` is the published value, weighted by distributors' sales since 2004-10-31; for a municipality, it is the simple mean of the stations. `n_postos` is the sample size, not the weight. `DIESEL` is common S500 B diesel oil. Details and an example on the [source page](../sources/anp_diesel.md).

Weeks use their published start date and retain their end, including across month boundaries. `nivel` distinguishes municipality, state and Brazil. Monthly output is derived by agrobr: the simple arithmetic mean of available weekly means, assigned to the month of each week's start, without outlet or day weights. It retains `n_semanas` and `n_postos_media`; it is not an independent ANP monthly survey. Entirely identical selected rows are deduplicated with a `UserWarning` and an entry in `MetaInfo.validation_warnings`, including overlaps between workbooks. Conflicting values for the same week raise `ParseError` in both weekly and monthly output. Missing values remain null. Valid periods without a published workbook raise `SourceUnavailableError`; malformed or reversed dates raise `InvalidParameterError`. No artificial contractual primary key is imposed.

`return_meta=True` returns data and `MetaInfo`, including attempted/selected sources, acquisition time, contract version and source diagnostics. These current publications do not support selecting a historical revision through `deterministic`.

Text columns use the installed pandas version's default dtype (`str` in pandas 3 and `object` in pandas 2), including empty results. Dates use `datetime64[ns]`, monetary values use `float64`, and counts use `Int64`. Pass `as_polars` and `return_meta` by name.

## Parameters

| Parameter | Type | Default |
|---|---|---|
| `produto` | `str` | `'DIESEL S10'` |
| `uf` | `str \| None` | `None` |
| `municipio` | `int \| str \| None` | `None` |
| `inicio` | `str \| date \| None` | `None` |
| `fim` | `str \| date \| None` | `None` |
| `agregacao` | `str` | `'semanal'` |
| `nivel` | `str` | `'municipio'` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

## Example

```python
from agrobr import contracts, datasets

df, meta = await datasets.precos_diesel("DIESEL S10", nivel="uf", uf="MT", inicio="2025-01-01", fim="2025-01-31", return_meta=True)
contracts.validate_dataset(df, "precos_diesel")
```

Use `from agrobr.sync import datasets` for synchronous calls, without `await`. `as_polars=True` requires `pip install agrobr[polars]`.

## JSON schema and licence

`agrobr/schemas/precos_diesel.json` · `get_contract("precos_diesel")`.

`livre` — see [data licences](../licenses.md) and the [API/source details](../api/anp_diesel.md).

## Weeks at year boundaries

In municipal workbooks, a week starting in late December may appear in the following period's file. Selection includes the available adjacent workbook when needed: the week from 2023-12-31 to 2024-01-06 is in `2024–2025`, matches a December 2023 filter and contributes to that month's mean. A query may download two workbooks even when both date bounds are in the same year.

Monthly output from this API is calculated from the selected weeks; it does not use the separate monthly workbooks also published by ANP. Missing distribution prices in municipal files leave `preco_compra` and `margem` null. Acquisition receipts identify every workbook used.
