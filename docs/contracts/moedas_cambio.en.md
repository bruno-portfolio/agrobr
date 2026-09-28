# moedas_cambio v1.0

Current currency catalogue from the BCB PTAX OData service.

Source: **BCB**. Contract registry key: `bcb_ptax_moedas`.

## Schema

| Column | Type | Nullable | Unit |
|---|---|---|---|
| `moeda` | str | No | — |
| `nome` | str | No | — |
| `tipo_moeda` | str | No | — |

**Primary key:** `moeda`

All stable columns must exist, including nullable columns and empty results. Breaking schema changes require a major contract version.

## Semantics and provenance

One row per three-letter ASCII currency symbol, sorted by currency. Names and types remain as published. The current catalogue does not establish historical validity; `top` controls page size and does not replace pagination.

`return_meta=True` returns data and `MetaInfo`, including attempted/selected sources, acquisition time, contract version and source diagnostics. These current publications do not support selecting a historical revision through `deterministic`.

## Parameters

| Parameter | Type | Default |
|---|---|---|
| `top` | `int` | `1000` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

## Example

```python
from agrobr import contracts, datasets

df, meta = await datasets.moedas_cambio(top=1000, return_meta=True)
contracts.validate_dataset(df, "bcb_ptax_moedas")
```

Use `from agrobr.sync import datasets` for synchronous calls, without `await`. `as_polars=True` requires `pip install agrobr[polars]`.

## JSON schema and licence

`agrobr/schemas/bcb_ptax_moedas.json` · `get_contract("bcb_ptax_moedas")`.

`livre` — see [data licences](../licenses.md) and the [API/source details](../contracts/bcb_ptax.md).
