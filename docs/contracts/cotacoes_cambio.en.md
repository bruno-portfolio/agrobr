# cotacoes_cambio v2.0

Published PTAX exchange-rate quotes by currency and bulletin.

Source: **BCB**. Contract registry key: `bcb_ptax`.

## Schema

| Column | Type | Nullable | Unit |
|---|---|---|---|
| `cotacao_compra` | float | Yes | — |
| `cotacao_venda` | float | Yes | — |
| `data_hora` | datetime | No | — |
| `data` | date | No | — |
| `moeda` | str | No | — |
| `paridade_compra` | float | Yes | — |
| `paridade_venda` | float | Yes | — |
| `tipo_boletim` | str | Yes | — |

**Primary key:** `moeda`, `data_hora`, `tipo_boletim`

All stable columns must exist, including nullable columns and empty results. Breaking schema changes require a major contract version.

## Semantics and provenance

USD and closing bulletins are the defaults. Use `data` or the `inicio`/`fim` pair as `date`, `datetime`, ISO or DD/MM/YYYY. Published timestamps preserve nanoseconds without an inferred timezone. Monetary units depend on the historical reference and currency; parity differs from the exchange-rate quote. Bulletin labels are literal and belong in the key.

`return_meta=True` returns data and `MetaInfo`, including attempted/selected sources, acquisition time, contract version and source diagnostics. These current publications do not support selecting a historical revision through `deterministic`.

## Parameters

| Parameter | Type | Default |
|---|---|---|
| `data` | `str \| date \| datetime \| None` | `None` |
| `inicio` | `str \| date \| datetime \| None` | `None` |
| `fim` | `str \| date \| datetime \| None` | `None` |
| `moeda` | `str` | `'USD'` |
| `boletim` | `Literal['todos', 'fechamento', 'abertura', 'intermediario']` | `'fechamento'` |
| `top` | `int` | `1000` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

## Example

```python
from agrobr import contracts, datasets

df, meta = await datasets.cotacoes_cambio(moeda="EUR", boletim="todos", data="04/09/2026", return_meta=True)
contracts.validate_dataset(df, "bcb_ptax")
```

Use `from agrobr.sync import datasets` for synchronous calls, without `await`. `as_polars=True` requires `pip install agrobr[polars]`.

## JSON schema and licence

`agrobr/schemas/bcb_ptax.json` · `get_contract("bcb_ptax")`.

`livre` — see [data licences](../licenses.md) and the [API/source details](../contracts/bcb_ptax.md).
