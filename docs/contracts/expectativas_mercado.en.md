# expectativas_mercado v2.0

Annual or monthly market expectations from BCB Focus surveys.

Source: **BCB**. Contract registry key: `bcb_focus`.

## Schema

| Column | Type | Nullable | Unit |
|---|---|---|---|
| `indicador` | str | No | — |
| `data` | date | No | — |
| `data_referencia` | str | No | — |
| `media` | float | Yes | of the indicator |
| `mediana` | float | Yes | of the indicator |
| `desvio_padrao` | float | Yes | of the indicator |
| `minimo` | float | Yes | of the indicator |
| `maximo` | float | Yes | of the indicator |
| `numero_respondentes` | int | Yes | — |
| `base_calculo` | int | Yes | — |
| `periodicidade` | str | No | — |
| `indicador_detalhe` | str | Yes | — |

The statistics (`media`, `mediana`, `desvio_padrao`, `minimo` and `maximo`) are in the unit of the indicator: IPCA in %, the exchange rate in R$/US$.

**Primary key:** `periodicidade`, `indicador`, `indicador_detalhe`, `data`, `data_referencia`, `base_calculo`

All stable columns must exist, including nullable columns and empty results. Breaking schema changes require a major contract version.

## Semantics and provenance

Survey date differs from the `data_referencia` forecast horizon, which retains YYYY or MM/YYYY. Detail, calculation base and periodicity are part of identity. Published negative statistics are retained; finite inconsistencies receive diagnostics. Metadata distinguishes local limits from coverage without an independent total.

`return_meta=True` returns data and `MetaInfo`, including attempted/selected sources, acquisition time, contract version and source diagnostics. These current publications do not support selecting a historical revision through `deterministic`.

## Parameters

| Parameter | Type | Default |
|---|---|---|
| `indicador` | `str` | `'PIB Agropecuária'` |
| `periodicidade` | `Literal['anual', 'mensal']` | `'anual'` |
| `top` | `int` | `1000` |
| `inicio` | `str \| date \| datetime \| None` | `None` |
| `max_registros` | `int \| None` | `None` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

## Example

```python
from agrobr import contracts, datasets

df, meta = await datasets.expectativas_mercado("PIB Agropecuária", inicio="2026-08-24", max_registros=6, return_meta=True)
contracts.validate_dataset(df, "bcb_focus")
```

Use `from agrobr.sync import datasets` for synchronous calls, without `await`. `as_polars=True` requires `pip install agrobr[polars]`.

## JSON schema and licence

`agrobr/schemas/bcb_focus.json` · `get_contract("bcb_focus")`.

`livre` — see [data licences](../licenses.md) and the [API/source details](../contracts/bcb_focus.md).
