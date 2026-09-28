# composicao_defensivos v1.0

Pesticide components and published concentrations from Agrofit/MAPA.

Source: **MAPA**. Contract registry key: `agrofit_composicao`.

## Schema

| Column | Type | Nullable | Unit |
|---|---|---|---|
| `tipo` | str | No | — |
| `nr_registro` | str | No | — |
| `ordem_componente` | int | No | — |
| `ingrediente_ativo` | str | Yes | — |
| `grupo_quimico` | str | Yes | — |
| `componente_texto` | str | No | — |
| `concentracao_texto` | str | Yes | — |
| `concentracao_valor` | float | Yes | — |
| `concentracao_unidade` | str | Yes | — |

**Primary key:** `tipo`, `nr_registro`, `ordem_componente`

All stable columns must exist, including nullable columns and empty results. Breaking schema changes require a major contract version.

## Semantics and provenance

Position distinguishes repeated ingredients within each family and registration. `tipo` accepts `formulados` or `tecnicos`. Concentration text and units are retained; ambiguous numeric values remain null with diagnostics, without implicit unit conversion.

`return_meta=True` returns data and `MetaInfo`, including attempted/selected sources, acquisition time, contract version and source diagnostics. These current publications do not support selecting a historical revision through `deterministic`.

## Parameters

| Parameter | Type | Default |
|---|---|---|
| `tipo` | `str` | `'formulados'` |
| `nr_registro` | `str \| None` | `None` |
| `ingrediente_ativo` | `str \| None` | `None` |
| `use_cache` | `bool` | `True` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

## Example

```python
from agrobr import contracts, datasets

df, meta = await datasets.composicao_defensivos(tipo="tecnicos", nr_registro="00301", return_meta=True)
contracts.validate_dataset(df, "agrofit_composicao")
```

Use `from agrobr.sync import datasets` for synchronous calls, without `await`. `as_polars=True` requires `pip install agrobr[polars]`.

## JSON schema and licence

`agrobr/schemas/agrofit_composicao.json` · `get_contract("agrofit_composicao")`.

`livre` — see [data licences](../licenses.md) and the [API/source details](../api/defensivos_datasets.md).

## Export of 2026-09-18

The two CSVs of the 2026-09-18 export contain 4,403 formulated products, 279,707 authorization occurrences and 2,992 technical products. Two ambiguous expressions in that export, `1.9 10*10 UFC/g` and `200 1x10E10 UFC/g`, retain their text and come out with null value and unit and a diagnostic. Numeric interpretation is not guaranteed for the whole component population.

The cache retains non-null values, dtypes, component position and UTC provenance; `None` and `pd.NA` are equivalent only in nullable fields. Composition and situation text retain the literal; other fields retain the documented cleanup. Authorizations are not deduplicated.

A suffix published in the concentration fields can include an ambiguous expression and does not by itself certify a measurement unit or numeric interpretation. Catalogue, CSV and cache share an origin: none of them confirms the historical population.
