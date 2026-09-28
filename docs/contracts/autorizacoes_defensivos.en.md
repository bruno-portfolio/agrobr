# autorizacoes_defensivos v1.1

Pesticide use authorizations published by Agrofit/MAPA.

Source: **MAPA**. Contract registry key: `agrofit_autorizacoes`.

## Schema

| Column | Type | Nullable | Unit |
|---|---|---|---|
| `nr_registro` | str | No | — |
| `marca_comercial` | str | Yes | — |
| `ingrediente_ativo` | str | Yes | — |
| `titular` | str | Yes | — |
| `classe` | str | Yes | — |
| `cultura` | str | Yes | — |
| `praga` | str | Yes | — |
| `praga_nome_comum` | str | Yes | — |
| `modalidade_de_emprego` | str | Yes | — |
| `situacao` | str | Yes | — |

**Primary key:** None; repeated source rows are preserved.

All stable columns must exist, including nullable columns and empty results. Breaking schema changes require a major contract version.

## Semantics and provenance

Every published authorization is retained, including repetitions. Registration IDs are exact text, preserving leading zeros. No primary key is asserted: registration, crop and pest do not establish uniqueness.

`return_meta=True` returns data and `MetaInfo`, including attempted/selected sources, acquisition time, contract version and source diagnostics. These current publications do not support selecting a historical revision through `deterministic`.

## Parameters

| Parameter | Type | Default |
|---|---|---|
| `nr_registro` | `str \| None` | `None` |
| `cultura` | `str \| None` | `None` |
| `ingrediente_ativo` | `str \| None` | `None` |
| `classe` | `str \| None` | `None` |
| `situacao` | `str \| None` | `None` |
| `use_cache` | `bool` | `True` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

## Example

```python
from agrobr import contracts, datasets

df, meta = await datasets.autorizacoes_defensivos(nr_registro="08725", return_meta=True)
contracts.validate_dataset(df, "agrofit_autorizacoes")
```

Use `from agrobr.sync import datasets` for synchronous calls, without `await`. `as_polars=True` requires `pip install agrobr[polars]`.

## JSON schema and licence

`agrobr/schemas/agrofit_autorizacoes.json` · `get_contract("agrofit_autorizacoes")`.

`livre` — see [data licences](../licenses.md) and the [API/source details](../api/defensivos_datasets.md).

## Export of 2026-09-18

The two CSVs of the 2026-09-18 export contain 4,403 formulated products, 279,707 authorization occurrences and 2,992 technical products. Two ambiguous expressions in that export, `1.9 10*10 UFC/g` and `200 1x10E10 UFC/g`, retain their text and come out with null value and unit and a diagnostic. Numeric interpretation is not guaranteed for the whole component population.

The cache retains non-null values, dtypes, component position and UTC provenance; `None` and `pd.NA` are equivalent only in nullable fields. Composition and situation text retain the literal; other fields retain the documented cleanup. Authorizations are not deduplicated.

A suffix published in the concentration fields can include an ambiguous expression and does not by itself certify a measurement unit or numeric interpretation. Catalogue, CSV and cache share an origin: none of them confirms the historical population.
