# defensivos_tecnicos v1.1

Current technical pesticide registrations from Agrofit/MAPA.

Source: **MAPA**. Contract registry key: `agrofit_tecnicos`.

## Schema

| Column | Type | Nullable | Unit |
|---|---|---|---|
| `nr_registro` | str | No | — |
| `marca_comercial` | str | Yes | — |
| `ingrediente_ativo` | str | Yes | — |
| `titular` | str | Yes | — |
| `classe` | str | Yes | — |
| `grupo_quimico` | str | Yes | — |
| `nome_cientifico` | str | Yes | — |
| `classe_toxicologica` | str | Yes | — |
| `classe_ambiental` | str | Yes | — |
| `composicao_texto` | str | Yes | — |

**Primary key:** `nr_registro`

All stable columns must exist, including nullable columns and empty results. Breaking schema changes require a major contract version.

## Semantics and provenance

One row per textual registration. Compound ingredient and chemical-group labels are not split at arbitrary parentheses. `composicao_texto` retains the published cell; detailed composition is available through `composicao_defensivos(tipo="tecnicos")`.

`return_meta=True` returns data and `MetaInfo`, including attempted/selected sources, acquisition time, contract version and source diagnostics. These current publications do not support selecting a historical revision through `deterministic`.

## Parameters

| Parameter | Type | Default |
|---|---|---|
| `ingrediente_ativo` | `str \| None` | `None` |
| `titular` | `str \| None` | `None` |
| `classe` | `str \| None` | `None` |
| `marca` | `str \| None` | `None` |
| `nr_registro` | `str \| None` | `None` |
| `use_cache` | `bool` | `True` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

## Example

```python
from agrobr import contracts, datasets

df, meta = await datasets.defensivos_tecnicos(nr_registro="00301", return_meta=True)
contracts.validate_dataset(df, "agrofit_tecnicos")
```

Use `from agrobr.sync import datasets` for synchronous calls, without `await`. `as_polars=True` requires `pip install agrobr[polars]`.

## JSON schema and licence

`agrobr/schemas/agrofit_tecnicos.json` · `get_contract("agrofit_tecnicos")`.

`livre` — see [data licences](../licenses.md) and the [API/source details](../api/defensivos_datasets.md).

## Reconciliation of the 2026-09-18 capture

The two complete CSVs in this capture contain 4,403 formulated products, 279,707 authorization occurrences and 2,992 technical products. An independent oracle checks all 12 formulated-product columns, all ten columns of every authorization and the eight direct technical-product fields. All ten technical columns, including ingredient and group extracted from composition, are checked in nine explicit complete-register cohorts.

Composition has independent reconciliation of 57 components in 32 complete product cohorts: nine technical and 23 formulated. Coverage includes file endpoints, leading zeros, an accented premix identifier, nested parentheses, repeated components, zero concentration, scientific notation and published units. Two real ambiguous expressions, `1.9 10*10 UFC/g` and `200 1x10E10 UFC/g`, retain text, null numeric value/unit and diagnostics. The complete component population has not received independent numeric interpretation; the validated scope is explicit in the manifest.

Replays use complete CSV bodies with identical hashes after gzip decompression, through public source and dataset APIs. Cache retains non-null values, dtypes, component position and UTC provenance; pandas `None`/`pd.NA` sentinels are equivalent only in nullable fields. Composition and situation text retain the literal; other fields retain the documented cleanup. Authorizations are not deduplicated.

The structural comparator inventories all columns, both CKAN resources and published suffixes in concentration fields. A suffix can include an ambiguous expression and does not by itself certify a measurement unit or numeric interpretation. Undecided formats, columns, resources or expressions require review. Catalogue, CSV and cache share an origin and do not independently confirm the historical population. Parser 3 and contracts 1.1/1.0 remain unchanged.
