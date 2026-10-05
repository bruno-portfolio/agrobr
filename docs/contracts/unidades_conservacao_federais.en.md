# unidades_conservacao_federais v1.0

Federal conservation-unit attributes published by ICMBio.

Source: **ICMBio**. Contract registry key: `unidades_conservacao_federais`.

## Schema

| Column | Type | Nullable | Unit |
|---|---|---|---|
| `codigo` | str | No | — |
| `nome` | str | No | — |
| `categoria` | str | No | — |
| `grupo` | str | No | — |
| `uf` | str | No | — |
| `bioma` | str | No | — |
| `area_ha` | float | Yes | ha |
| `ano_criacao` | int | Yes | — |
| `ato_criacao` | str | No | — |

**Primary key:** None; repeated source rows are preserved.

All stable columns must exist, including nullable columns and empty results. Breaking schema changes require a major contract version.

## Semantics and provenance

`categoria` is the category acronym as ICMBio publishes it (`PARNA`, `ESEC`, `APA`, etc.), and `bioma` is ICMBio's upper-case text, which may join biomes and the law (`CERRADO E MATA ATLÂNTICA (LEI 11.428)`, `SISTEMA COSTEIRO-MARINHO`). The names match those of [`unidades_conservacao`](unidades_conservacao.md), with another domain: there, the category is spelled out (`Parque`) and the biome is derived from the per-biome areas (`Cerrado/Mata Atlântica`). Normalize both before stacking or joining the datasets.

Tabular output without geometry. State and biome fields may contain multiple published classifications; codes are not assigned artificial uniqueness. `bbox` uses longitude/latitude (west, south, east, north). Metadata retains source coverage, selected route, resources and hashes.

`return_meta=True` returns data and `MetaInfo`, including attempted/selected sources, acquisition time, contract version and source diagnostics. These current publications do not support selecting a historical revision through `deterministic`.

## Parameters

| Parameter | Type | Default |
|---|---|---|
| `uf` | `str \| None` | `None` |
| `grupo` | `str \| None` | `None` |
| `bioma` | `str \| None` | `None` |
| `bbox` | `tuple[float, float, float, float] \| None` | `None` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

## Example

```python
from agrobr import contracts, datasets

df, meta = await datasets.unidades_conservacao_federais(uf="DF", return_meta=True)
contracts.validate_dataset(df, "unidades_conservacao_federais")
```

Use `from agrobr.sync import datasets` for synchronous calls, without `await`. `as_polars=True` requires `pip install agrobr[polars]`.

## JSON schema and licence

`agrobr/schemas/unidades_conservacao_federais.json` · `get_contract("unidades_conservacao_federais")`.

`livre` — see [data licences](../licenses.md) and the [API/source details](../sources/icmbio.md).

## Layer content

On 2026-09-18, the layer had 347 units without filters, 50 with `bioma="Cerrado"` and 22 with `uf="SP"`, overlapping selections with 347 distinct CNUC codes. The contract has no primary key and does not remove potential future duplicates.

`areahaalb` is preserved as `area_ha`, without summation or scale conversion; `criacaoano` is an attribute of a unit, not a layer edition. Compound state/biome labels remain intact: 43 units in the unfiltered query span multiple states. `area_ha` is the whole unit's area, not the part inside the state: the `uf=` filter returns a shared unit with its full area, and summing by state counts that unit more than once. No fields were blank on that date; area and year remain nullable.

The WFS returns 11 fields; `FID` and `ogc_fid` are omitted from public output. The layer's XSD declares 22 properties, 13 of them outside the tabular contract, including geometry. Agreement with `numberOfFeatures` requested before the CSV is recorded as `count_reconciled`, without claiming a transactional snapshot.
