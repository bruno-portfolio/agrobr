# unidades_conservacao v1.0

Federal, state and municipal conservation units, including private reserves (RPPN), from CNUC.

Source: **CNUC/MMA**. Contract registry key: `unidades_conservacao`.

## Schema

| Column | Type | Nullable | Unit | Description |
|---|---|---|---|---|
| `codigo` | str | No | — | Published CNUC code |
| `nome` | str | No | — | — |
| `esfera` | str | No | — | `federal`, `estadual` or `municipal` |
| `categoria` | str | No | — | SNUC management category, as published (12 values) |
| `grupo` | str | No | — | `PI` (strict protection) or `US` (sustainable use) |
| `categoria_iucn` | str | Yes | — | Published IUCN category (`Category Ia` to `Category VI`) |
| `uf` | str | No | — | State codes in alphabetical order, separated by `/` |
| `municipios` | str | No | — | Published list, as `NAME (UF), …`; the source cuts long texts with `...` |
| `bioma` | str | Yes | — | Biomes with published area in the unit, separated by `/` |
| `area_ha` | float | Yes | ha | Area of the whole unit, not the part inside the state |
| `data_criacao` | date | Yes | — | Creation date |
| `ato_criacao` | str | Yes | — | — |
| `orgao_gestor` | str | Yes | — | — |
| `qualidade_poligono` | str | Yes | — | Whether the polygon follows the legal description, is an estimate or is schematic |
| `wdpa_id` | str | Yes | — | World Database on Protected Areas (WDPA) identifier |

**Primary key:** `codigo`

All stable columns must exist, including nullable columns and empty results. Breaking schema changes require a major contract version.

## Semantics and provenance

Tabular output without geometry; geometry is available from `cnuc.ucs_geo`. The layer only has units with a boundary registered in CNUC: units without a polygon and buffer zones are left out. `uf` may hold more than one state, and the `uf=` filter matches the state code inside the list. The `municipio=` filter matches the full name of each published municipality against the IBGE register, never a substring; lists cut by the source and spellings that do not match IBGE go to `MetaInfo.validation_warnings` and to a `UserWarning`. `bbox` uses longitude/latitude (west, south, east, north).

`bioma` is derived: the biomes (Amazônia, Caatinga, Cerrado, Mata Atlântica, Pampa and Pantanal) with an area greater than zero in the layer's per-biome area fields. Marine area is left out, and a unit without published per-biome area has a null `bioma`.

`return_meta=True` returns data and `MetaInfo`, including attempted/selected sources, acquisition time, contract version and source diagnostics (service count before download, reconciled with the result). The layer is current and does not support selecting a historical revision through `deterministic`.

## Parameters

| Parameter | Type | Default |
|---|---|---|
| `uf` | `str \| None` | `None` |
| `municipio` | `str \| int \| None` | `None` |
| `esfera` | `str \| None` | `None` |
| `categoria` | `str \| None` | `None` |
| `grupo` | `str \| None` | `None` |
| `bioma` | `str \| None` | `None` |
| `bbox` | `tuple[float, float, float, float] \| None` | `None` |
| `max_registros` | `int \| None` | `None` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

A selection above 10,000 units raises `ResourceLimitError` before download. `max_registros` returns the first units ordered by `codigo`.

## Example

```python
from agrobr import contracts, datasets

df, meta = await datasets.unidades_conservacao(uf="SE", return_meta=True)
contracts.validate_dataset(df, "unidades_conservacao")
```

For synchronous calls, use `from agrobr.sync import datasets` and remove `await`. `as_polars=True` requires `pip install agrobr[polars]`.

## JSON schema and license

`agrobr/schemas/unidades_conservacao.json` · `get_contract("unidades_conservacao")`.

`livre` — see the [licenses](../licenses.md) and the [source details](../sources/cnuc.md).

## Layer content

On 2026-10-01 the layer had 3,450 units with a boundary: 1,086 federal, 1,458 state and 906 municipal, including 1,426 private reserves (736 federal, 669 state and 21 municipal). No CNUC code repeated, and 65 units had no per-biome area. The CNUC CSV register of July 2026 has 3,576 units: the difference is mostly units without a polygon in CNUC.

`unidades_conservacao_federais` keeps the ICMBio layer (federal units only, without private reserves) and does not change.
