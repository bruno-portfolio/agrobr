# Contract: desmatamento

Consolidated deforestation (PRODES) and real-time alerts (DETER) by biome.

## Modes

| `tipo=` | Contract | Source |
|---------|----------|--------|
| `"prodes"` (default) | `DESMATAMENTO_PRODES_V2` | INPE TerraBrasilis |
| `"deter"` | `DESMATAMENTO_DETER_V2` | INPE TerraBrasilis |

## Schema: PRODES

| Column | Type | Nullable | Unit | Constraints |
|--------|------|----------|------|-------------|
| `ano` | INTEGER | No | — | 1 to 9999, integral |
| `uf` | STRING | No | — | valid state |
| `classe` | STRING | No | — | — |
| `area_km2` | FLOAT | Yes | km² | ≥ 0 |
| `satelite` | STRING | Yes | — | — |
| `sensor` | STRING | Yes | — | — |
| `bioma` | STRING | No | — | valid biome |

**PK:** `(ano, uf, classe, bioma)`

## Schema: DETER

| Column | Type | Nullable | Unit | Constraints |
|--------|------|----------|------|-------------|
| `data` | DATE | No | — | valid date |
| `classe` | STRING | No | — | — |
| `uf` | STRING | No | — | valid state |
| `municipio` | STRING | Yes | — | — |
| `municipio_id` | STRING | Yes | — | — |
| `cod_municipio` | INTEGER | Yes | — | 7-digit IBGE code, from `municipio_id`; without it (DETER Cerrado, whose layer does not carry the code), from the full `municipio` name within the state, with `normalize.resolver_municipio`; null where the row is not a municipality or the name is not in the registry, with a warning |
| `area_km2` | FLOAT | Yes | km² | ≥ 0 |
| `satelite` | STRING | Yes | — | — |
| `sensor` | STRING | Yes | — | — |
| `bioma` | STRING | No | — | Amazônia or Cerrado |

**PK:** `(data, classe, uf, municipio, municipio_id, bioma)`

Text uses the default dtype of the installed pandas (`str` in pandas 3, `object` in 2), with text nulls
as `NaN` or `None`. In the source features, `pub_date` (PRODES Cerrado and Pampa) is `datetime64[ns]`.

## Constraints

- DETER is only available for **Amazônia** and **Cerrado** (fail-fast with `ValueError`)
- Biome is normalized automatically (`"cerrado"` → `"Cerrado"`)
- In PRODES, the Amazon is the biome cut, not the Legal Amazon of INPE's headline rate ([source](../sources/desmatamento.en.md))

## Aggregation

Each row is a primary-key group, not a feature. `area_km2` is the **sum of the published feature
areas** in the group (`area_km` for PRODES, `areamunkm` for DETER) — it is not the official
deforestation rate. Any missing area in the group makes the total missing, with no partial sum.
`satelite` and `sensor` carry the published value when it is unique within the group and are null
when the group mixes values; `meta.source_details["aggregation"]["heterogeneous"]` counts those
cases. Aggregation requires a reconciled selection: if the local limit truncates it, the dataset
raises `ContractViolationError` instead of publishing a partial aggregate. The WFS count is checked
before the download: when it exceeds `max_registros` (default 50,000), the refusal comes at once,
with the count in the message, without downloading any feature. For a larger selection, use
`max_registros=None`: the whole Cerrado in 2023 has 68,620 features, and the download takes
minutes. For individual features use `agrobr.desmatamento.prodes` / `deter` (contracts
`desmatamento.prodes_feicoes` and `desmatamento.deter_feicoes`).

## Example

```python
from agrobr import datasets

# PRODES — consolidated annual deforestation (103 features in DF in 2023)
df = await datasets.desmatamento("Cerrado", tipo="prodes", ano=2023, uf="DF")

# DETER — deforestation alerts in Acre, first quarter of 2024
df = await datasets.desmatamento(
    "Amazônia", tipo="deter", uf="AC", inicio="2024-01-01", fim="2024-03-31"
)

# With metadata (all years of the Cerrado in DF: 3,643 features)
df, meta = await datasets.desmatamento("Cerrado", uf="DF", return_meta=True)
```
