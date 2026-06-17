# Contract: uso_do_solo

Land cover and use (MapBiomas) — annual cover and transitions between classes.

## Modes

| `tipo=` | Contract | Source |
|---------|----------|--------|
| `"cobertura"` (default) | `MAPBIOMAS_COBERTURA_V1` | MapBiomas |
| `"transicao"` | `MAPBIOMAS_TRANSICAO_V1` | MapBiomas |

## Schema: Cover

| Column | Type | Nullable | Unit | Constraints |
|--------|------|----------|------|-------------|
| `bioma` | STRING | No | — | valid biome |
| `estado` | STRING | No | — | valid state |
| `classe_id` | INTEGER | No | — | MapBiomas LULC code |
| `classe` | STRING | No | — | — |
| `nivel_0` | STRING | Yes | — | — |
| `ano` | INTEGER | No | — | ≥ 1985 |
| `area_ha` | FLOAT | No | ha | ≥ 0 |

**PK:** `(bioma, estado, classe_id, ano)`

## Schema: Transition

| Column | Type | Nullable | Unit | Constraints |
|--------|------|----------|------|-------------|
| `bioma` | STRING | No | — | valid biome |
| `estado` | STRING | No | — | valid state |
| `classe_de_id` | INTEGER | No | — | LULC code |
| `classe_de` | STRING | No | — | — |
| `classe_para_id` | INTEGER | No | — | LULC code |
| `classe_para` | STRING | No | — | — |
| `periodo` | STRING | No | — | YYYY-YYYY format |
| `area_ha` | FLOAT | No | ha | ≥ 0 |

**PK:** `(bioma, estado, classe_de_id, classe_para_id, periodo)`

## Municipal level

Cover supports `nivel="municipio"` — the PK contract is skipped in that mode (the PK does not include municipality).

## Example

```python
from agrobr import datasets

# Cover by state
df = await datasets.uso_do_solo(tipo="cobertura", bioma="Cerrado", ano=2022)

# Cover by municipality
df = await datasets.uso_do_solo(tipo="cobertura", nivel="municipio", municipio="Cuiabá")

# Transitions between classes
df = await datasets.uso_do_solo(tipo="transicao", periodo="2020-2021")

# With metadata
df, meta = await datasets.uso_do_solo(tipo="cobertura", return_meta=True)
```
