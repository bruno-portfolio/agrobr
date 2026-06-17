# Contract: desmatamento

Consolidated deforestation (PRODES) and real-time alerts (DETER) by biome.

## Modes

| `tipo=` | Contract | Source |
|---------|----------|--------|
| `"prodes"` (default) | `DESMATAMENTO_PRODES_V1` | INPE TerraBrasilis |
| `"deter"` | `DESMATAMENTO_DETER_V1` | INPE TerraBrasilis |

## Schema: PRODES

| Column | Type | Nullable | Unit | Constraints |
|--------|------|----------|------|-------------|
| `ano` | INTEGER | No | — | ≥ 2000 |
| `uf` | STRING | No | — | valid state |
| `classe` | STRING | No | — | — |
| `area_km2` | FLOAT | No | km² | ≥ 0 |
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
| `municipio_id` | INTEGER | Yes | — | — |
| `area_km2` | FLOAT | No | km² | ≥ 0 |
| `satelite` | STRING | Yes | — | — |
| `sensor` | STRING | Yes | — | — |
| `bioma` | STRING | No | — | Amazônia or Cerrado |

**PK:** `(data, classe, uf, municipio, bioma)`

## Constraints

- DETER is only available for **Amazônia** and **Cerrado** (fail-fast with `ValueError`)
- Biome is normalized automatically (`"cerrado"` → `"Cerrado"`)

## Example

```python
from agrobr import datasets

# PRODES — consolidated annual deforestation
df = await datasets.desmatamento("Cerrado", tipo="prodes", ano=2023)

# DETER — deforestation alerts
df = await datasets.desmatamento("Amazônia", tipo="deter", data_inicio="2024-01-01")

# With metadata
df, meta = await datasets.desmatamento("Cerrado", return_meta=True)
```
