# Contract: queimadas

Satellite-detected fire hotspots — INPE Queimadas.

## Schema

| Column | Type | Nullable | Unit | Constraints |
|--------|------|----------|------|-------------|
| `data` | DATE | No | — | valid date |
| `hora_gmt` | STRING | Yes | — | — |
| `lat` | FLOAT | No | — | -35 to 6 |
| `lon` | FLOAT | No | — | -74 to -30 |
| `satelite` | STRING | No | — | — |
| `municipio` | STRING | Yes | — | — |
| `municipio_id` | INTEGER | Yes | — | — |
| `estado` | STRING | Yes | — | — |
| `uf` | STRING | Yes | — | — |
| `bioma` | STRING | Yes | — | — |
| `numero_dias_sem_chuva` | FLOAT | Yes | days | ≥ 0 |
| `precipitacao` | FLOAT | Yes | mm | ≥ 0 |
| `risco_fogo` | FLOAT | Yes | — | 0 to 1 |
| `frp` | FLOAT | Yes | MW | ≥ 0 |

**PK:** `(data, lat, lon, satelite, hora_gmt)`

## Required parameters

- `ano: int` — hotspot year
- `mes: int` — hotspot month

## Example

```python
from agrobr import datasets

# August 2024 hotspots
df = await datasets.queimadas(ano=2024, mes=8)

# With filters
df = await datasets.queimadas(ano=2024, mes=8, uf="TO", bioma="Cerrado")

# With metadata
df, meta = await datasets.queimadas(ano=2024, mes=8, return_meta=True)
```
