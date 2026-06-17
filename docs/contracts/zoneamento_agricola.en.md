# Contract: zoneamento_agricola

Agricultural Climate Risk Zoning — sowing windows by municipality/crop/soil (ZARC/MAPA).

## Schema

| Column | Type | Nullable | Constraints |
|--------|------|----------|-------------|
| `cultura` | STRING | No | normalized pt-br name |
| `safra` | STRING | No | "YYYY/YYYY" or "perene" |
| `geocodigo` | STRING | No | IBGE 7-digit |
| `uf` | STRING | No | 2-letter abbreviation |
| `municipio` | STRING | Yes | — |
| `solo_codigo` | INTEGER | No | soil type code |
| `ciclo_codigo` | INTEGER | No | cultivar cycle code |
| `clima` | STRING | Yes | — |
| `manejo` | STRING | Yes | — |
| `portaria` | STRING | Yes | — |
| `dec1`..`dec36` | INTEGER | Yes | risk per 10-day period (0-5) |

**PK:** `(cultura, safra, geocodigo, solo_codigo, ciclo_codigo)`

The 36 columns `dec1`..`dec36` represent the 36 ten-day periods of the year. Values from 0 (no risk) to 5 (maximum risk).

## Example

```python
from agrobr import datasets

# Soybean zoning in MT
df = await datasets.zoneamento_agricola(cultura="SOJA", uf="MT")

# With filters
df = await datasets.zoneamento_agricola(
    cultura="SOJA", uf="MT", solo=2, ciclo=1, safra="2024/2025"
)

# No filters (returns everything available)
df = await datasets.zoneamento_agricola()

# Available crops (via the source API directly)
from agrobr import zarc
culturas = zarc.culturas()
```
