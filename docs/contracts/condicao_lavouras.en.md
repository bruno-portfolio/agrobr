# Contract: condicao_lavouras

Paraná crop conditions — SEAB/DERAL.

## Schema

| Column | Type | Nullable | Unit | Constraints |
|--------|------|----------|------|-------------|
| `produto` | STRING | No | — | normalized DERAL key |
| `data` | STRING | No | — | dd/mm/yyyy |
| `condicao` | STRING | No | — | boa, media, ruim, plantio, colheita |
| `pct` | FLOAT | Yes | % | 0-100 |
| `plantio_pct` | FLOAT | Yes | % | 0-100 |
| `colheita_pct` | FLOAT | Yes | % | 0-100 |

**PK:** `(produto, data, condicao)`

## Products

14 crops: aveia, cafe, cana, canola, cevada, feijao, feijao_1, feijao_2, mandioca, milho, milho_1, milho_2, soja, trigo.

## Geographic scope

Data covers exclusively the state of Paraná (PR).

## Normalization

The dataset automatically normalizes progress rows (sowing/harvest) coming from the DERAL parser:
- `condicao=""` + `plantio_pct` present → `condicao="plantio"`
- `condicao=""` + `colheita_pct` present → `condicao="colheita"`

## Example

```python
from agrobr import datasets

# All crops
df = await datasets.condicao_lavouras()

# Soybeans only
df = await datasets.condicao_lavouras("soja")

# With metadata
df, meta = await datasets.condicao_lavouras(return_meta=True)
```
