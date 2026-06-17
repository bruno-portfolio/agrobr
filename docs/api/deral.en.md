# DERAL API

The DERAL module provides crop condition, planting and harvest progress data from the Rural Economy Department of Parana.

## Functions

### `condicao_lavouras`

Weekly condition of Parana crops.

```python
async def condicao_lavouras(
    produto: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str \| None` | Filter by crop (`"soja"`, `"milho"`, `"milho_1"`, `"milho_2"`, `"trigo"`, `"feijao"`, `"cana"`, `"cafe"`, etc.). None returns all |
| `as_polars` | `bool` | Return as polars DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `produto`, `data`, `condicao`, `pct`, `plantio_pct`, `colheita_pct`

**Example:**

```python
from agrobr import deral

# All crops
df = await deral.condicao_lavouras()

# Soybean only
df = await deral.condicao_lavouras("soja")

# With metadata
df, meta = await deral.condicao_lavouras("milho", return_meta=True)
```

## Synchronous Version

```python
from agrobr.sync import deral

df = deral.condicao_lavouras("soja")
```

## Notes

- Source: [DERAL/SEAB-PR](https://www.agricultura.pr.gov.br) — `livre` license
- Parana-exclusive data
- Published in Excel (PC.xls) — layout may vary between crop years
