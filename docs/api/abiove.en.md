# ABIOVE API

The ABIOVE module provides export data for the soybean complex — beans, meal, oil and corn — published by the Brazilian Association of Vegetable Oil Industries.

!!! warning "zona_cinza license"
    Terms of use not located publicly. Formal authorization requested in Feb/2026 — awaiting reply.

## Functions

### `exportacao`

Export volumes and revenue for the soybean complex.

```python
async def exportacao(
    ano: int,
    *,
    mes: int | None = None,
    produto: str | None = None,
    agregacao: str = "detalhado",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `ano` | `int` | Reference year |
| `mes` | `int \| None` | Specific month (1-12). None returns all |
| `produto` | `str \| None` | Filter: `"grao"`, `"farelo"`, `"oleo"`, `"milho"` |
| `agregacao` | `str` | `"detalhado"` (by product/month) or `"mensal"` (sum) |
| `as_polars` | `bool` | Return as polars DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `ano`, `mes`, `produto`, `volume_ton`, `receita_usd_mil`

**Example:**

```python
from agrobr import abiove

# Full 2024 exports
df = await abiove.exportacao(2024)

# Meal only
df = await abiove.exportacao(2024, produto="farelo")

# Specific month
df = await abiove.exportacao(2024, mes=6)
```

## Synchronous Version

```python
from agrobr.sync import abiove

df = abiove.exportacao(2024)
```

## Notes

- Source: [ABIOVE](https://abiove.org.br) — `zona_cinza` license
- Data in Excel with a multi-section format
