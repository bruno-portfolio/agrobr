# ComexStat API

The ComexStat module provides Brazilian export and import data from MDIC/SECEX — volumes, FOB values (USD) by product, state and country.

## Functions

### `exportacao`

Export data by agricultural product.

```python
async def exportacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

### `importacao`

Import data by agricultural product. Same interface as `exportacao()`.

```python
async def importacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters (both):**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str` | Product (soja, milho, cafe, algodao, acucar, farelo_soja, oleo_soja, ...) |
| `ano` | `int \| None` | Reference year. Default: previous year |
| `uf` | `str \| None` | Filter by state |
| `agregacao` | `str` | `"mensal"` (default) or `"detalhado"` |
| `as_polars` | `bool` | If True, returns polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `ano`, `mes`, `ncm`, `uf`, `kg_liquido`, `valor_fob_usd`, `volume_ton` (monthly aggregation only)

**Example:**

```python
from agrobr import comexstat

# Soybean exports 2024
df = await comexstat.exportacao("soja", ano=2024)

# Soybean imports 2024
df = await comexstat.importacao("soja", ano=2024)

# Filter by state
df = await comexstat.exportacao("milho", ano=2024, uf="MT")

# Detailed (per record)
df = await comexstat.exportacao("cafe", ano=2024, agregacao="detalhado")
```

## Synchronous Version

```python
from agrobr.sync import comexstat

df = comexstat.exportacao("soja", ano=2024)
df = comexstat.importacao("soja", ano=2024)
```

## Notes

- Source: [ComexStat/MDIC](https://comexstat.mdic.gov.br) — free license
- 30 products mapped by NCM prefix
- Annual CSV files of ~100MB each
- Data available from 1997 onward
