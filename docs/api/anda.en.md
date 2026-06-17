# ANDA API

The ANDA module provides fertilizer delivery data by state and month, published by the Associacao Nacional para Difusao de Adubos.

!!! warning "zona_cinza license"
    Terms of use not found publicly. Formal authorization requested in Feb/2026 — awaiting response.

## Dependency

Requires `pdfplumber`:

```bash
pip install agrobr[pdf]
```

## Functions

### `entregas`

Volume of fertilizer deliveries by state and month.

```python
async def entregas(
    ano: int,
    *,
    uf: str | None = None,
    produto: str = "total",
    agregacao: str = "detalhado",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `ano` | `int` | Reference year. A year unavailable on the site raises `SourceUnavailableError` listing the available years |
| `uf` | `str \| None` | Filter by state. None returns all |
| `produto` | `str` | Fertilizer type. Default: `"total"` |
| `agregacao` | `str` | `"detalhado"` (per state/month) or `"mensal"` (sum per month) |
| `as_polars` | `bool` | If True, returns `polars.DataFrame` |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `ano`, `mes`, `uf`, `produto_fertilizante`, `volume_ton`

**Example:**

```python
from agrobr import anda

# 2024 deliveries
df = await anda.entregas(2024)

# Filter by state
df = await anda.entregas(2024, uf="MT")

# Monthly aggregate
df = await anda.entregas(2024, agregacao="mensal")
```

## Synchronous Version

```python
from agrobr.sync import anda

df = anda.entregas(2024)
```

## Notes

- Source: [ANDA](https://anda.org.br) — `zona_cinza` license
- Data extracted from PDF via `pdfplumber`
- Data available from 2009 onward
