# IMEA API

The IMEA module provides daily quotations, price indicators and trade data from the Mato Grosso Institute of Agricultural Economics.

!!! danger "restrito license"
    IMEA's terms of use prohibit redistribution without written authorization. Personal/educational use only. Ref: [imea.com.br/termo-de-uso](https://imea.com.br/imea-site/termo-de-uso.html)

## Functions

### `cotacoes`

Quotations and price indicators for Mato Grosso.

```python
async def cotacoes(
    cadeia: str = "soja",
    *,
    safra: str | None = None,
    unidade: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `cadeia` | `str` | Production chain: `"soja"`, `"milho"`, `"algodao"`, `"bovinocultura"`, `"suinocultura"`, `"leite"` |
| `safra` | `str \| None` | Filter by crop year (e.g. `"24/25"`). None returns all |
| `unidade` | `str \| None` | Filter by unit (e.g. `"R$/sc"`, `"R$/t"`, `"%"`) |
| `as_polars` | `bool` | Return as polars DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `cadeia`, `localidade`, `valor`, `variacao`, `safra`, `unidade`, `unidade_descricao`, `data_publicacao`

**Example:**

```python
from agrobr import imea

# Soybean quotations MT
df = await imea.cotacoes("soja")

# Specific crop year
df = await imea.cotacoes("milho", safra="24/25")
```

## Synchronous Version

```python
from agrobr.sync import imea

df = imea.cotacoes("soja")
```

## Notes

- Source: [IMEA](https://imea.com.br) — `restrito` license
- Mato Grosso-exclusive data
- Warning emitted on first use
