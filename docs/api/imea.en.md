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
| `safra` | `str \| None` | Filter by crop year: `"24/25"`, `"2024/25"` or `"2024/2025"` (IMEA publishes `"24/25"`). None returns all; an invalid format raises `InvalidParameterError` before any request |
| `unidade` | `str \| None` | Filter by unit (e.g. `"R$/sc"`, `"R$/t"`, `"%"`) |
| `as_polars` | `bool` | Return as polars DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `cadeia`, `indicador_id`, `indicador`, `localidade`, `valor`, `variacao`, `safra`, `unidade`, `unidade_descricao`, `data_publicacao`. `indicador_id` is IMEA's indicator code, and `indicador` is its name in the chain's catalog. `data_publicacao` is `datetime64[ns]`. A filter with no match returns an empty DataFrame with the same dtypes; an empty quote list or catalog at the source raises `ParseError`.

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
