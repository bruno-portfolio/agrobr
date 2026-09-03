# CEPEA API

The CEPEA module provides access to price indicators from the Center for Advanced Studies in Applied Economics (ESALQ/USP).

## Functions

### `indicador`

Retrieves the historical series of price indicators.

```python
async def indicador(
    produto: str,
    praca: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    as_polars: bool = False,
    validate_sanity: bool = False,
    force_refresh: bool = False,
    offline: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) when return_meta=True
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str` | CEPEA product (22 available). See `produtos()` for the full list |
| `praca` | `str \| None` | Quotation location. `None` returns all |
| `inicio` | `str \| date \| None` | Start date (YYYY-MM-DD). Default: 365 days ago |
| `fim` | `str \| date \| None` | End date. Default: today |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `validate_sanity` | `bool` | Run statistical validation (outliers, gaps). Default: `False` |
| `force_refresh` | `bool` | Bypass cache and fetch fresh data |
| `offline` | `bool` | Use local cache/history only |
| `return_meta` | `bool` | Returns a `(df, MetaInfo)` tuple with provenance |

**Returns:**

DataFrame with columns:
- `data`: Indicator date
- `produto`: Product name
- `praca`: Quotation location
- `valor`: Value in BRL/unit
- `unidade`: Unit (e.g. 'BRL/sc60kg')
- `fonte`: Data source ('cepea' or 'noticias_agricolas')
- `metodologia`: Indicator methodology
- `anomalies`: Detected anomalies/markers (e.g. `media_semanal` from the fallback, or `validate_sanity` outliers); `None` when empty

**Example:**

```python
from agrobr import cepea

# Basic
df = await cepea.indicador('soja')

# With a period
df = await cepea.indicador(
    'soja',
    inicio='2024-01-01',
    fim='2024-06-30'
)

# Force refresh
df = await cepea.indicador('soja', force_refresh=True)

# Offline mode (no network)
df = await cepea.indicador('soja', offline=True)
```

---

### `ultimo`

Retrieves the most recent available indicator.

```python
async def ultimo(
    produto: str,
    praca: str | None = None,
    offline: bool = False,
) -> Indicador
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str` | Desired product |
| `praca` | `str \| None` | Quotation location. `None` does not filter by location |
| `offline` | `bool` | Use local cache only |

**Returns:**

An `Indicador` object with:
- `data`: Indicator date
- `valor`: Value as Decimal
- `unidade`: Unit (e.g. 'BRL/sc60kg')
- `produto`: Product name
- `fonte`: Data source

**Example:**

```python
from agrobr import cepea

ultimo = await cepea.ultimo('soja')
print(f"Soybean on {ultimo.data}: R$ {ultimo.valor}/bag")
```

---

### `produtos`

Lists available products.

```python
async def produtos() -> list[str]
```

**Returns:**

List of strings with product names.

**Example:**

```python
from agrobr import cepea

prods = await cepea.produtos()
# ['soja', 'soja_parana', 'milho', 'cafe', 'cafe_arabica', 'cafe_robusta',
#  'boi', 'boi_gordo', 'bezerro', 'trigo', 'algodao', 'arroz', 'acucar', 'acucar_refinado',
#  'frango_congelado', 'frango_resfriado', 'suino', 'etanol_hidratado',
#  'etanol_anidro', 'leite', 'laranja_industria', 'laranja_in_natura']
# 'cafe'/'cafe_arabica' = Arabica (SP); 'cafe_robusta' = Robusta/Conilon (ES)
# Aliases: boi_gordo → boi, cafe_arabica → cafe
```

---

### `pracas`

Lists available quotation locations for a product.

```python
async def pracas(produto: str) -> list[str]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str` | Product |

**Returns:**

List of available locations — empty for a valid product with no mapped locations. An unknown product raises `ValueError`.

---

## Models

### `Indicador`

```python
class Indicador(BaseModel):
    fonte: Fonte
    produto: str = Field(..., min_length=2)
    praca: str | None = None
    data: date
    valor: Decimal = Field(..., gt=0)
    unidade: str
    metodologia: str | None = None
    revisao: int = Field(default=0, ge=0)
    meta: dict[str, Any] = Field(default_factory=dict)
    parsed_at: datetime = Field(default_factory=utcnow)
    parser_version: int = Field(default=1)
    anomalies: list[str] = Field(default_factory=list)
```

## Synchronous Version

```python
from agrobr.sync import cepea

# Same functions, no async/await
df = cepea.indicador('soja')
ultimo = cepea.ultimo('milho')
produtos = cepea.produtos()
```

## Cache Behavior

1. **Fresh cache**: returns immediately from cache (smart expiry — valid until 18:00 BRT, the CEPEA update time)
2. **Stale cache**: tries to refresh, but returns cache on failure
3. **No cache**: fetches from source and saves to cache

History accumulates progressively in the local DuckDB, allowing queries over old periods without new requests.

## Fallback

When CEPEA is unavailable (Cloudflare), agrobr automatically uses Notícias Agrícolas as a fallback source, which republishes the same CEPEA/ESALQ indicators.
