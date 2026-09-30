# CEPEA API

The CEPEA module provides access to price indicators from the Center for Advanced Studies in Applied Economics (ESALQ/USP).

## Functions

### `indicador`

Retrieves the historical series of price indicators.

The page publishes a recent window, usually around 15 trading days; the earlier
period comes from CEPEA's historical series, downloaded whole the first time and kept
in the cache (oranges have no series; see [the source](../sources/cepea.md#coverage-and-series-selection)). Ethanol is weekly; milk is monthly,
with `data` set to the first day of the reference month and `praca` identifying
the state.

```python
async def indicador(
    produto: str,
    praca: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    _moeda: str = "BRL",
    *,
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
| `praca` | `str \| None` | Quotation location. Accepts a slug from `pracas()` or the source display label; `None` returns all |
| `inicio` | `str \| date \| None` | Start date (YYYY-MM-DD). Default: 365 days ago |
| `fim` | `str \| date \| None` | End date. Default: today |
| `_moeda` | `str` | Reserved; has no effect on the result |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `validate_sanity` | `bool` | Check unit, price range and temporal change when a rule exists. Default: `False` |
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
- `anomalies`: JSON text containing the anomaly/marker list (e.g. `["out_of_range: valor"]`); `None` when empty. Use `json.loads()` to recover the list. The `Indicador` model returned by `ultimo()` retains `list[str]`.
- `valor_usd`: USD price published by CEPEA in the same row; `NaN` when the source does not publish it (Notícias Agrícolas fallback, history cached before migration 10)
- `peso_medio_kg`: Average animal weight from the page's auxiliary table (calf, MS); `NaN` for other products

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

# Filter with a slug from pracas(); the DataFrame preserves "Paranaguá/PR"
df = await cepea.indicador('soja', praca='paranagua')

# Force refresh
df = await cepea.indicador('soja', force_refresh=True)

# Offline mode (no network)
df = await cepea.indicador('soja', offline=True)
```

---

### `ultimo`

Retrieves the most recent available indicator.

For milk, accounts for the monthly publication lag and returns the latest
reference month, not the publication date. Use `praca` to select the state.

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
| `praca` | `str \| None` | Quotation location. Accepts a slug from `pracas()` or the source display label; `None` does not filter |
| `offline` | `bool` | Use local cache only |

**Returns:**

An `Indicador` object with:
- `data`: Indicator date
- `valor`: Value as Decimal
- `unidade`: Unit (e.g. 'BRL/sc60kg')
- `produto`: Product name
- `fonte`: Data source

With no network and no indicator in the recent cache, it raises `SourceUnavailableError`, with `attempted_sources` (up to 1.1.0, `ParseError`). With `offline=True` and no indicator in the cache, the same error, with the reason "offline sem dado no cache".

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
# ['soja', 'soja_parana', 'milho', 'bezerro', 'cafe', 'cafe_arabica', 'cafe_robusta',
#  'boi', 'boi_gordo', 'trigo', 'algodao', 'arroz', 'acucar', 'acucar_refinado',
#  'frango_congelado', 'frango_resfriado', 'suino', 'etanol_hidratado',
#  'etanol_anidro', 'leite', 'laranja_industria', 'laranja_in_natura']
# 'bezerro' = 8–12-month-old calf, Mato Grosso do Sul, BRL/cabeca; exposes valor_usd and peso_medio_kg
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

List of parser-mapped locations as normalized slugs accepted by `indicador()` and `ultimo()`. The DataFrame and `Indicador` model preserve the label displayed by the source. The list is empty for a valid product without a mapped location; an unknown product raises `InvalidParameterError`.

```python
soy_locations = await cepea.pracas('soja')
# ['paranagua'] — corresponds to the "Paranaguá/PR" label in the data

wheat_locations = await cepea.pracas('trigo')
# ['parana', 'rio_grande_do_sul'] — the page publishes one table per location, and
# indicador() returns both; datasets.preco_diario uses Paraná
```

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

In an `Indicador` collected from CEPEA, `meta` holds the variations as published text: `variacao` is the daily one
("Var./Dia"), `variacao_mes` the monthly one ("Var./Mês") and `variacao_semana` the weekly one ("Var./semana", in the weekly
ethanol indicators). The variations come only in an `Indicador` freshly collected from the source. A cache read, warm or
`offline`, carries the value, the unit, `anomalies` and, in `meta`, only `valor_usd` and `peso_medio_kg`.

## Synchronous Version

```python
from agrobr.sync import cepea

# Same functions, no async/await
df = cepea.indicador('soja')
ultimo = cepea.ultimo('milho')
produtos = cepea.produtos()
```

## Cache Behavior

1. **Fresh cache**: returns immediately from cache. The product's latest collection is valid until 18:00 BRT of the next business day (the CEPEA update time; Saturday and Sunday do not count)
2. **Stale cache**: fetches again; if the source fails, returns the cache with `StaleDataWarning` and `source="cache_fallback"`
3. **No cache**: fetches from source and saves to cache

With `return_meta=True`, the `MetaInfo` of a cached response carries the actual collection time in `fetched_at` and `fetch_timestamp` (the most recent among the returned rows), not the call time, and `cache_expires_at` is the 18:00 BRT turnover following that collection. `ultimo()` follows the same turnover. With `fim` before the recent 25-calendar-day window (a closed period, which never goes back to the source), `cache_expires_at` is null: the validity does not apply.

History accumulates progressively in the local DuckDB, allowing queries over old periods without new requests.

## Fallback

When CEPEA is unavailable (Cloudflare), agrobr automatically uses Notícias Agrícolas as a fallback source, which republishes the same CEPEA/ESALQ indicators.

On the first call to `indicador()` or `ultimo()`, the module emits a `UserWarning`: CEPEA data is licensed under CC BY-NC 4.0, and commercial use requires authorization from CEPEA (`cepea@usp.br`). The Notícias Agrícolas fallback keeps its own `restrito` license warning; see `docs/licenses.md`.

Monthly milk uses CEPEA collection and its cache only: the Notícias Agrícolas fallback is disabled for this product. Its standalone parser exposes the closing date, whereas CEPEA uses the reference month. Previously cached NA milk rows are preserved in quarantine by migration 9. Milk history comes from the series with 2 decimals; the page, with 4, wins when both exist.

Cache upgrades retain affected originals in `indicadores_quarentena`; migration
failures raise `CacheMigrationError` without completing removal from active
data. See [audit and recovery](../guides/migracao-2.md#18-automatic-preservation-of-existing-caches).

With `return_meta=True`, `selected_source="cache"` identifies local retrieval,
`attempted_sources` distinguishes normal cache from use after failure, and
`data_sources` identifies providers of returned rows. Warm/offline Notícias
Agrícolas records are not reported as a CEPEA fetch. In a cached response,
`parser_version` is the parser version stored with the returned rows (the
highest, if there is more than one); when the rows carry a version other than
the reported one, `source_details["parser_versions"]` lists the versions per
provider, for example `{"cepea": [2], "noticias_agricolas": [3]}`. The direct API emits
`StaleDataWarning` when cache follows failure; `datasets.preco_diario` also emits
`SourceFallbackWarning`, which can be promoted to an exception.

For the same date, product, and normalized location, the CEPEA observation
takes precedence over Notícias Agrícolas, including warm and offline cache
reads. A new collection from the same provider replaces its earlier
observation. DuckDB retains both providers' observations; selection happens
at query time. `indicador()` keeps distinct locations, and `data_sources`
describes only returned rows. `force_refresh=True` still bypasses the initial
cache read and returns the requested collection.
