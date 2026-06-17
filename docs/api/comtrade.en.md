# UN Comtrade API

Bilateral international trade via the UN Comtrade API. Headline feature: trade mirror — comparing exports declared by the reporter vs imports declared by the partner.

## API Key (Optional)

Works in guest mode (no key, 500 records/call). For more capacity:

1. Register at [comtradeplus.un.org](https://comtradeplus.un.org)
2. Configure: `export AGROBR_COMTRADE_API_KEY=your_key`

## Functions

### `comercio`

Bilateral trade data by product, country and period.

```python
async def comercio(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str | None = None,
    fluxo: str = "X",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str` | Product: `"soja"`, `"complexo_soja"`, `"carne_bovina"` or HS code |
| `reporter` | `str` | Reporter country: `"BR"`, `"CN"`, `"US"`. Default: `"BR"` |
| `partner` | `str \| None` | Partner country. None = World (all). Default: None |
| `fluxo` | `str` | `"X"` (export) or `"M"` (import). Default: `"X"` |
| `periodo` | `str \| int \| None` | Year, month or range: `2024`, `202401`, `"2022-2024"`. None = current year |
| `freq` | `str` | `"A"` (annual) or `"M"` (monthly). Default: `"A"` |
| `api_key` | `str \| None` | API key (or uses `AGROBR_COMTRADE_API_KEY`) |
| `as_polars` | `bool` | If True, returns polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `periodo`, `ano`, `mes`, `reporter_iso`, `partner_iso`, `fluxo_code`, `hs_code`, `produto_desc`, `peso_liquido_kg`, `volume_ton`, `valor_fob_usd`, `valor_cif_usd`, `valor_primario_usd`

### `trade_mirror`

Bilateral comparison: what the reporter exported vs what the partner imported.

```python
async def trade_mirror(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str = "CN",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Returns:**

DataFrame with columns from both sides + discrepancies:

| Column | Description |
|--------|-------------|
| `peso_liquido_kg_reporter` | Weight declared by the reporter (export) |
| `valor_fob_usd_reporter` | FOB value declared by the reporter |
| `peso_liquido_kg_partner` | Weight declared by the partner (import) |
| `valor_cif_usd_partner` | CIF value declared by the partner |
| `diff_peso_kg` | Weight difference (reporter - partner) |
| `diff_valor_fob_usd` | FOB value difference (reporter - partner) |
| `ratio_valor` | FOB reporter / CIF partner (expected ~0.85-0.95) |
| `ratio_peso` | Weight reporter / weight partner (expected ~1.0) |

### `paises`

```python
def paises() -> list[str]
```

Returns the list of accepted ISO3 codes.

### `produtos`

```python
def produtos() -> dict[str, list[str]]
```

Returns the mapping of canonical name -> list of HS codes.

## Examples

```python
from agrobr import comtrade

# BR soybean exports to China
df = await comtrade.comercio("soja", reporter="BR", partner="CN", periodo=2024)

# Trade mirror — the killer feature
df = await comtrade.trade_mirror("soja", reporter="BR", partner="CN", periodo=2024)

# Monthly with automatic chunking
df = await comtrade.trade_mirror("soja", partner="CN", freq="M", periodo="2022-2024")

# Soybean complex (grain + oil + meal)
df = await comtrade.comercio("complexo_soja", partner="CN")

# Other partners
df = await comtrade.trade_mirror("carne_bovina", partner="US")
```

## Synchronous Version

```python
from agrobr.sync import comtrade

df = comtrade.trade_mirror("soja", partner="CN")
```

## Notes

- Source: [UN Comtrade](https://comtradeapi.un.org) — free license (UN public data)
- Guest mode: 500 records/call, ~100 req/hour
- Free key: 500 calls/day, 100k records/call
- Periods > 12 months are split into chunks automatically
- Monthly data since 2000, annual since 1988
