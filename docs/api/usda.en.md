# USDA PSD API

The USDA module provides data from Production, Supply and Distribution (PSD) — international agricultural supply and demand estimates from the U.S. Department of Agriculture.

## API Key

Requires a free USDA key:

1. Register at [api.data.gov/signup](https://api.data.gov/signup/)
2. Configure: `export AGROBR_USDA_API_KEY=your_key`

## Functions

### `psd`

Production, supply and distribution data by commodity and country.

```python
async def psd(
    commodity: str,
    *,
    country: str = "BR",
    market_year: int | None = None,
    attributes: list[str] | None = None,
    pivot: bool = False,
    api_key: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `commodity` | `str` | Commodity: `"soja"`, `"milho"`, `"trigo"`, `"cafe"`, `"arroz"`, `"algodao"`, `"acucar"`, `"farelo_soja"`, `"oleo_soja"` or a USDA code |
| `country` | `str` | Country: `"BR"`, `"US"`, `"world"` (aggregate), `"all"` (every country). Default: `"BR"` |
| `market_year` | `int \| None` | Market year. None uses the most recent |
| `attributes` | `list[str] \| None` | Filter attributes (e.g. `["Production", "Exports"]`) |
| `pivot` | `bool` | If True, pivots attributes into columns |
| `api_key` | `str \| None` | API key (or uses `AGROBR_USDA_API_KEY`) |
| `as_polars` | `bool` | If True, returns a polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `commodity_code`, `commodity`, `country_code`, `country`, `market_year`, `attribute`, `attribute_br`, `value`, `unit`

**Example:**

```python
from agrobr import usda

# Brazil soybeans
df = await usda.psd("soja")

# World corn, pivoted
df = await usda.psd("milho", country="world", pivot=True)

# Specific attributes
df = await usda.psd("soja", attributes=["Production", "Exports"])

# Several countries
df = await usda.psd("soja", country="all", market_year=2024)
```

## Synchronous Version

```python
from agrobr.sync import usda

df = usda.psd("soja")
```

## Notes

- Source: [USDA FAS](https://apps.fas.usda.gov/psdonline/) — `livre` license
- Global data for ~180 countries
- Updated monthly (WASDE report)
