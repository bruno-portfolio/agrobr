# USDA PSD API

The USDA module provides data from Production, Supply and Distribution (PSD) — international agricultural supply and demand estimates from the U.S. Department of Agriculture.

## API Key

Requires a free USDA key:

1. Register at [api.data.gov/signup](https://api.data.gov/signup/)
2. Configure: `export AGROBR_USDA_API_KEY=your_key`

The key goes only in the `X-Api-Key` header of the `https://api.fas.usda.gov/api/psd` gateway. Without a key, or with a
rejected key (HTTP 403 `API_KEY_INVALID`), `SourceUnavailableError` is raised.

## Functions

### `psd`

Production, supply and distribution data by commodity and country.

```python
async def psd(
    produto: str,
    *,
    country: str = "BR",
    market_year: int | None = None,
    attributes: list[str] | None = None,
    pivot: bool = False,
    api_key: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str` | Commodity: `"soja"`, `"milho"`, `"trigo"`, `"cafe"`, `"arroz"`, `"algodao"`, `"acucar"`, `"farelo_soja"`, `"oleo_soja"` or a `commodityCode` from the official PSD catalog |
| `country` | `str` | Country: `"BR"`, `"US"`, `"world"` (aggregate), `"all"` (every country) or a `countryCode` from the PSD catalog (not ISO: `"CH"` is China, `"E4"` the EU). Default: `"BR"` |
| `market_year` | `int \| None` | Market year. `None` uses the current calendar year and, when the PSD has published nothing for it yet (January until the May WASDE), the previous year; the year used goes to `source_details["market_year"]` |
| `attributes` | `list[str] \| None` | Filter attributes by official name (e.g. `["Production", "Exports"]`) or by agrobr label (`"producao"`, `"consumo_domestico"`...) |
| `pivot` | `bool` | If True, pivots attributes into columns |
| `api_key` | `str \| None` | API key (or uses `AGROBR_USDA_API_KEY`) |
| `as_polars` | `bool` | If True, returns a polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `commodity_code`, `commodity`, `country_code`, `country`, `market_year`, `attribute`, `attribute_br`, `value`, `unit`, `attribute_id`, `unit_id`, `last_update_year`, `last_update_month`. Labels come from the official catalogs; `last_update_*` is the series' last update, not the queried edition. Details in [USDA PSD](../sources/usda.md).

**Errors:** a commodity, country or attribute outside the catalogs raises `InvalidParameterError` before the network; a body outside the gateway layout or a code the local catalog does not know raises `ParseError`.

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

`produto` accepts normalized crop names, including case and accents. `market_year` must be an integer from 1960 through the current year in Brasília. Invalid countries, attributes, year types and boolean flags raise `InvalidParameterError` before network access. Source columns retain their technical names. Integer columns use nullable `Int64`, `value` uses `float64`, and text uses the installed pandas default, including empty results.
