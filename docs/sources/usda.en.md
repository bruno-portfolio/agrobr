# USDA PSD — International Estimates

United States Department of Agriculture — Production, Supply, Distribution.
International estimates of agricultural production, supply and demand.

## Configuration

Requires a free USDA API key:

1. Register at [api.data.gov/signup](https://api.data.gov/signup/)
2. Set the environment variable:

```bash
export AGROBR_USDA_API_KEY="sua-key-aqui"
```

Or pass it directly:

```python
df = await usda.psd("soja", api_key="sua-key-aqui")
```

## API

```python
from agrobr import usda

# PSD data for Brazil for soybeans
df = await usda.psd("soja", country="BR", market_year=2024)

# All countries
df = await usda.psd("soja", country="all", market_year=2024)

# Aggregated world data
df = await usda.psd("milho", country="world")

# Filter by attributes
df = await usda.psd("soja", attributes=["Production", "Exports"])

# Pivot attributes as columns
df = await usda.psd("soja", pivot=True)
```

## Columns — `psd`

| Column | Type | Description |
|---|---|---|
| `commodity_code` | str | USDA commodity code |
| `commodity` | str | Commodity name |
| `country_code` | str | Country ISO code |
| `country` | str | Country name |
| `market_year` | int | Marketing year |
| `attribute` | str | Attribute (Production, Exports, etc.) |
| `attribute_br` | str | Translated attribute (PT-BR) |
| `value` | float | Value |
| `unit` | str | Unit |

## Commodities

| agrobr name | USDA Commodity |
|---|---|
| `soja` | Oilseed, Soybean |
| `milho` | Corn |
| `trigo` | Wheat |
| `algodao` | Cotton |
| `arroz` | Rice, Milled |
| `cafe` / `coffee` | Coffee, Green |
| `acucar` / `sugar` | Sugar, Centrifugal |
| `farelo_soja` / `soybean_meal` | Soybean Meal |
| `oleo_soja` / `soybean_oil` | Soybean Oil |

## MetaInfo

```python
df, meta = await usda.psd("soja", return_meta=True)
print(meta.source)  # "usda"
print(meta.source_method)  # "httpx"
```

## Source

- API: `https://apps.fas.usda.gov/OpenData/api/psd`
- Format: JSON (REST API)
- Update: monthly (WASDE reports)
- History: 1960+
