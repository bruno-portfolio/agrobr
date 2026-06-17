# Contract: oferta_demanda_global

Global supply and demand of agricultural commodities — USDA PSD.

## Schema (long format)

| Column | Type | Nullable | Unit | Constraints |
|--------|------|----------|------|-------------|
| `commodity_code` | STRING | No | — | PSD 7-digit code |
| `commodity` | STRING | No | — | pt-br |
| `country_code` | STRING | No | — | ISO 2-letter |
| `country` | STRING | Yes | — | — |
| `market_year` | INTEGER | No | — | >= 1960 |
| `attribute` | STRING | No | — | English name |
| `attribute_br` | STRING | Yes | — | pt-br name |
| `value` | FLOAT | Yes | 1000 MT / 1000 HA | — |
| `unit` | STRING | Yes | — | — |

**PK:** `(commodity_code, country_code, market_year, attribute)`

## Pivot mode

When `pivot=True`, dynamic columns are generated (one per attribute). Contract validation is skipped in that case.

## Example

```python
from agrobr import datasets

# Brazil soybeans — long format
df = await datasets.oferta_demanda_global("soja")

# Pivot (attributes as columns)
df = await datasets.oferta_demanda_global("soja", pivot=True)

# Another country + specific year
df = await datasets.oferta_demanda_global("milho", country="US", market_year=2023)

# With metadata
df, meta = await datasets.oferta_demanda_global("soja", return_meta=True)
```
