# Contract: oferta_demanda_global v1.1

Global supply and demand of agricultural commodities — USDA PSD, through the `https://api.fas.usda.gov/api/psd` gateway.

## Schema (long format)

| Column | Type | Nullable | Unit | Constraints |
|--------|------|----------|------|-------------|
| `commodity_code` | STRING | No | — | 7-digit PSD code |
| `commodity` | STRING | No | — | agrobr name (`soja`...); outside the registry, official catalog name |
| `country_code` | STRING | No | — | PSD country code, not ISO (`CH` China, `E4` EU, `00` world) |
| `country` | STRING | Yes | — | official name from the countries catalog; `World` for the aggregate |
| `market_year` | INTEGER | No | — | >= 1960; USDA marketing year, literal (`2024` = 2024/25) |
| `attribute` | STRING | No | — | official name from the `commodityAttributes` catalog |
| `attribute_br` | STRING | Yes | — | agrobr label for the balance-sheet attributes; null for the others |
| `value` | FLOAT | Yes | `unit` column | — |
| `unit` | STRING | Yes | — | official unit from the `unitsOfMeasure` catalog |
| `attribute_id` | INTEGER | No | — | PSD `attributeId` (1.1) |
| `unit_id` | INTEGER | No | — | PSD `unitId` (1.1) |
| `last_update_year` | INTEGER | No | — | >= 1960; year of the series' last update (1.1) |
| `last_update_month` | INTEGER | Yes | — | 1 to 12; month of the last update; null when the PSD publishes `00` (1.1) |

**PK:** `(commodity_code, country_code, market_year, attribute)`

Unit per row: `(1000 MT)` for most attributes, `(1000 HA)` for area, `(MT/HA)` for yield; cotton in
`1000 480 lb. Bales` (yield in `(KG/HA)`) and coffee in `(1000 60 KG BAGS)`.

`last_update_year`/`last_update_month` is the month in which the USDA last updated the series (country × marketing
year), the same as the gateway's `dataReleaseDates`. It is not the queried WASDE edition: series the month's report did
not revise keep the old month.

`attribute_br` and the balance identity (beginning stocks + production + imports = supply = exports + consumption +
losses + ending stocks) are on the [source page](../sources/usda.md#balance-sheet-attributes). Consumption is attribute
125 for most products, 126 for sugar and 142 for cotton, which also has losses (150).

Without `market_year`, the source default applies: the current calendar year or, while the PSD has published nothing
for it (January until the May WASDE), the previous one, with the year used in `source_details["market_year"]`
([source page](../sources/usda.md#api)).

## History

- **1.1 (2.0.0):** columns `attribute_id`, `unit_id`, `last_update_year` and `last_update_month`. Labels from the
  gateway's official catalogs. 1.1.0 read the old OpenData, which returns 500, and mislabeled 6 of the 9 attributes
  (production came out as beginning stocks), soybean meal (it was cottonseed oil) and the EU (it was the EU-15). See the
  [migration guide](../guides/migracao-2.md).
- **1.0 (0.13.0):** initial version.

## Pivot mode

When `pivot=True`, each attribute becomes a column: its `attribute_br` when there is one, otherwise the official name.
Two attributes with the same label in the same series raise `ParseError`. Contract validation is skipped in that case.

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
