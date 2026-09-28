# serie_historica_safra v1.1

Crop historical series by product, crop year, region and state.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | CONAB | Crop Historical Series |

## Products

45 products: `soja`, `milho`, `milho_1`, `milho_2`, `milho_3`, `arroz`, `arroz_irrigado`, `arroz_sequeiro`, `feijao`, `feijao_1`, `feijao_2`, `feijao_3`, `feijao_caupi`, `feijao_caupi_1`, `feijao_caupi_2`, `feijao_caupi_3`, `feijao_cores`, `feijao_cores_1`, `feijao_cores_2`, `feijao_cores_3`, `feijao_preto`, `feijao_preto_1`, `feijao_preto_2`, `feijao_preto_3`, `algodao`, `algodao_pluma`, `algodao_caroco`, `trigo`, `sorgo`, `aveia`, `cevada`, `canola`, `girassol`, `mamona`, `amendoim`, `amendoim_1`, `amendoim_2`, `centeio`, `triticale`, `gergelim`, `cafe`, `cafe_arabica`, `cafe_conilon`, `cana`, `cana_area_total`

## Product interpretation

Each product is a CONAB series: a crop, a season (`milho_1` through `milho_3`, bean seasons), or a selection (`cana_area_total`, `algodao`, `algodao_pluma`, `algodao_caroco`).

The `safra` period follows the publication: calendar year `YYYY` for `cafe`, `cafe_arabica`, `cafe_conilon`, `trigo`, `aveia`, `cevada`, `canola`, `centeio`, `triticale`; other products use `YYYY/YY`. The `inicio`/`fim` filters still use the starting year.

The forecast column (labels such as `Previsão` or `(¹)`) is excluded from the historical series; the exclusion is logged with its product, sheet and label. For the current crop year, use `estimativa_safra` for products available in that dataset.

Crop years still published by CONAB's monthly grain survey (the current and the previous one; each crop year's first survey comes out in October) may have been revised after the annual XLS. A grain query that includes those crop years emits a warning (`warnings.warn`, once per product and crop years) pointing to `conab.safras` and `estimativa_safra`; the series value is not replaced. Coffee and sugarcane have their own surveys and do not get this warning.

`algodao` represents seed cotton; `algodao_pluma`, cotton lint; `algodao_caroco`, cottonseed. All three share the same area, with production and yield from their respective selection.

`cana` publishes the Área sheet, whose title in the official spreadsheet is "Série Histórica de Área Colhida" (harvested area), in `area_colhida_mil_ha`; `area_plantada_mil_ha` stays null for this product and is not imputed from `cana_area_total`.

`regiao` is the macro-region under which the spreadsheet lists the state, recognized only by the exact label. Sub-regions (the coffee ones in Bahia and Minas Gerais) and aggregates (`NORTE/NORDESTE`, `CENTRO-SUL`, `OUTROS`) do not change the region and are not published.

`cana_area_total` reads only the Área Total sheet, and the composition CONAB publishes there changes over the series. From 2007/08 to 2021/22 (except 2016/17), it is harvested area plus planting (expansion and renewal) plus seedlings. In 2016/17, it equals harvested area in 23 states. In 2022/23, it adds harvested area and planting, without seedlings, in 19 states. From 2023/24 to 2025/26, it equals harvested area in 19 states, including SP and MG. agrobr returns the published number; comparing crop seasons in this column requires accounting for the break.

Empty Área Total cells are not filled from Área Colhida; production and yield remain null. Seedling and harvesting method sheets are excluded.

A zero published in the spreadsheet comes out as `0.0`, including a state with no production in that season (the spreadsheet lists all 27 states). The exception is a season column that is zero in every state: the season was not surveyed and yields no row. Wheat 1976 is zero even in the BRASIL row, and canola, triticale, sunflower, 3rd-season beans and 2nd-season corn start that way. Across the 40 grain and sugarcane spreadsheets of September 2026, 7,533 zeros belong to unsurveyed seasons; the other 43,981 published zeros come out as `0.0`. Coffee has no such column.

Unreadable, missing or ambiguous selected sheets raise `ParseError`; filters with no observations return an empty table with its schema. Unknown sheets produce a warning.

## Schema

| Column | Type | Nullable | Unit | Stable |
|--------|------|----------|------|--------|
| `produto` | str | ❌ | - | Yes |
| `safra` | str | ❌ | - | Yes |
| `regiao` | str | ✅ | - | Yes |
| `uf` | str | ✅ | - | Yes |
| `area_plantada_mil_ha` | float | ✅ | thousand ha | Yes |
| `area_em_producao_mil_ha` | float | ✅ | thousand ha | No (optional, since 1.1) |
| `area_formacao_mil_ha` | float | ✅ | thousand ha | No (optional, since 1.1) |
| `area_colhida_mil_ha` | float | ✅ | thousand ha | No (optional, since 1.1) |
| `producao_mil_ton` | float | ✅ | thousand tons | Yes |
| `produtividade_kg_ha` | float | ✅ | kg/ha | Yes |

**Primary key:** `[produto, safra, regiao, uf]`

**Constraints:** `area_plantada_mil_ha >= 0`, `producao_mil_ton >= 0`, `produtividade_kg_ha >= 0`

## Guarantees

- Unique PK per product + crop year + region + state combination
- `produto` is lowercase (e.g. soja, milho_2)
- `safra` is the period published by CONAB: YYYY/YY (e.g. 2023/24) or YYYY (e.g. 2025; coffee and winter cereals)
- `regiao` when present: NORTE, NORDESTE, CENTRO-OESTE, SUDESTE, SUL
- `uf` when present: 2-letter uppercase state code
- Metrics (area, production, yield) >= 0 when present

For coffee, `area_plantada_mil_ha` is the sum of producing and developing areas;
it remains null when either component is unavailable. Both official areas are
preserved separately. Production in thousand 60 kg bags of processed coffee is
converted to thousand tonnes (× 0.06); yield in bags/ha is converted to kg/ha
(× 60) and remains based on the producing area.

`cana_industria` was removed from advertised support in package version 2.0:
sugar, ethanol and ATR metrics require their own contract.

## Example

```python
from agrobr import datasets

# Async
df = await datasets.serie_historica_safra("soja")
df = await datasets.serie_historica_safra("soja", inicio=2020, fim=2024, uf="MT")

# With metadata
df, meta = await datasets.serie_historica_safra("soja", return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.serie_historica_safra("soja")
```

## JSON Schema

Available at `agrobr/schemas/serie_historica_safra.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("serie_historica_safra")
print(contract.to_json())
```
