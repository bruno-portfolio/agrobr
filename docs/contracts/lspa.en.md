# lspa v2.0

Systematic Survey of Agricultural Production — monthly IBGE estimates.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | IBGE LSPA | Systematic Survey of Agricultural Production |

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ano` | int | ❌ | Reference year (>= 1974) |
| `mes` | int | ❌ | Reference month (1-12), including annual queries |
| `localidade` | str | ❌ | Locality name |
| `localidade_cod` | int | ❌ | IBGE locality code |
| `produto` | str | ❌ | Product name |
| `variavel` | str | ❌ | Measured variable: planted area, harvested area, production or yield |
| `variavel_cod` | int | ❌ | SIDRA variable code |
| `valor` | float64 | ✅ | Measurement in the unit specified in the row |
| `unidade` | str | ❌ | Unit published by SIDRA, such as Hectares or Toneladas |
| `fonte` | str | ❌ | Always `ibge_lspa` |

## Primary Key

`[ano, mes, produto, localidade, variavel]`

## Guarantees

- Column names never change (additions only)
- `ano` is always a valid year
- `mes` is always present and between 1 and 12
- Each row identifies a month, product, locality and variable; measurements with different units remain separate
- `fonte` is always `ibge_lspa`

Version 2.0 corrects the identity of table 6588 dimensions and extends the primary key.
`mes=None` returns the available months of the requested year. The `estimativa_safra`
dataset retains its aggregate output and the units specified by its own contract.

Since 2012, `produto="cafe"` queries arabica and canephora, retaining each species in the
`produto` column, just as `milho` retains its two crop seasons. To obtain total
coffee production or area, add the species values for the same month and
territory. Compute total yield as production in tonnes × 1,000 / harvested area
in hectares; species yields are not additive.

Before 2012, `produto="cafe"` returns the official total with `produto="cafe"`,
without inventing a species breakdown. Explicit species queries for those years
preserve SIDRA's unavailable values. Separate publication began in January 2012,
as explained in the [IBGE note on the historical series](https://www.ibge.gov.br/en/highlights/20651-ibge-makes-available-monthly-time-series-of-systematic-survey-of-agricultural-production-lspa.html?lang=en-GB).

`produto="batata"` queries all three potato crop seasons (`batata_1`, `batata_2`,
`batata_3`) and retains each in the `produto` column. `algodao` represents seed
cotton; production is not cotton lint alone.

## JSON Schema

Available at `agrobr/schemas/lspa.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("lspa")
print(contract.to_json())
```
