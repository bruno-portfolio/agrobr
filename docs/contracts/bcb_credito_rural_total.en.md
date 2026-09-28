# bcb.credito_rural_total v1.0

Rural credit by state and purpose, without product, from SICOR's `RegiaoUF` entity: the four purposes, including agro-industrialisation, which SICOR does not publish by product. Source contract of the `bcb.credito_rural_total` function; it is not a dataset.

## Source

| Source | Entity | Description |
|---|---|---|
| BCB/SICOR (OData v2, Olinda) | `RegiaoUF` | Count and value by month, state, programme, sub-programme, funding source and activity, with the four purposes as columns |

No fallback to the Base dos Dados table.

## Schema

| Column | Type | Nullable | Unit | Stable |
|---|---|---|---|---|
| `safra` | str | No | — | Yes |
| `uf` | str | No | — | Yes |
| `finalidade` | str | No | — | Yes |
| `agregacao` | str | No | — | Yes |
| `programa` | str | Yes | — | Yes |
| `cd_programa` | str | Yes | — | Yes |
| `qtd_contratos` | int | No | contracts | Yes |
| `valor` | float | No | BRL | Yes |
| `fonte` | str | No | — | Yes |

**Primary key:** `[safra, uf, finalidade, programa]`

**Constraints:** `qtd_contratos >= 0`, `valor >= 0`

- `safra` in `YYYY/YYYY` format, from July to June.
- `finalidade`: `custeio`, `investimento`, `comercializacao` or `industrializacao`.
- With `uf` aggregation, `programa` and `cd_programa` are null. With `programa` aggregation, they identify the programme, with the current name from the official table.
- `valor` is the sum in BRL, to the cent; `qtd_contratos` is the sum of contracts.

## Source rules

- **Filler zero:** in the wide row, the source carries 0 for purposes without operations. A pair with count = 0 and value = 0 does not become a row, and a purpose without operations is absent. Example: in crop year 2022/23, agro-industrialisation in AM, AP and RR and marketing in AP.
- **No Brazil row:** SICOR publishes no national total. The Brazil total is the sum of the states, and the function does not invent the row.
- **Partial crop year:** the current crop year is partial. `MetaInfo.source_details["meses"]` records the first and last month with data and the number of months.
- **Crop-year query × sum of the monthly queries:** SICOR may return, for the crop-year query, numbers that differ from the sum of month-by-month queries. On 2026-09-26, for crop year 2026/27 (July and August), 51 state × purpose pairs diverged; for `custeio` in AC, 274 contracts and R$ 56,788,261.98 in the crop-year query, which the function uses, against 272 and R$ 56,541,830.82 in the monthly ones. The cause was not identified, and agrobr reproduces the body received.
- **Consistency across entities**, checked for 2022 and 2023: the total by state and purpose equals the sum of the municipalities of `CusteioInvestimentoComercialIndustrialSemFiltros` and, for operating costs, investment and marketing, the by-product sum of the `*RegiaoUFProduto` entities read by `credito_rural`.

## Version history

| Version | Change |
|---|---|
| v1.0 | Initial contract |

## Example

```python
from agrobr import bcb

df = await bcb.credito_rural_total(safra="2022/23")
industrializacao = await bcb.credito_rural_total(safra="2022/23", finalidade="industrializacao")
por_programa = await bcb.credito_rural_total(safra="2022/23", uf="MT", agregacao="programa")
```

## JSON Schema

Available at `agrobr/schemas/bcb_credito_rural_total.json`.

```python
from agrobr.contracts import get_contract

contract = get_contract("bcb_credito_rural_total")
print(contract.to_json())
```
