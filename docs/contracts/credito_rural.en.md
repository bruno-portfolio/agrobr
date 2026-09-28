# credito_rural v2.0

Rural credit by crop and state through the semantic layer. The contract represents the two levels actually provided by the SICOR endpoint: aggregation by state (the default) or by program. Record by record (`agregacao="registro"`) has its own contract, with 23 columns: [bcb.credito_rural_registro](./bcb_credito_rural_registro.en.md).

## Sources

| Priority | Source | Description |
|---|---|---|
| 1 | BCB/SICOR (OData) | Official Banco Central API |
| 2 | BigQuery (Base dos Dados) | Fallback for network failures or HTTP 5xx responses, only for the `uf` aggregation without `programa` or `tipo_seguro`: the table aggregates by municipality and carries no programme or insurance |

## Products

`soja`, `milho`, `cafe`, `algodao`, `trigo`, `arroz`, `feijao`, `cana`, `mandioca`, `sorgo`

## Schema

| Column | Type | Nullable | Unit | Stable |
|---|---|---|---|---|
| `safra` | str | No | — | Yes |
| `produto` | str | No | — | Yes |
| `uf` | str | Yes | — | Yes |
| `finalidade` | str | No | — | Yes |
| `agregacao` | str | No | — | Yes |
| `programa` | str | Yes | — | Yes |
| `cd_programa` | str | Yes | — | Yes |
| `qtd_contratos` | int | Yes | contracts | Yes |
| `valor` | float | Yes | BRL | Yes |
| `area_financiada` | float | Yes | ha | Yes |
| `fonte` | str | No | — | Yes |

**Primary key:** `[safra, produto, uf, finalidade, programa]`

**Constraints:** `qtd_contratos >= 0`, `valor >= 0`, `area_financiada >= 0`

For `uf` aggregation, `programa` and `cd_programa` are null. For `programa` aggregation, they identify the grouped dimension.

The crop year in progress (July to June, by today's date) is flagged: a warning in `validation_warnings` and `UserWarning`, plus `source_details["safra_em_curso"]` and `source_details["meses_cobertos"]`. Its total changes until the crop year ends.

## Version history

| Version | Change |
|---|---|
| v1.0 | Initial schema |
| v1.1 | Optional dimensions added |
| v2.0 | Schema aligned with the actual SICOR modes; removes `volume` and dimensions not present in aggregated output; default changes from `municipio` to `uf` |

## Example

```python
from agrobr import datasets

df = await datasets.credito_rural("soja", safra="2024/25")
df_by_program = await datasets.credito_rural(
    "soja",
    safra="2024/25",
    agregacao="programa",
)
df_records = await datasets.credito_rural("soja", safra="2024/25", agregacao="registro")
```

The by-product entities agrobr reads (`*RegiaoUFProduto`) have no municipality. `agregacao="municipio"` raises `InvalidParameterError`; SICOR publishes municipality by product (`CusteioMunicipioProduto` and `InvestMunicipioProduto`), which agrobr does not read, and the `agrobr[bigquery]` extra has municipality-level data. Agro-industrialisation is not published by product: it is in [bcb.credito_rural_total](./bcb_credito_rural_total.en.md).

## JSON Schema

Available at `agrobr/schemas/credito_rural.json`.

```python
from agrobr.contracts import get_contract

contract = get_contract("credito_rural")
print(contract.to_json())
```

## Fallback requirements

```bash
pip install agrobr[bigquery]
```
