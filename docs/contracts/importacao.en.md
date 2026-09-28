# importacao v1.2

Brazilian agricultural imports by product, state and month.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | ComexStat | Official MDIC data by NCM |

## Products

`soja`, `milho`, `cafe`, `algodao`, `acucar`, `farelo_soja`, `oleo_soja`

## Schema

| Column | Type | Nullable | Unit | Stable |
|--------|------|----------|------|--------|
| `ano` | int | ❌ | - | Yes |
| `mes` | int | ❌ | - | Yes |
| `produto` | str | ❌ | - | Yes |
| `uf` | str | ✅ | - | Yes |
| `kg_liquido` | float | ✅ | kg | Yes |
| `valor_fob_usd` | float | ✅ | USD | Yes |
| `valor_frete_usd` | float | ✅ | USD | No |
| `valor_seguro_usd` | float | ✅ | USD | No |
| `volume_ton` | float | ✅ | t (`kg_liquido` / 1000) | No |

When published, `valor_frete_usd` and `valor_seguro_usd` separately retain
freight and insurance values in US dollars (USD). These columns are optional in the contract.

**Primary key:** `[ano, mes, produto, uf]`

The dataset returns only these columns, in this order.

**Constraints:** `ano >= 1997`, `mes` between 1 and 12, `kg_liquido >= 0`, `valor_fob_usd >= 0`

## Product semantics

Each product sums every NCM code that makes it up, with the codes in force in each year
("included / not included" table in the [ComexStat API](../api/comexstat.md)):

- `soja`: soybeans, whether or not broken, except seed (`12019000`; `12010090` until 2013).
- `milho`: the whole of heading `1005`.
- `cafe`: not roasted and roasted, decaffeinated or not (`09011`, `09012`); husks and
  substitutes (`09019000`) and soluble coffee (`2101`) are excluded.
- `algodao`: `5201` (not carded or combed) and `5203` (carded or combed); waste (`5202`),
  yarn and fabrics are excluded.
- `acucar`: the whole of heading `1701` (raw cane and beet sugar and refined sugar).
- `farelo_soja`: the whole of heading `2304` (flours and pellets and cake and other residues).
- `oleo_soja`: the whole of heading `1507` (crude, refined and other).

The adapter consolidates each product's codes by `ano`, `mes`, and `uf` before validating
the primary key; the `ncm` column is not part of the dataset output (per-NCM detail is in
`agrobr.comexstat`).

## Guarantees

- Column names never change (additions only)
- `ano` is always >= 1997
- `mes` between 1 and 12
- Numeric values are always >= 0

## Example

```python
from agrobr import datasets

# Async
df = await datasets.importacao("soja", ano=2024)
df = await datasets.importacao("soja", ano=2024, uf="SP")

# With metadata
df, meta = await datasets.importacao("soja", ano=2024, return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.importacao("soja", ano=2024)
```

## JSON Schema

Available at `agrobr/schemas/importacao.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("importacao")
print(contract.to_json())
```
