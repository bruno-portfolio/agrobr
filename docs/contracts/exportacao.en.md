# exportacao v1.0

Brazilian agricultural exports by product, state and month.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | ComexStat | Official MDIC data by NCM |
| 2 | ABIOVE | National fallback with unit normalization; unavailable with a state filter |

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

**Primary key:** `[ano, mes, produto, uf]`

**Constraints:** `ano >= 1997`, `mes` between 1 and 12, `kg_liquido >= 0`, `valor_fob_usd >= 0`

## Product semantics

- `oleo_soja` covers the complete NCM heading `1507`, including crude,
  refined, and other soybean oils. The adapter consolidates the different NCM
  codes by `ano`, `mes`, and `uf` before validating the primary key.
- `oleo_soja_bruto` remains available in the standalone ComexStat API as the
  specific code `15071000`, but it is not part of this dataset's vocabulary.
- The ABIOVE fallback uses the generic `oleo` category, equivalent to the
  scope of `oleo_soja`, and returns the dataset's canonical product name.

## Guarantees

- Column names never change (additions only)
- `ano` is always >= 1997
- `mes` between 1 and 12
- Numeric values are always >= 0

## Fallback behavior

- When `ano` is omitted, the latest complete calendar year is used.
- ABIOVE publishes national totals only. When `uf` is provided, the fallback
  does not replace the requested state breakdown with those totals; if
  ComexStat is unavailable, the call raises `SourceUnavailableError`.
- The adapter maps `soja`, `farelo_soja`, `oleo_soja`, and `milho` to ABIOVE's
  product names and restores the canonical name in the output. The `uf` column
  is null for these national totals.
- ABIOVE does not provide `cafe`, `algodao`, or `acucar`. For these products, a
  ComexStat failure ends the cascade without downloading the fallback file.

## Example

```python
from agrobr import datasets

# Async
df = await datasets.exportacao("soja", ano=2024)
df = await datasets.exportacao("soja", ano=2024, uf="MT")

# With metadata
df, meta = await datasets.exportacao("soja", ano=2024, return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.exportacao("soja", ano=2024)
```

## JSON Schema

Available at `agrobr/schemas/exportacao.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("exportacao")
print(contract.to_json())
```
