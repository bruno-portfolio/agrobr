# exportacao v1.0

Brazilian agricultural exports by product, state and month.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | ComexStat | Official MDIC data by NCM |
| 2 | ABIOVE | Fallback with unit normalization |

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

## Guarantees

- Column names never change (additions only)
- `ano` is always >= 1997
- `mes` between 1 and 12
- Numeric values are always >= 0

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
