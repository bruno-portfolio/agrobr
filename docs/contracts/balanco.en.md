# balanco v1.0

Supply/demand balance for commodities.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | CONAB | Supply and Demand Balance |

## Products

`soja`, `milho`, `arroz`, `feijao`, `trigo`, `algodao`

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `safra` | str | ❌ | Crop year in "2024/25" format |
| `produto` | str | ❌ | Product name |
| `estoque_inicial` | float64 | ✅ | Opening stock (thousand tons) |
| `producao` | float64 | ✅ | Production (thousand tons) |
| `importacao` | float64 | ✅ | Imports (thousand tons) |
| `suprimento` | float64 | ✅ | Total supply (thousand tons) |
| `consumo` | float64 | ✅ | Domestic consumption (thousand tons) |
| `exportacao` | float64 | ✅ | Exports (thousand tons) |
| `estoque_final` | float64 | ✅ | Ending stock (thousand tons) |
| `fonte` | str | ❌ | Data origin |

## Guarantees

- Complete supply/demand balance
- Updated monthly along with CONAB surveys

## Example

```python
from agrobr import datasets

# Current crop-year balance
df = await datasets.balanco("soja")

# Specific crop-year balance
df = await datasets.balanco("soja", safra="2024/25")

# With metadata
df, meta = await datasets.balanco("soja", return_meta=True)
```

## Balance Components

```
Supply = Opening Stock + Production + Imports
Demand = Consumption + Exports
Ending Stock = Supply - Demand
```

## JSON Schema

Available at `agrobr/schemas/balanco.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("balanco")
print(contract.primary_key)  # ['safra', 'produto']
print(contract.to_json())
```

## Units

All numeric values are in **thousand tons**.
