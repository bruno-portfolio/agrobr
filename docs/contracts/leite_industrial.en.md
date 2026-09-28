# leite_industrial v1.0

Quarterly milk acquisition and processing by state.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | IBGE Milk | Quarterly Milk Survey |

## Products

`leite`

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `trimestre` | str | ❌ | Quarter YYYYQQ |
| `localidade` | str | ✅ | State |
| `localidade_cod` | int | ✅ | IBGE code |
| `leite_adquirido` | float64 | ✅ | Raw milk acquired (thousand liters) |
| `leite_industrializado` | float64 | ✅ | Raw milk processed (thousand liters) |
| `preco_medio` | float64 | ✅ | Average price paid to producer (BRL/liter) |
| `fonte` | str | ❌ | Data origin |

## Primary Key

`[trimestre, localidade]`

## Guarantees

- Quarterly data with 3 variables in wide format
- Typical latency: Q+2 months
- Historical series since 1997

## Notes

- The state is that of the dairy that bought the milk, not where it was produced.
- There is no Brazil row: the query is by state (SIDRA territorial level 3).
- A state under statistical confidentiality comes out null, because SIDRA publishes "X" (for example, RR and DF), and some
  states are not published by SIDRA (AP does not appear). That is why the sum of the states falls below the Brazil total
  released by IBGE.

## Example

```python
from agrobr import datasets

# Quarterly milk by state
df = await datasets.leite_industrial(trimestre="202303")

# Filter by state
df = await datasets.leite_industrial(trimestre="202303", uf="MG")

# With metadata
df, meta = await datasets.leite_industrial(trimestre="202303", return_meta=True)
```

## JSON Schema

Available at `agrobr/schemas/leite_industrial.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("leite_industrial")
print(contract.to_json())
```
