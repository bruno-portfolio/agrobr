# lspa v1.0

Systematic Survey of Agricultural Production — monthly IBGE estimates.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | IBGE LSPA | Systematic Survey of Agricultural Production |

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ano` | int | ❌ | Reference year (>= 1974) |
| `mes` | int | ✅ | Reference month (1-12) |
| `produto` | str | ❌ | Product name |
| `variavel` | str | ✅ | Measured variable |
| `valor` | float64 | ✅ | Measurement value |
| `fonte` | str | ❌ | Always `ibge_lspa` |

## Primary Key

`[ano, mes, produto]`

## Guarantees

- Column names never change (additions only)
- `ano` is always a valid year
- `mes` is between 1 and 12 when present
- `fonte` is always `ibge_lspa`

## JSON Schema

Available at `agrobr/schemas/lspa.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("lspa")
print(contract.to_json())
```
