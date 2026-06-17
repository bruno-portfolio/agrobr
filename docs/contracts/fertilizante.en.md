# fertilizante v1.0

Fertilizer deliveries by state and month.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | ANDA | National Association for Fertilizer Distribution |

## Products

`total`, `npk`, `ureia`, `map`, `dap`, `ssp`, `tsp`, `kcl`

## Schema

| Column | Type | Nullable | Unit | Stable |
|--------|------|----------|------|--------|
| `ano` | int | ❌ | - | Yes |
| `mes` | int | ❌ | - | Yes |
| `uf` | str | ✅ | - | Yes |
| `produto_fertilizante` | str | ❌ | - | Yes |
| `volume_ton` | float | ✅ | ton | Yes |

**Primary key:** `[ano, mes, uf, produto_fertilizante]`

**Constraints:** `ano >= 2000`, `mes` between 1 and 12, `volume_ton >= 0`

## Guarantees

- Column names never change (additions only)
- `ano` is always >= 2000
- `mes` between 1 and 12
- Numeric values are always >= 0

## Example

```python
from agrobr import datasets

# Async
df = await datasets.fertilizante(ano=2024)
df = await datasets.fertilizante(ano=2024, uf="MT")
df = await datasets.fertilizante(ano=2024, produto="ureia")

# With metadata
df, meta = await datasets.fertilizante(ano=2024, return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.fertilizante(ano=2024)
```

## JSON Schema

Available at `agrobr/schemas/fertilizante.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("fertilizante")
print(contract.to_json())
```

## Requirements

```bash
pip install agrobr[pdf]
```
