# preco_atacado v1.0

Wholesale prices at Brazilian CEASAs (CONAB/PROHORT).

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | CONAB CEASA | PROHORT — daily fruit and vegetable prices |

## Products

48+ dynamic products from PROHORT (validation delegated to the source).

## Schema

| Column | Type | Nullable | Unit | Stable |
|--------|------|----------|------|--------|
| `data` | date | ❌ | - | Yes |
| `produto` | str | ❌ | - | Yes |
| `categoria` | str | ❌ | - | Yes |
| `unidade` | str | ❌ | - | Yes |
| `ceasa` | str | ❌ | - | Yes |
| `ceasa_uf` | str | ❌ | - | Yes |
| `preco` | float | ❌ | BRL | Yes |

**Primary key:** `[data, produto, ceasa]`

**Constraints:** `preco >= 0`

## Guarantees

- Column names never change (additions only)
- `data` is always in date format
- `ceasa_uf` is a 2-letter uppercase state code
- Monetary values are in BRL
- `preco` is always > 0 (nulls filtered)

## Example

```python
from agrobr import datasets

# Async — all products
df = await datasets.preco_atacado()

# Filter by product
df = await datasets.preco_atacado("TOMATE")

# Filter by CEASA
df = await datasets.preco_atacado(ceasa="CEAGESP")

# With metadata
df, meta = await datasets.preco_atacado(return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.preco_atacado()
```

## JSON Schema

Available at `agrobr/schemas/preco_atacado.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("preco_atacado")
print(contract.to_json())
```
