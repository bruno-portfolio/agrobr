# preco_atacado v2.0

Wholesale prices at Brazilian CEASAs (CONAB/PROHORT).

The current contract is `PRECO_ATACADO_V2`, effective since agrobr 2.0.0.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | CONAB CEASA | PROHORT — daily fruit and vegetable prices |

## Products

48 PROHORT products in agrobr's table; without a product filter, a newly published product comes out with a
null `categoria` and a warning. The requested product and CEASA are checked after the
network call, against what the body publishes (the product ignoring accents and case), listing the valid
ones; a published product outside the 48 in `conab.ceasa_produtos()` also filters.

## Schema

| Column | Type | Nullable | Unit | Stable |
|--------|------|----------|------|--------|
| `data` | date | ❌ | - | Yes |
| `produto` | str | ❌ | - | Yes |
| `categoria` | str | ✅ | - | Yes |
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
- `categoria` is FRUTAS, HORTALICAS or OVOS; null only for a product outside agrobr's table, with a warning (2.0)

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
