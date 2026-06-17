# pib_agro v1.0

Brazilian agricultural GDP by sector and quarter.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | IBGE | Quarterly National Accounts (SIDRA) |

## Products (sectors)

`agropecuaria`, `industria`, `servicos`, `pib_total`

The dataset's `produto` parameter maps to the IBGE API's `setor` parameter.

## Schema

| Column | Type | Nullable | Unit | Stable |
|--------|------|----------|------|--------|
| `trimestre` | str | ❌ | - | Yes |
| `valor` | float | ✅ | R$ (millions) | Yes |
| `unidade` | str | ❌ | - | Yes |
| `setor` | str | ❌ | - | Yes |
| `precos` | str | ❌ | - | Yes |
| `fonte` | str | ❌ | - | Yes |

**Primary key:** `[trimestre, setor, precos]`

**Note:** `precos` in the PK is essential — without it, calls with different `precos` produce identical PKs with semantically different `valor`. `precos` is context metadata injected by the dataset (it does not come directly from the source).

## Guarantees

- Column names never change (additions only)
- `trimestre` is always in YYYYQQ format
- `setor` is one of the valid keys: agropecuaria, industria, servicos, pib_total
- `precos` indicates the deflator type: corrente, real_1995

## Example

```python
from agrobr import datasets

# Async
df = await datasets.pib_agro("agropecuaria")
df = await datasets.pib_agro("agropecuaria", trimestre="202401", precos="real_1995")

# With metadata
df, meta = await datasets.pib_agro("agropecuaria", return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.pib_agro("agropecuaria")
```

## JSON Schema

Available at `agrobr/schemas/pib_agro.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("pib_agro")
print(contract.to_json())
```
