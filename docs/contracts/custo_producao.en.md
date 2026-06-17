# custo_producao v1.0

Detailed production costs by crop, state and crop year.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | CONAB | Production costs per hectare |

## Products

`soja`, `milho`, `arroz`, `feijao`, `trigo`, `algodao`, `cafe`

## Schema

| Column | Type | Nullable | Unit | Stable |
|--------|------|----------|------|--------|
| `cultura` | str | ❌ | - | Yes |
| `uf` | str | ❌ | - | Yes |
| `safra` | str | ❌ | - | Yes |
| `tecnologia` | str | ✅ | - | Yes |
| `categoria` | str | ❌ | - | Yes |
| `item` | str | ❌ | - | Yes |
| `unidade` | str | ✅ | - | Yes |
| `quantidade_ha` | float | ✅ | unit/ha | Yes |
| `preco_unitario` | float | ✅ | BRL | Yes |
| `valor_ha` | float | ✅ | BRL/ha | Yes |
| `participacao_pct` | float | ✅ | % | Yes |

**Primary key:** `[cultura, uf, safra, categoria, item]`

**Constraints:** `quantidade_ha >= 0`, `preco_unitario >= 0`

## Guarantees

- Column names never change (additions only)
- `safra` is always in YYYY/YY format
- `uf` is always a valid Brazilian state code

## Example

```python
from agrobr import datasets

# Async
df = await datasets.custo_producao("soja", uf="MT", safra="2024/25")
df = await datasets.custo_producao("milho", uf="PR")

# With metadata
df, meta = await datasets.custo_producao("soja", uf="MT", return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.custo_producao("soja", uf="MT", safra="2024/25")
```

## JSON Schema

Available at `agrobr/schemas/custo_producao.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("custo_producao")
print(contract.to_json())
```
