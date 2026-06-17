# estimativa_safra v1.0

Current crop-year estimates by state.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | CONAB | Crop Monitoring (Acompanhamento de Safra) |
| 2 | IBGE LSPA | Systematic Survey of Agricultural Production |

## Products

`soja`, `milho`, `arroz`, `feijao`, `trigo`, `algodao`

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `safra` | str | ❌ | Crop year in "2024/25" format |
| `produto` | str | ❌ | Product name |
| `uf` | str | ✅ | State |
| `area_plantada` | float64 | ✅ | Planted area (thousand ha) |
| `area_colhida` | float64 | ✅ | Harvested area (thousand ha) |
| `produtividade` | float64 | ✅ | Yield (kg/ha) |
| `producao` | float64 | ✅ | Production (thousand tons) |
| `levantamento` | int | ❌ | Survey number (1-12) |
| `data_publicacao` | date | ❌ | CONAB publication date |
| `fonte` | str | ❌ | Data origin |

## Guarantees

- Estimates updated monthly
- Typical latency: M+0 (current-month data)

## Example

```python
from agrobr import datasets

# Current crop year
df = await datasets.estimativa_safra("soja")

# Specific crop year
df = await datasets.estimativa_safra("soja", safra="2024/25")

# Filter by state
df = await datasets.estimativa_safra("milho", uf="MT")

# With metadata
df, meta = await datasets.estimativa_safra("soja", return_meta=True)
```

## JSON Schema

Available at `agrobr/schemas/estimativa_safra.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("estimativa_safra")
print(contract.primary_key)  # ['safra', 'produto', 'uf', 'levantamento']
print(contract.to_json())
```

## Crop Year Format

The crop year follows the Brazilian pattern: `YYYY/YY`

- `2024/25` = crop year starting in 2024 and ending in 2025
- Summer crop: sowing Sep–Dec, harvest Jan–May
- Winter crop: sowing Feb–Apr, harvest Jun–Sep
