# producao_anual v1.0

Consolidated annual agricultural output by state or municipality.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | IBGE PAM | Municipal Agricultural Production |
| 2 | CONAB | Crop Monitoring |

## Products

`soja`, `milho`, `arroz`, `feijao`, `trigo`, `algodao`, `cafe`, `cacau`

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ano` | int | ❌ | Reference year |
| `produto` | str | ❌ | Product name |
| `localidade` | str | ✅ | State or municipality |
| `area_plantada` | float64 | ✅ | Planted area (ha) |
| `area_colhida` | float64 | ✅ | Harvested area (ha) |
| `producao` | float64 | ✅ | Production (tons) |
| `rendimento` | float64 | ✅ | Yield (kg/ha) |
| `valor_producao` | float64 | ✅ | Output value (thousand reais) |
| `fonte` | str | ❌ | Data origin |

## Primary Key

`[ano, produto, localidade]`

## Guarantees

- Consolidated data for the complete crop year
- Typical latency: Y+1 (data available the following year)

## Example

```python
from agrobr import datasets

# Output by state
df = await datasets.producao_anual("soja", ano=2023)

# Output by municipality
df = await datasets.producao_anual("milho", ano=2023, nivel="municipio")

# Filter by state
df = await datasets.producao_anual("soja", ano=2023, uf="MT")

# With metadata
df, meta = await datasets.producao_anual("soja", ano=2023, return_meta=True)
```

## JSON Schema

Available at `agrobr/schemas/producao_anual.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("producao_anual")
print(contract.to_json())
```

## Territorial Levels

| Level | Description |
|-------|-------------|
| `brasil` | National total |
| `uf` | By state (default) |
| `municipio` | By municipality |
