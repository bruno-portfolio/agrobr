# silvicultura v1.0

Silvicultural output (eucalyptus, pine, charcoal, timber) by state or municipality.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | IBGE PEVS | Plant Extraction and Silviculture Production |

## Products

`carvao`, `lenha`, `madeira_tora`, `madeira_celulose`, `madeira_outras_finalidades`, `acacia_negra`, `eucalipto_folha`, `resina`

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ano` | int | ❌ | Reference year |
| `localidade` | str | ✅ | State or municipality |
| `localidade_cod` | int | ✅ | IBGE code |
| `produto` | str | ❌ | Product name |
| `valor` | float64 | ✅ | Value (tons or cubic meters) |
| `unidade` | str | ❌ | Unit of measure |
| `fonte` | str | ❌ | Data origin |

## Primary Key

`[ano, produto, localidade]`

## Guarantees

- Consolidated annual data
- Typical latency: Y+1 (data available the following year)
- Historical series since 1986

## Example

```python
from agrobr import datasets

# Timber (logs) output by state
df = await datasets.silvicultura("madeira_tora", ano=2023)

# Charcoal in Minas Gerais
df = await datasets.silvicultura("carvao", ano=2023, uf="MG")

# Eucalyptus planted area (via ibge directly)
from agrobr import ibge
df = await ibge.silvicultura("eucalipto", variavel="area")

# With metadata
df, meta = await datasets.silvicultura("madeira_tora", ano=2023, return_meta=True)
```

## JSON Schema

Available at `agrobr/schemas/silvicultura.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("silvicultura")
print(contract.to_json())
```

## Territorial Levels

| Level | Description |
|-------|-------------|
| `brasil` | National total |
| `uf` | By state (default) |
| `municipio` | By municipality |
