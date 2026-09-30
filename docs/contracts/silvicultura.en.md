# silvicultura v1.1

Silvicultural output (eucalyptus, pine, charcoal, timber) by state or municipality.

In API 2.0, only `produto`, `ano` accept positional arguments; all other filters and flags are passed by keyword. Empty results preserve contract dtypes: integers use `Int64`, measures use `float64`, and text follows the installed pandas default.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | IBGE PEVS | Plant Extraction and Silviculture Production |

## Products

`carvao`, `lenha`, `madeira_tora`, `madeira_celulose`, `madeira_outras_finalidades`, `acacia_negra`, `eucalipto_folha`, `resina`

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ano` | Int64 | ❌ | Reference year |
| `localidade` | str | ✅ | State or municipality |
| `localidade_cod` | Int64 | ✅ | IBGE code |
| `cod_municipio` | Int64 | ✅ | IBGE municipality code (7 digits), the common key of the municipal datasets; null outside municipality rows |
| `produto` | str | ❌ | Product name |
| `valor` | float64 | ✅ | Quantity produced (tons or cubic meters) or, with `variavel="valor_producao"`, production value in thousand reais; the scale is in `unidade` |
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
