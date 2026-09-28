# extrativismo_vegetal v1.1

Extractive plant production (açaí, Brazil nut, yerba mate, palm heart, etc.) by state or municipality.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | IBGE PEVS | Plant Extraction and Silviculture Production |

## Products

`acai`, `castanha_para`, `erva_mate`, `palmito`, `pequi_fruto`, `babacu`, `piacava`, `carnauba_cera`, `carvao`, `lenha`, `madeira_tora`, `hevea_coagulado`

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ano` | int | ❌ | Reference year |
| `localidade` | str | ✅ | State or municipality |
| `localidade_cod` | int | ✅ | IBGE code |
| `cod_municipio` | int | ✅ | IBGE municipality code (7 digits), the common key of the municipal datasets; null outside municipality rows |
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

# Açaí output by state
df = await datasets.extrativismo_vegetal("acai", ano=2023)

# Brazil nut in Amazonas
df = await datasets.extrativismo_vegetal("castanha_para", ano=2023, uf="AM")

# Filter by state
df = await datasets.extrativismo_vegetal("erva_mate", ano=2023, uf="PR")

# With metadata
df, meta = await datasets.extrativismo_vegetal("acai", ano=2023, return_meta=True)
```

## JSON Schema

Available at `agrobr/schemas/extrativismo_vegetal.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("extrativismo_vegetal")
print(contract.to_json())
```

## Territorial Levels

| Level | Description |
|-------|-------------|
| `brasil` | National total |
| `uf` | By state (default) |
| `municipio` | By municipality |
