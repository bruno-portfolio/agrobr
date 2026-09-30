# pecuaria_municipal v1.1

Herd inventory and animal-origin output by state or municipality.

In API 2.0, only `produto`, `ano` accept positional arguments; all other filters and flags are passed by keyword. Empty results preserve contract dtypes: integers use `Int64`, measures use `float64`, and text follows the installed pandas default.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | IBGE PPM | Municipal Livestock Survey |

## Products

### Herds

`bovino`, `bubalino`, `equino`, `suino_total`, `suino_matrizes`, `caprino`, `ovino`, `galinaceos_total`, `galinhas`, `codornas`

PPM's `bovino` is not USDA's cattle herd (`usda.psd`, code `0011000`): the PPM is the herd surveyed by IBGE per municipality for the reference year, and USDA's `Beginning Stocks` is its estimate for the start of the year. The 2024 PPM shows 238.2 million head, and the 2025 `Beginning Stocks`, 186.9 million (21.5% lower). The 2 series are not the same measure.

`galinhas` is the IBGE category "Galináceos - galinhas", which includes laying and breeder hens. `galinhas_poedeiras` is still accepted as a deprecated alias (`FutureWarning`) and returns `especie="galinhas"`.

### Animal-origin output

`leite`, `ovos_galinha`, `ovos_codorna`, `mel`, `casulos`, `la`

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ano` | Int64 | ❌ | Reference year |
| `localidade` | str | ✅ | State or municipality |
| `localidade_cod` | Int64 | ✅ | IBGE code |
| `cod_municipio` | Int64 | ✅ | IBGE municipality code (7 digits), the common key of the municipal datasets; null outside municipality rows |
| `especie` | str | ❌ | Species/product name |
| `valor` | float64 | ✅ | Value (unit varies by species) |
| `unidade` | str | ❌ | Unit of measure |
| `fonte` | str | ❌ | Data origin |

## Primary Key

`[ano, especie, localidade]`

## Guarantees

- Consolidated calendar-year data (reference Dec 31)
- Typical latency: Y+1 (data available the following year)
- Historical series since 1974

## Example

```python
from agrobr import datasets

# Cattle herd by state
df = await datasets.pecuaria_municipal("bovino", ano=2023)

# Milk production by municipality
df = await datasets.pecuaria_municipal("leite", ano=2023, nivel="municipio", uf="MG")

# Filter by state
df = await datasets.pecuaria_municipal("bovino", ano=2023, uf="MT")

# With metadata
df, meta = await datasets.pecuaria_municipal("bovino", ano=2023, return_meta=True)
```

## JSON Schema

Available at `agrobr/schemas/pecuaria_municipal.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("pecuaria_municipal")
print(contract.to_json())
```

## Territorial Levels

| Level | Description |
|-------|-------------|
| `brasil` | National total |
| `uf` | By state (default) |
| `municipio` | By municipality |
