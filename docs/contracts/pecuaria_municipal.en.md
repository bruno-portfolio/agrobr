# pecuaria_municipal v1.0

Herd inventory and animal-origin output by state or municipality.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | IBGE PPM | Municipal Livestock Survey |

## Products

### Herds

`bovino`, `bubalino`, `equino`, `suino_total`, `suino_matrizes`, `caprino`, `ovino`, `galinaceos_total`, `galinhas_poedeiras`, `codornas`

### Animal-origin output

`leite`, `ovos_galinha`, `ovos_codorna`, `mel`, `casulos`, `la`

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ano` | int | ❌ | Reference year |
| `localidade` | str | ✅ | State or municipality |
| `localidade_cod` | int | ✅ | IBGE code |
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
