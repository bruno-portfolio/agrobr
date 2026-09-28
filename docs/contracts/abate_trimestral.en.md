# abate_trimestral v1.0

Animal slaughter by species, quarter and state (cattle, hogs, poultry).

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | IBGE Slaughter | Quarterly Animal Slaughter Survey |

## Species

`bovino`, `suino`, `frango`

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `trimestre` | str | ❌ | Quarter in YYYYQQ format |
| `localidade` | str | ✅ | State |
| `localidade_cod` | int | ✅ | IBGE code |
| `especie` | str | ❌ | bovino, suino or frango |
| `animais_abatidos` | float64 | ✅ | Number of animals slaughtered (head) |
| `peso_carcacas` | float64 | ✅ | Total carcass weight (kg) |
| `fonte` | str | ❌ | Data origin |

The dataset returns the states and has no Brazil row. IBGE suppresses (cell `X`) the states with few respondents for confidentiality, and they come out null (cattle 2025: AP, DF and PB). As a result, the sum of the states falls short of the published Brazil total: for cattle in the first two quarters of 2025, by 0.4 %. For the national total, use SIDRA table 1092 at the Brazil level.

## Primary Key

`[trimestre, especie, localidade]`

## Guarantees

- Consolidated quarterly data
- Typical latency: Q+2 months
- Historical series since 1997

## Example

```python
from agrobr import datasets

# Cattle slaughter by state
df = await datasets.abate_trimestral("bovino", trimestre="202303")

# Poultry slaughter in Paraná
df = await datasets.abate_trimestral("frango", trimestre="202303", uf="PR")

# With metadata
df, meta = await datasets.abate_trimestral("bovino", trimestre="202303", return_meta=True)
```

## JSON Schema

Available at `agrobr/schemas/abate_trimestral.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("abate_trimestral")
print(contract.to_json())
```
