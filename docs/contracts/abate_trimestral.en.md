# abate_trimestral v2.1

Animal slaughter by species, quarter and state (cattle, hogs, poultry).

In API 2.0, only `produto`, `trimestre` accept positional arguments; all other filters and flags are passed by keyword. Empty results preserve contract dtypes: integers use `Int64`, measures use `float64`, and text follows the installed pandas default.

Version 2.0 changes `animais_abatidos` from `float64` to `Int64`. Empty results also use `Int64`; fractional values raise `ParseError` without truncation. Contract 1.0 remains available as `IBGE_ABATE_V1`.

Version 2.1 adds `categoria` (cattle herd type) to the output and to the primary key. The default, `categoria="total"`, returns the same rows as 2.0, with `categoria` equal to `total`; `categoria="todas"` returns 6 rows per state and quarter (total, bois, vacas, novilhos, novilhas and vitelos). For swine and poultry, `categoria` is always `total`.

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
| `localidade_cod` | Int64 | ✅ | IBGE code |
| `especie` | str | ❌ | bovino, suino or frango |
| `categoria` | str | ❌ | Cattle herd type: total, bois (steers), vacas (cows), novilhos (young steers), novilhas (heifers) or vitelos (calves); `total` for swine and poultry |
| `animais_abatidos` | Int64 | ✅ | Number of animals slaughtered (head) |
| `peso_carcacas` | float64 | ✅ | Total carcass weight (kg) |
| `fonte` | str | ❌ | Data origin |

The dataset returns the states and has no Brazil row. IBGE suppresses (cell `X`) the states with few respondents for confidentiality, and they come out null (cattle 2025: AP, DF and PB). As a result, the sum of the states falls short of the published Brazil total: for cattle in the first two quarters of 2025, by 0.4 %. For the national total, use SIDRA table 1092 at the Brazil level.

## Primary Key

`[trimestre, especie, localidade, categoria]`

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

# Share of females (cows + heifers) in cattle slaughter, by state
df = await datasets.abate_trimestral("bovino", trimestre="202303", categoria="todas")
cabecas = df.pivot(index="localidade", columns="categoria", values="animais_abatidos")
femeas = (cabecas["vacas"] + cabecas["novilhas"]) / cabecas["total"]
```

## JSON Schema

Available at `agrobr/schemas/abate_trimestral.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("abate_trimestral")
print(contract.to_json())
```
