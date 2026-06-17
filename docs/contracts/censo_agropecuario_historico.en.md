# censo_agropecuario_historico v1.0

Agricultural Census historical series (1920-2006) by theme and state via SIDRA.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | IBGE Agri Census Historical | Historical series via SIDRA (9 tables, up to state) |

## Themes

`estabelecimentos_area`, `uso_terra`, `pessoal_tratores`, `condicao_produtor`, `efetivo_animais`, `producao_animal`, `producao_vegetal`, `lavoura_permanente`, `lavoura_temporaria`

### Temporal coverage by theme

| Theme | Available censuses | Total |
|------|--------------------|:-----:|
| `estabelecimentos_area` | 1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006 | 10 |
| `uso_terra` | 1970, 1975, 1980, 1985, 1995, 2006 | 6 |
| `pessoal_tratores` | 1970, 1975, 1980, 1985, 1995, 2006 | 6 |
| `condicao_produtor` | 1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006 | 10 |
| `efetivo_animais` | 1970, 1975, 1980, 1985, 1995, 2006 | 6 |
| `producao_animal` | 1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006 | 10 |
| `producao_vegetal` | 1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006 | 10 |
| `lavoura_permanente` | 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006 | 9 |
| `lavoura_temporaria` | 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006 | 9 |

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ano` | int | ❌ | Census year (1920-2006) |
| `localidade` | str | ✅ | State or region |
| `localidade_cod` | int | ✅ | IBGE code |
| `tema` | str | ❌ | Historical-series theme |
| `categoria` | str | ❌ | Category within the theme (or "total") |
| `variavel` | str | ❌ | Variable name |
| `valor` | float64 | ✅ | Variable value |
| `unidade` | str | ❌ | Unit of measure |
| `fonte` | str | ❌ | Always "ibge_censo_agro_historico" |

## Primary Key

`[ano, tema, categoria, variavel, localidade]`

## Format

Long format: each row holds one variable/value pair.

### Variables by theme

| Theme | Variable | Unit |
|------|----------|------|
| `estabelecimentos_area` | `estabelecimentos`, `area`, `estabelecimentos_pct`, `area_pct` | Units, Hectares, % |
| `uso_terra` | `area`, `area_pct` | Hectares, % |
| `pessoal_tratores` | `pessoal_ocupado`, `tratores` | Persons, Units |
| `condicao_produtor` | `estabelecimentos`, `area`, `estabelecimentos_pct`, `area_pct` | Units, Hectares, % |
| `efetivo_animais` | `efetivo` | Head / Thousand head (poultry) |
| `producao_animal` | `producao` | Thousand liters / Thousand dozens / Tons |
| `producao_vegetal` | `producao`, `area_colhida` | varies by crop, Hectares |
| `lavoura_permanente` | `quantidade_produzida` | varies by crop |
| `lavoura_temporaria` | `quantidade_produzida` | varies by crop |

## Territorial Levels

| Level | Description |
|-------|-------------|
| `brasil` | National total |
| `regiao` | By region (Norte, Nordeste, etc.) |
| `uf` | By state (default) |

**Municipal NOT available** — municipal data does not exist in SIDRA for the historical series.

## Quirks

- **Poultry**: unit "Mil cabeças" (table 281), other animals in "Cabeças"
- **Mixed units**: animal/plant production and crops have units that vary by category (liters, dozens, tons, fruits, bunches, etc.)
- **Classifications without Total**: tables 281/282/283/1730/1731 have no "Total" category
- **Missing values**: `".."` = unavailable, `"..."` = suppressed, `"-"` = not applicable → all converted to NaN

## Guarantees

- Valid years are census years only (1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006)
- Numeric values are always >= 0
- `fonte` is always "ibge_censo_agro_historico"
- Maximum territorial level is state (no municipal data)
- Cache with 30-day TTL (static data)

## Example

```python
from agrobr import ibge

# Establishments and area, Brazil, 1985
df = await ibge.censo_agro_historico('estabelecimentos_area', ano=1985, nivel='brasil')

# Animal inventory, all states, all censuses
df = await ibge.censo_agro_historico('efetivo_animais')

# Persons and tractors in São Paulo, 1980 and 1985
df = await ibge.censo_agro_historico('pessoal_tratores', ano=[1980, 1985], uf='SP')

# Via the semantic dataset
from agrobr import datasets
df = await datasets.censo_agropecuario_historico('producao_vegetal')

# With metadata
df, meta = await ibge.censo_agro_historico('uso_terra', ano=1985, return_meta=True)
```

## JSON Schema

Available at `agrobr/schemas/censo_agropecuario_historico.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("censo_agropecuario_historico")
print(contract.to_json())
```

## Relationship with other contracts

| Contract | Scope | Periods |
|----------|-------|---------|
| `censo_agropecuario` | 10 thematic themes (SIDRA) | 1995, 2006, 2017 |
| `censo_agropecuario_legado` | 6 legacy themes (FTP) | 1995 |
| **`censo_agropecuario_historico`** | **9 historical-series themes (SIDRA)** | **1920-2006** |

These are separate contracts, no conflict. Each has its own dataset wrapper and registry entry.
