# censo_agropecuario v1.0

Agricultural Census 1995/2006/2017 data by theme, state and territorial level.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | IBGE Agri Census | Agricultural Census 1995, 2006 and 2017 |

## Themes

`efetivo_rebanho`, `uso_terra`, `lavoura_temporaria`, `lavoura_permanente`, `preparo_solo`, `adubacao`, `calagem`, `agrotoxicos`, `praticas_agricolas`, `irrigacao`

### Temporal coverage by theme

| Theme | 1995 | 2006 | 2017 |
|------|:----:|:----:|:----:|
| `efetivo_rebanho` | ✅ | — | ✅ |
| `uso_terra` | ✅ | — | ✅ |
| `lavoura_temporaria` | ✅ | — | ✅ |
| `lavoura_permanente` | ✅ | — | ✅ |
| `preparo_solo` | — | ✅ | ✅ |
| `adubacao` | — | ✅ | ✅ |
| `calagem` | — | ✅ | ✅ |
| `agrotoxicos` | — | ✅ | ✅ |
| `praticas_agricolas` | — | ✅ | ✅ |
| `irrigacao` | — | ✅ | ✅ |

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ano` | int | ❌ | Reference year (1995, 2006 or 2017) |
| `localidade` | str | ✅ | State or municipality |
| `localidade_cod` | int | ✅ | IBGE code |
| `tema` | str | ❌ | Census theme |
| `categoria` | str | ❌ | Category within the theme |
| `variavel` | str | ❌ | Variable name |
| `valor` | float64 | ✅ | Variable value |
| `unidade` | str | ❌ | Unit of measure |
| `fonte` | str | ❌ | Data origin |

## Primary Key

`[ano, tema, categoria, variavel, localidade]`

## Format

Long format: each row holds one variable/value pair.

### Variables by theme (original themes)

| Theme | Variable | Unit |
|------|----------|------|
| `efetivo_rebanho` | `estabelecimentos` | units |
| `efetivo_rebanho` | `cabecas` | head |
| `uso_terra` | `estabelecimentos` | units |
| `uso_terra` | `area` | hectares |
| `lavoura_temporaria` | `estabelecimentos` | units |
| `lavoura_temporaria` | `producao` | varies |
| `lavoura_temporaria` | `area_colhida` | hectares |
| `lavoura_permanente` | `estabelecimentos` | units |
| `lavoura_permanente` | `producao` | varies |
| `lavoura_permanente` | `area_colhida` | hectares |

### New themes — categories

| Theme | Categories (examples) |
|------|----------------------|
| `preparo_solo` | Cultivo convencional, Cultivo minimo, Plantio direto na palha |
| `adubacao` | Quimica, Organica, Adubacao verde (2006); Fez adubacao, Quimica, Organica (2017) |
| `calagem` | Fez aplicacao, Nao fez aplicacao |
| `agrotoxicos` | Utilizou, Nao utilizou |
| `praticas_agricolas` | Plantio em nivel, Rotacao de culturas, Pousio |
| `irrigacao` | Gotejamento, Pivo central, Inundacao, Aspersao |

## Guarantees

- Consolidated decennial data (Agricultural Census 1995, 2006 and 2017)
- 2017 reference period: October 2016 to September 2017
- Cache with 30-day TTL (stable data)
- The `ano` parameter filters by census year; `ano=None` returns all available years

## Example

```python
from agrobr import ibge

# Herd inventory by state (2017)
df = await ibge.censo_agro('efetivo_rebanho')

# Land use in Mato Grosso
df = await ibge.censo_agro('uso_terra', uf='MT')

# Soil preparation — both years
df = await ibge.censo_agro('preparo_solo')

# Irrigation, 2017 only
df = await ibge.censo_agro('irrigacao', ano=2017)

# Temporary crops by municipality
df = await ibge.censo_agro('lavoura_temporaria', nivel='municipio', uf='PR')

# With metadata
df, meta = await ibge.censo_agro('efetivo_rebanho', return_meta=True)
```

## JSON Schema

Available at `agrobr/schemas/censo_agropecuario.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("censo_agropecuario")
print(contract.to_json())
```

## Territorial Levels

| Level | Description |
|-------|-------------|
| `brasil` | National total |
| `uf` | By state (default) |
| `municipio` | By municipality |

## Legacy Themes (FTP)

6 additional themes from the 1995/96 Census are available via `censo_agro_legado()` with a separate contract. See [censo_agropecuario_legado](./censo_agropecuario_legado.md).
