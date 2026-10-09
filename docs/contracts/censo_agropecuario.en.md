# censo_agropecuario v1.2

Agricultural Census 1995/2006/2017 data by theme, state and territorial level.

In API 2.0, only `tema` accepts positional arguments; all other filters and flags are passed by keyword. Empty results preserve contract dtypes: integers use `Int64`, measures use `float64`, and text follows the installed pandas default.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | IBGE Agri Census | Agricultural Census 1995, 2006 and 2017 |

## Themes

`efetivo_rebanho`, `uso_terra`, `lavoura_temporaria`, `lavoura_permanente`, `preparo_solo`, `adubacao`, `calagem`, `agrotoxicos`, `praticas_agricolas`, `irrigacao`, `despesa_adubos`

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
| `despesa_adubos` | — | — | ✅ |

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ano` | Int64 | ❌ | Reference year (1995, 2006 or 2017) |
| `localidade` | str | ✅ | State or municipality |
| `localidade_cod` | Int64 | ✅ | IBGE code |
| `cod_municipio` | Int64 | ✅ | IBGE municipality code (7 digits), the common key of the municipal datasets; null outside municipality rows |
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
| `efetivo_rebanho` | `estabelecimentos` (2017 only) | units |
| `efetivo_rebanho` | `cabecas` | head |
| `uso_terra` | `estabelecimentos` | units |
| `uso_terra` | `area` | hectares |
| `lavoura_temporaria` | `estabelecimentos` (2017) or `informantes` (1995) | units |
| `lavoura_temporaria` | `producao` | varies |
| `lavoura_temporaria` | `area_colhida` | hectares |
| `lavoura_permanente` | `estabelecimentos` (2017) or `informantes` (1995) | units |
| `lavoura_permanente` | `producao` | varies |
| `lavoura_permanente` | `area_colhida` | hectares |

In 1995, the crop themes publish `informantes`: SIDRA variable 151 (tables 492 and 504), which SIDRA labels
"Número de informantes" (number of informants). In 2017, `estabelecimentos` is the "Número de estabelecimentos
agropecuários com lavoura temporária" (10084) and, for permanent crops, "com 50 pés e mais existentes" (9504).

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
- No cache: every call queries IBGE
- The `ano` parameter filters by census year; `ano=None` returns all available years
- `categoria = "Total"` is the row the source publishes as the total of the theme's classification (e.g. all
  irrigation methods, all livestock species). It does not add up with the other categories.
- `estabelecimentos` does not add up across categories: one establishment can fall in more than one. Irrigation,
  Brasília 2017: 2,726 establishments in the Total and 3,224 adding up the 11 methods. Additive measures, such as area,
  match the Total (25,626 ha in both) when no category is confidential. A confidential category comes out null ("X" in
  the source), and the sum falls below the Total: in the AL livestock herd, 747 head are missing because Buffalo and
  Ostriches come out "X".
- The Census counts only agricultural establishments and does not match PAM and PPM, even with the same names: in 2017 it
  is about 10% below PAM for soybean and corn (up to 20% in PR) and about 20% below PPM for the cattle herd. The reference date also differs: the 2017 Census herd is that of 2017-09-30, and PPM's is that of December 31 of each year.
- The year 1995 is also in `censo_agropecuario_historico`, with different numbers, because the SIDRA tables are different
  ([details](./censo_agropecuario_historico.en.md#relationship-with-other-contracts)).

## Example

```python
from agrobr import ibge

# Herd inventory by state (1995 and 2017)
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

Through the dataset, with the semantic layer's `MetaInfo`:

```python
from agrobr import datasets

df, meta = await datasets.censo_agropecuario("efetivo_rebanho", uf="MT", return_meta=True)
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
