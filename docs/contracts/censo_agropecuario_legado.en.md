# censo_agropecuario_legado v1.0

Agricultural Census 1995/96 data — 6 legacy themes obtained via FTP (XLS).

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | IBGE FTP | Agricultural Census 1995/96 via FTP (legacy XLS) |

## Themes

`tecnologia`, `pessoal_ocupado`, `maquinas`, `producao_animal`, `valor_producao`, `financeiro`

### Categories by theme

| Theme | Categories |
|------|-----------|
| `tecnologia` | assistencia_tecnica, irrigacao, adubos_corretivos, controle_pragas, conservacao_solo, energia_eletrica |
| `pessoal_ocupado` | total, familiar, permanentes, temporarios, parceiros_outra |
| `maquinas` | total_tratores, menos_10cv, 10_50cv, 50_100cv, mais_100cv |
| `producao_animal` | leite_vaca, leite_cabra, la, ovos_galinha |
| `valor_producao` | total, vegetal, vegetal_subtipo, animal, animal_subtipo |
| `financeiro` | investimentos, financiamentos, despesas, receitas |

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ano` | int | ❌ | Always 1995 |
| `localidade` | str | ✅ | Locality (mesoregion, microregion, municipality) |
| `localidade_cod` | int | ✅ | IBGE code (unavailable in legacy XLS) |
| `tema` | str | ❌ | Census theme |
| `categoria` | str | ❌ | Category within the theme |
| `variavel` | str | ❌ | Variable name |
| `valor` | float64 | ✅ | Variable value |
| `unidade` | str | ❌ | Unit of measure |
| `fonte` | str | ❌ | Always 'ibge_censo_agro_legado' |

## Primary Key

`[ano, tema, categoria, variavel, localidade]`

## Guarantees

- `ano` is always 1995 (Census 1995/96)
- Numeric values are always >= 0
- `fonte` is always 'ibge_censo_agro_legado'
- Static data (update_frequency = never)

## Example

```python
from agrobr import ibge

# Technology by mesoregion
df = await ibge.censo_agro_legado('tecnologia')

# Employed persons in São Paulo
df = await ibge.censo_agro_legado('pessoal_ocupado', uf='SP')

# Machinery — municipality level
df = await ibge.censo_agro_legado('maquinas', nivel='municipio')

# With metadata
df, meta = await ibge.censo_agro_legado('tecnologia', return_meta=True)
```

## Territorial Levels

| Level | Description |
|-------|-------------|
| `brasil` | National total (Totais in XLS) |
| `uf` | Mesoregions (default) |
| `municipio` | Municipalities |
