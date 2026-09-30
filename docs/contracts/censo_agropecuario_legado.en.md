# censo_agropecuario_legado v2.1

Agricultural Census 1995/96 data — six themes published in ZIP archives containing XLS or HTML tables.

In API 2.0, only `tema` accepts positional arguments; all other filters and flags are passed by keyword. Empty results preserve contract dtypes: integers use `Int64`, measures use `float64`, and text follows the installed pandas default.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | IBGE FTP | Agricultural Census 1995/96, national and state tables |

## Themes

`tecnologia`, `pessoal_ocupado`, `maquinas`, `producao_animal`, `valor_producao`, `financeiro`

National categories are the activity labels published in table rows, such as `Total` and `Arroz`. State and municipal tables use category `Total`. `variavel` preserves the official header hierarchy, separated by ` / `; replace the old names inferred from column positions.

Table numbers differ between Brazil and state directories. For `financeiro`, Brazil combines table 11 (expenses) with table 12 (revenue); state table 11 includes investments, financing, expenses, and revenue.

The `maquinas` theme has no Pará: IBGE published Table 6 (personnel) in place of Table 7 in `Para/Tab_7Mn.zip`. The query without `uf` returns 26 states, with the warning in `validation_warnings`; `uf='PA'` raises `SourceUnavailableError`.

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ano` | Int64 | ❌ | Always 1995 |
| `localidade` | str | ✅ | Brazil, state name, or historical municipality name |
| `localidade_cod` | Int64 | ✅ | Brazil/state code; null for municipalities without a code in the table |
| `cod_municipio` | Int64 | ✅ | IBGE municipality code (7 digits), the common key of the municipal datasets; null outside municipality rows (and for municipalities without a code in the table) |
| `uf` | str | ✅ | State abbreviation from the official directory/header; null for Brazil |
| `tema` | str | ❌ | Census theme |
| `categoria` | str | ❌ | Category within the theme |
| `variavel` | str | ❌ | Variable name |
| `valor` | float64 | ✅ | Variable value |
| `unidade` | str | ❌ | Unit of measure |
| `fonte` | str | ❌ | Always 'ibge_censo_agro_legado' |

## Primary Key

`[ano, tema, categoria, variavel, localidade, uf]`

The state distinguishes municipalities with identical names without changing those names or assigning current codes to historical localities.

## Guarantees

- `ano` is always 1995 (Census 1995/96)
- Numeric values are always >= 0
- `fonte` is always 'ibge_censo_agro_legado'
- Static data (update_frequency = never)

`valor` uses the unit specified in its row. The parser interprets the header and the Excel cell's numeric scale: for example, monetary values stored in reais and displayed in thousand reais are divided by 1,000. Respondent counts remain in units. Available decimal precision is preserved.

In Census 1995/96 HTML tables, `-` is interpreted as zero according to the official legend, preserving the distinction between absence of the phenomenon and an uninterpretable value.

## Example

```python
from agrobr import ibge

# National expenses and revenue by activity
df = await ibge.censo_agro_legado('financeiro', nivel='brasil')

# Employed persons in São Paulo
df = await ibge.censo_agro_legado('pessoal_ocupado', uf='SP')

# Machinery in Goiás municipalities
df = await ibge.censo_agro_legado('maquinas', uf='GO', nivel='municipio')

# With metadata
df, meta = await ibge.censo_agro_legado('tecnologia', return_meta=True)
```

Through the dataset, with the semantic layer's `MetaInfo`:

```python
from agrobr import datasets

df, meta = await datasets.censo_agropecuario_legado("pessoal_ocupado", uf="SP", return_meta=True)
```

## Territorial Levels

| Level | Description |
|-------|-------------|
| `brasil` | National tables, including activity categories; incompatible with the `uf` filter |
| `uf` | Actual state totals (default); without `uf`, queries all 27 state directories |
| `municipio` | Municipalities in the requested state; without a filter, queries all states |

Contract 2.0 corrects geography, categories, and variables, and adds `uf` to the key. Mesoregions and microregions are not returned as states.
