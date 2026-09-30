# producao_anual v2.2

Consolidated annual agricultural output by state or municipality.

In API 2.0, only `produto`, `ano` accept positional arguments; all other filters and flags are passed by keyword. Empty results preserve contract dtypes: integers use `Int64`, measures use `float64`, and text follows the installed pandas default.

If IBGE is unavailable, year lists do not trigger an invalid CONAB request: that fallback reports `SourceUnavailableError` because it does not cover multiple years.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | IBGE PAM | Municipal Agricultural Production |
| 2 | CONAB | Crop Monitoring |

For the CONAB fallback, the calendar year is the second year of the crop season:
`ano=2023` queries crop season `2022/23`. CONAB provides state-level data; the
`brasil` level is computed by summing states and `municipio` has no fallback.
Because the bulletin does not publish harvested area, `area_colhida` is null in this fallback.

The fallback uses the grain bulletin and covers `soja`, `milho`, `arroz`,
`feijao`, `trigo`, and `algodao`. `cafe`, `cacau`, `cana`, `mandioca`, and
`laranja` depend on PAM in this dataset; CONAB's historical sugarcane series
is a separate API.

Without `ano`, PAM returns its latest published year. The CONAB fallback returns
the calendar year before the current one, that is, crop season `(Y-2)/(Y-1)`,
already harvested for both summer and winter crops; never the estimate of the
season in progress. Between January and the PAM release (September/October), the
two sources may differ by one year. A crop season two or more behind the most recent
bulletin edition comes from CONAB's historical series, which carries the latest
revision (see `conab.safras`).

`rendimento` changes denominator with the source. In PAM, it is production ÷
harvested area. In CONAB, it is the yield published by state, computed over the
only area the bulletin publishes; at the `brasil` level, it is production ÷ planted
area, both summed over the states.

CONAB and IBGE estimate different levels. For soybeans, CONAB was 3.8% above PAM in 2025 (171.5 against 165.3 million
t) and 3.1% above LSPA in 2025/26; areas differ by less than 1%. A series mixing both sources (the year PAM fails, or
a year still without PAM) shifts level in the year of the switch, and the jump is not a change in the crop: check
`fonte` before comparing years.

## Products

`soja`, `milho`, `arroz`, `feijao`, `trigo`, `algodao`, `cafe`, `cacau`, `cana`, `mandioca`, `laranja`

Use the canonical keys listed above. Sugarcane and cassava are long-duration
temporary crops: `area_plantada` is the area intended for harvest during the
calendar year. For oranges, a permanent crop, it also means area intended
for harvest. The series starts in 1974; `area_plantada` is missing before 1988.
The dataset requests area, production, and yield; `valor_producao` stays null
because it is not requested from PAM. The `ibge.pam` API can request that
variable explicitly.

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ano` | Int64 | ❌ | Reference year |
| `produto` | str | ❌ | Product name |
| `localidade` | str | ✅ | State or municipality |
| `localidade_cod` | Int64 | ❌ | Locality IBGE code (SIDRA D1C); optional column, IBGE rows only |
| `cod_municipio` | Int64 | ✅ | IBGE municipality code (7 digits), the common key of the municipal datasets; null outside municipality rows (and in the CONAB fallback, which is by state) |
| `area_plantada` | float64 | ✅ | Planted area (ha) |
| `area_colhida` | float64 | ✅ | Harvested area (ha) |
| `producao` | float64 | ✅ | Production (see `unidade_producao`) |
| `rendimento` | float64 | ✅ | Yield (see `unidade_rendimento`) |
| `valor_producao` | float64 | ✅ | Output value (see `unidade_valor_producao`) |
| `fonte` | str | ❌ | Data origin: `ibge_pam` or `conab` |
| `unidade_producao` | str | ✅ | Production unit: `ton`; oranges before 2001 use `mil_frutos` (thousand fruits) |
| `unidade_rendimento` | str | ✅ | Yield unit: `kg/ha`; oranges before 2001 use `frutos/ha` (fruits/ha) |
| `unidade_valor_producao` | str | ✅ | Currency and scale: `mil_reais` (thousand reais) or historical currencies according to the year |
| `condicao_produto` | str | ✅ | Coffee is `em_coco` through 2001 and `beneficiado` from 2002; missing for other products |

## Primary Key

`[ano, produto, localidade]`

## Guarantees

- Consolidated data for the complete crop year
- Typical latency: Y+1 (data available the following year)
- CONAB area and production are converted from thousand ha/thousand tons to ha/tons

## Example

```python
from agrobr import datasets

# Output by state
df = await datasets.producao_anual("soja", ano=2023)

# Output by municipality
df = await datasets.producao_anual("milho", ano=2023, nivel="municipio")

# Filter by state
df = await datasets.producao_anual("soja", ano=2023, uf="MT")

# With metadata
df, meta = await datasets.producao_anual("soja", ano=2023, return_meta=True)
```

```python
df = await datasets.producao_anual("cana", ano=2024, uf="SP")
df = await datasets.producao_anual("mandioca", ano=2024, nivel="municipio", uf="RO")
df, meta = await datasets.producao_anual("laranja", ano=2000, nivel="brasil", return_meta=True)
```

## JSON Schema

Available at `agrobr/schemas/producao_anual.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("producao_anual")
print(contract.to_json())
```

## Territorial Levels

| Level | Description |
|-------|-------------|
| `brasil` | National total |
| `uf` | By state (default) |
| `municipio` | By municipality; available only from the primary IBGE PAM source |

## PAM units and historical breaks

Published values are not implicitly converted. `unidade_producao`, `unidade_rendimento`, and `unidade_valor_producao` identify each row's scale. Before 2001, oranges use `mil_frutos` and `frutos/ha`; from 2001 onward, `ton` and `kg/ha`. `condicao_produto` distinguishes coffee `em_coco` through 2001 from `beneficiado` since 2002. Historical currencies remain identified without conversion to BRL or inflation adjustment. See the [IBGE methodology notes](https://sidra.ibge.gov.br/pesquisa/pam/tabelas/).

SIDRA's `-` symbol means numeric zero and remains zero; `..`, `...`, and `X` remain missing. Municipalities with zero production are retained. The `producao_anual` contract is 2.1; the four descriptive columns and `localidade_cod` are optional in the contract and supplied by the PAM API.

PAM parser 2 also preserves localities and measures whose values are entirely missing or suppressed. Two observations for the same locality, year and measure, including colliding variable aliases, raise `ParseError`; unmapped variables are also rejected. The reader does not silently select the first value. The schema is 2.1 (2.0 plus `localidade_cod`).

In the CONAB fallback, Brazil totals are the sum of states and yield is
recalculated as production in tonnes × 1,000 / area in hectares. CONAB's
published national yield is rounded and may differ from this ratio.
Reconciliation compares summed area and production with the published `BRASIL`
row; regions and other aggregates are excluded from the sum.
