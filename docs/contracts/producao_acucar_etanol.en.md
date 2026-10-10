# producao_acucar_etanol v1.0

Sugar production, cane and corn ethanol production and average ATR by crop season and state, from CONAB's industrial sugarcane historical series.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | CONAB | Historical Series — sugarcane, industry (`canaseriehist-industria.xls`) |

## Interpretation

Each sheet of the workbook becomes one column, in the published units and without conversion. There is one row per crop season and state: all 27 states appear in every closed season, even when the workbook only has dashes for the state. Regions, `NORTE/NORDESTE`, `CENTRO-SUL` and `BRASIL` are not published.

**Corn ethanol.** The "Etanol Total (cana e milho)" sheet adds cane and corn anhydrous and hydrated ethanol, and `etanol_total_mil_l` is that published total: it is not cane ethanol. In MT, 2024/25 season, the total is 6,577,571.7 thousand liters, of which 5,418,000.0 thousand liters (82%) come from corn; cane ethanol (cane anhydrous + hydrated) is 1,159,571.7 thousand liters, and reading the total as cane multiplies it by 5.7. Adding all ethanol columns counts ethanol twice. Corn appears in the workbook from 2018/19 (GO, MT and PR in that season); before that the corn columns are null, not zero.

**Zero vs. empty.** A published zero becomes `0.0`. A dash (`-`), an empty cell and an Excel error (`#N/A`) become null, never zero. In the 10/10/2026 workbook, RR has zeros in the sugar and cane/total ethanol sheets in most seasons; in 2021/22 those sheets and ATR contain dashes. Both corn sheets are empty for RR in all closed seasons. DF 2019/20 has `#N/A` in total ethanol and in cane hydrated ethanol. ATR `0` only appears for a state and season without cane sugar or ethanol: it is not a measurement, so drop those zeros before averaging. ATR is a per-tonne average; do not add ATR across states.

**Warnings, numbers unchanged.** agrobr passes on the published numbers and warns (`warnings.warn` and `MetaInfo.validation_warnings`) when:

- total ethanol differs from the sum of the four parts (a null part counts as zero). In the 10/10/2026 workbook: CE 2009/10, RO 2018/19, RO 2021/22 and SC 2021/22;
- the sum of the states differs from the published BRASIL in a volume column. In the 10/10/2026 workbook: sugar 2005/06, cane anhydrous ethanol 2009/10 and 2025/26 and total ethanol 2021/22;
- the workbook publishes an Excel error in a state cell;
- the query includes the most recent closed season of the workbook (2025/26 on 10/10/2026), which CONAB may revise in the four-monthly sugarcane surveys.

**Estimate.** The last column of the workbook, marked `(¹)` and explained in the footer ("Estimativa em agosto de 2026"), is left out. The inclusive `ano_inicio`/`ano_fim` filters use the first year of the season and require integers; `uf` takes the state code.

**Layout.** A missing, unknown or repeated sheet, a title or unit different from the measured ones, a missing or repeated state, text instead of a number, a marked column before the last season, an estimate announced in the footer without a marked column and sheets with different seasons raise `ParseError`. None of these becomes an empty column.

**Deterministic mode.** Does not apply: the workbook is the current one. Inside `datasets.deterministic(...)` the query runs and warns.

License: CONAB, `livre`.

## Schema

| Column | Type | Nullable | Unit | Stable |
|--------|------|----------|------|--------|
| `safra` | str | ❌ | - | Yes |
| `regiao` | str | ❌ | - | Yes |
| `uf` | str | ❌ | - | Yes |
| `acucar_mil_ton` | float | ✅ | thousand t | Yes |
| `etanol_anidro_cana_mil_l` | float | ✅ | thousand liters | Yes |
| `etanol_hidratado_cana_mil_l` | float | ✅ | thousand liters | Yes |
| `etanol_anidro_milho_mil_l` | float | ✅ | thousand liters | Yes |
| `etanol_hidratado_milho_mil_l` | float | ✅ | thousand liters | Yes |
| `etanol_total_mil_l` | float | ✅ | thousand liters | Yes |
| `atr_kg_t` | float | ✅ | kg/t of cane | Yes |

**Primary key:** `[safra, uf]`

**Constraints:** every measure `>= 0`

## Guarantees

- Unique PK per safra + uf
- State rows only, all 27 in every closed season; regions, `NORTE/NORDESTE`, `CENTRO-SUL` and `BRASIL` are left out
- Closed seasons since 2005/06; the estimate column (marked with a note) is left out
- Source units, no conversion: sugar in thousand t, ethanol in thousand liters, ATR in kg/t of cane
- A published zero becomes `0.0`; dash, empty cell and Excel error become null, never zero
- `etanol_total_mil_l` is the published value and includes corn ethanol; a total different from the sum of the 4 parts (empty counts as 0) raises a warning in `validation_warnings` and a `UserWarning`, without changing the number
- A sum of the states different from the published BRASIL in the volume columns raises a warning, without changing the number
- Measures `>= 0` when present
- A sheet, title, unit, states or season columns different from the measured layout raise `ParseError`

## Example

Measures use `float64`, also when empty; text uses the pandas default dtype. `as_polars` and `return_meta` are keyword-only. `conab.cana_industria()` returns the same table straight from the source.

```python
from agrobr import datasets

# Async
df = await datasets.producao_acucar_etanol(ano_inicio=2020, ano_fim=2025, uf="MT")
etanol_cana = df["etanol_anidro_cana_mil_l"] + df["etanol_hidratado_cana_mil_l"]
etanol_milho = df[["etanol_anidro_milho_mil_l", "etanol_hidratado_milho_mil_l"]].sum(
    axis=1, min_count=1
)

# With metadata and warnings
df, meta = await datasets.producao_acucar_etanol(return_meta=True)
print(meta.validation_warnings)

# Sync
from agrobr.sync import datasets
df = datasets.producao_acucar_etanol(2024, 2025)
```

## JSON Schema

Available at `agrobr/schemas/producao_acucar_etanol.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("producao_acucar_etanol")
print(contract.to_json())
```
