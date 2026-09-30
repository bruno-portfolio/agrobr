# estimativa_safra v3.1

Crop estimates with an explicit CONAB survey or LSPA reference month. The dataset has its own contract, `ESTIMATIVA_SAFRA_V3_1`; the CONAB source and `CONAB_SAFRA_V2` remain at 2.0.

## Sources and selectors

| Selection | Source used | Reference |
|---|---|---|
| No source or reference selector | CONAB → IBGE LSPA | Latest available observation from the selected source |
| `fonte="conab"` | CONAB only | Most recent publication carrying the crop year (for a past crop year, the next crop year's, revised), or explicit `levantamento` |
| `levantamento=1` | CONAB only | Survey 1 of the requested crop year |
| `fonte="ibge_lspa"` | LSPA only | Latest observed month, or explicit `mes` |
| `mes="01"` | LSPA only | January in the crop year's final calendar year |

`levantamento` and `mes` are distinct selectors. Using both, or combining either with an incompatible `fonte`, raises an error before network access. A forced source is not replaced by another source. An unavailable explicit reference raises an error rather than silently selecting another month, survey or source.

Without an explicit month, LSPA selects the latest available period with observations. It does not fill a requested month's missing components from earlier months.

With `uf=None`, CONAB returns state rows and LSPA returns the Brazil aggregate (`uf` null). Specify a state for comparisons between sources. The fallback is not a guarantee of identical geographic coverage or methodology: CONAB and IBGE estimate different levels (for 2025/26 soybeans, CONAB was 3.1% above LSPA), and a series mixing both sources shifts level in the year of the switch.

CONAB tries HTTP first. Playwright and Chromium are optional transport fallback dependencies:

```bash
pip install agrobr[browser]
python -m playwright install chromium
```

The discovered catalog determines survey availability; this interface does not guarantee a complete archive of every crop year and survey.

## Parameters

`produto`, `safra` and `uf` accept positional arguments; flags and remaining selectors are keyword-only:

```python
async def estimativa_safra(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    *,
    return_meta: bool = False,
    fonte: Literal["conab", "ibge_lspa"] | None = None,
    levantamento: int | None = None,
    mes: int | str | None = None,
    as_polars: bool = False,
) -> DataFrameResult: ...
```

Products: `soja`, `milho`, `arroz`, `feijao`, `trigo`, `algodao`. `levantamento` must be an integer from 1 to 12. `mes` accepts an integer or integer string from 1 to 12, including `"01"`; it does not accept a period such as `"202501"` or a list of months.

`as_polars=True` returns a Polars DataFrame after contract validation and requires `pip install agrobr[polars]`.

`DataFrameResult` includes pandas or Polars DataFrames and a tuple with `MetaInfo` when `return_meta=True`. In pandas, `data_publicacao` uses `datetime64[ns]`; years, months and survey numbers use `Int64`, measurements use `float64`, and text follows the installed pandas version's default dtype. Empty frames retain these types.

Crop years are normalized to consecutive `YYYY/YY` years. For LSPA, `safra="2024/25"` selects calendar year **2025**. The two-year crop label is a dataset compatibility convention, not a native LSPA field or a query for the following year's forecast.

## Schema

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| `fonte` | str | No | `conab` or `ibge_lspa` |
| `produto` | str | No | Product name |
| `safra` | str | No | Normalized crop year, e.g. `2024/25` |
| `uf` | str | Yes | State; null for the LSPA Brazil aggregate |
| `area_plantada` | float64 | Yes | Planted area, thousand ha |
| `area_colhida` | float64 | Yes | LSPA harvested area, thousand ha; null for CONAB |
| `produtividade` | float64 | Yes | Yield, kg/ha |
| `producao` | float64 | Yes | Production, thousand tonnes |
| `levantamento` | Int64 | Yes | CONAB survey number (1–12) of the bulletin that published the number; null for LSPA |
| `data_publicacao` | date | Yes | Date of the CONAB bulletin that published the number, when available; null for LSPA |
| `ano_lspa` | Int64 | Yes | Observed LSPA calendar year; null for CONAB |
| `mes_lspa` | Int64 | Yes | Observed LSPA month (1–12); null for CONAB |
| `unidade_producao` | str | No | Unit of `producao`: `mil_ton` from both sources |
| `unidade_area` | str | No | Unit of `area_plantada` and `area_colhida`: `mil_ha` from both sources |

**Primary key:** `[fonte, safra, produto, uf, levantamento, ano_lspa, mes_lspa]`.

`levantamento` and `data_publicacao` belong to the CONAB bulletin that published the number, not to the crop year: without `levantamento`, a past crop year comes from the most recent publication carrying it, revised. Crop year 2024/25 served by the 12th survey of 2025/26 comes with `levantamento=12` and `data_publicacao=2026-09-15`; the 12th survey of 2024/25 is another bulletin, with another number. The bulletin crop year is in `meta.source_details["publicacao"]["safra"]`.

All columns are present, including nullable columns. The scale is the same on both routes: production in thousand tonnes and areas in thousand hectares, declared in `unidade_producao` (`mil_ton`) and `unidade_area` (`mil_ha`), the same values as `conab.brasil_total`. [`producao_anual`](./producao_anual.md) comes out in tonnes and hectares: to compare, multiply the estimate by 1,000. The key distinguishes origins and LSPA months that previously collided. Preserve the complete key when storing or deduplicating observations.

CONAB publishes one area measure, labelled "ÁREA (Em mil ha)", retained as `area_plantada`. `area_colhida` is null on the CONAB route; a separate harvested area is available only on the LSPA route. Neither area is copied or imputed from the other. CONAB contract V2 already permits this null value.

For wheat surveys, the published year is the contract crop year's final year (`Safra 2026` → `2025/26`). A survey that does not yet publish that year supplies no estimate for the requested crop year. The historical series retains its own annual period.

## LSPA normalization

The dataset uses planted area, harvested area and production from the selected period. It combines the expected crop components (two maize crops or three bean crops), checks variables, units and geography, and rejects missing components or duplicates.

Hectares and tonnes are converted to thousand ha and thousand tonnes. A missing value propagates to the corresponding aggregate; it is not replaced with zero. Yield is recalculated as total production divided by total harvested area in kg/ha. Zero harvested area produces a null yield. Published per-component yields are not summed or averaged without weights.

The [LSPA source contract](./lspa.md) remains 2.0 and returns the long table with its original year, month, variable and unit.

## Examples

```python
from agrobr import datasets

# Keep automatic source selection
current = await datasets.estimativa_safra("soja", uf="MT")

# Select CONAB survey editions
first = await datasets.estimativa_safra(
    "soja", safra="2024/25", uf="MT", levantamento=1
)
eleventh = await datasets.estimativa_safra(
    "soja", safra="2024/25", uf="MT", levantamento=11
)

# Select two LSPA months from the same calendar year
january, meta = await datasets.estimativa_safra(
    "soja", safra="2024/25", uf="MT",
    fonte="ibge_lspa", mes="01", return_meta=True,
)
december = await datasets.estimativa_safra(
    "soja", safra="2024/25", uf="MT", mes=12
)
print(january[["ano_lspa", "mes_lspa", "producao"]])
print(meta.selected_source)
```

The official 2025 LSPA captures contain soybean production in Mato Grosso of **45,586.022 thousand tonnes in January** and **50,175.032 thousand tonnes in December**. These are successive estimates of the same calendar-year crop, not production flows to add across months. [January SIDRA](https://apisidra.ibge.gov.br/values/t/6588/n3/51/h/n/p/202501/c48/39443), [December SIDRA](https://apisidra.ibge.gov.br/values/t/6588/n3/51/h/n/p/202512/c48/39443).

## Migration and JSON schema

Version 3.0 retains the previous ten columns and adds two nullable LSPA fields, but changes the primary key: the change is **major**, not merely an additive minor version. Update stored keys and schema checks. Version 3.1 adds `unidade_producao` and `unidade_area`, optional in the contract (minor). Do not use `CONAB_SAFRA_V2` to validate this dataset; it remains the source contract.

```python
from agrobr.contracts import get_contract

contract = get_contract("estimativa_safra")
print(contract.version)  # "3.1"
print(contract.primary_key)
```

JSON: `agrobr/schemas/estimativa_safra.json`. See the [migration guide](../guides/migracao-2.md) and [SemVer policy](./semver.md).
