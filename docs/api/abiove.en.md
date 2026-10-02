# ABIOVE API

The ABIOVE module provides export data for the soybean complex — beans, meal, oil and corn — published by the Brazilian Association of Vegetable Oil Industries.

!!! warning "zona_cinza license"
    Terms of use not located publicly. Formal authorization requested in Feb/2026 — awaiting reply.

## Functions

### `exportacao`

Export volumes and revenue for the soybean complex.

```python
async def exportacao(
    ano: int,
    *,
    mes: int | None = None,
    produto: str | None = None,
    agregacao: str = "detalhado",
    edicao: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `ano` | `int` | Reference year, from 2010 to the current one |
| `mes` | `int \| None` | Data month (1-12). None returns every published month |
| `produto` | `str \| None` | Filter: `"grao"`, `"farelo"`, `"oleo"`, `"milho"`. In the monthly sum, rows keep the filtered product. For the all-product total use `agregacao="mensal"` without `produto` or with `produto="total"` (returned as `produto="total"`); `produto="total"` with `agregacao="detalhado"` raises `InvalidParameterError` before the network call |
| `agregacao` | `str` | `"detalhado"` (by product/month) or `"mensal"` (sum) |
| `edicao` | `str \| None` | Workbook edition, `"YYYY-MM"` (e.g. `"2025-12"`), of `ano` or `ano + 1`. None reads the latest edition that publishes `ano` |
| `as_polars` | `bool` | Return as polars DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `ano`, `mes`, `produto`, `volume_ton`, `receita_usd_mil`

**Example:**

```python
from agrobr import abiove

# Full 2024 exports
df = await abiove.exportacao(2024)

# Meal only
df = await abiove.exportacao(2024, produto="farelo")

# Specific month
df = await abiove.exportacao(2024, mes=6)

# Original December 2025 number, from the December edition
df = await abiove.exportacao(2025, mes=12, edicao="2025-12")
```

**Edition:**

ABIOVE publishes one workbook per monthly edition (`exp_YYYYMM.xlsx`), with the edition year and the previous one, and revises months already published. Without `edicao`, agrobr reads the latest edition that carries `ano`: first the following year's editions (which carry `ano` as the comparison year), then the year's own, from newest to oldest, never past the current month. In September 2026, `exportacao(2025)` reads `exp_202608.xlsx`, which revises 19 of the 96 cells of 2025 published in `exp_202512.xlsx` (meal, Dec/2025: 1,990,304.323 t, not 2,020,365.023 t).

- The edition read goes to `MetaInfo.source_details["edicao"]` (`arquivo` and `mes`), and the workbook SHA-256 to `raw_content_hash`.
- `mes` only filters the data month: a month not yet published returns an empty DataFrame, and an `ano` that is not an integer from 2010 to the current year or a `mes` outside 1-12 raise `InvalidParameterError` before the network, as do `edicao` in any other format or from another year, a `produto` outside the list and an `agregacao` other than `"detalhado"` and `"mensal"`.
- A failure on the latest edition (timeout, HTTP 5xx) raises `SourceUnavailableError`; agrobr only moves to the previous edition when the latest one does not exist (HTTP 404).

## Synchronous Version

```python
from agrobr.sync import abiove

df = abiove.exportacao(2024)
```

## Notes

- Source: [ABIOVE](https://abiove.org.br) — `zona_cinza` license
- Data in Excel with a multi-section format
