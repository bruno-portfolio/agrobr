# DERAL API

The DERAL module provides crop condition, planting and harvest progress data from the Rural Economy Department of Parana.

## Functions

### `condicao_lavouras`

Weekly condition of Parana crops.

```python
async def condicao_lavouras(
    produto: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str \| None` | Filter by crop (`"soja"`, `"milho"`, `"milho_1"`, `"milho_2"`, `"trigo"`, `"feijao"`, `"cana"`, `"cafe"`, etc.). None returns all |
| `as_polars` | `bool` | Return as polars DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `produto`, `data`, `condicao`, `pct`, `plantio_pct`, `colheita_pct`

**Example:**

```python
from agrobr import deral

# All crops
df = await deral.condicao_lavouras()

# Soybean only
df = await deral.condicao_lavouras("soja")

# With metadata
df, meta = await deral.condicao_lavouras("milho", return_meta=True)
```

## Synchronous Version

```python
from agrobr.sync import deral

df = deral.condicao_lavouras("soja")
```

## Notes

- Source: [DERAL/SEAB-PR](https://www.agricultura.pr.gov.br) — `livre` license
- Parana-exclusive data
- Published in Excel (PC.xls) — layout may vary between crop years

## Reading the PC.xls workbook

The PC.xls published in February and September 2026 is BIFF/XLS: 26 sheets, 438
condition records and 730 numeric condition, planting and harvest cells. The
`.xlsx` extension of an older file does not describe its actual format.

Percentages in these editions are percentage points (0–100), with no conversion
from percent-formatted fractions. Phenological stage and commercialization
columns, potato rows and second-season soybean rows are outside the current
contract. A sheet reporting a holiday without observations produces no records
or zeros. The sheet named `18-12-2017` publishes 08/01/2018 as its reference;
the date comes from the cell, as published.

Parser 2 requires the Ruim, Média, Boa, Plantada and Colhida headers in tables
containing several crops. A missing header raises `ParseError` in the source
and `SourceUnavailableError` with the reason in the dataset, preventing partial
success containing only historical sheets. The contract remains at version 1.0.
