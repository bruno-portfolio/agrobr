# DERAL — Crop Conditions PR

Departamento de Economia Rural da Secretaria de Agricultura do Paraná (SEAB/PR).
Weekly data on crop conditions, planting progress and harvest.

## API

```python
from agrobr import deral

# Condition of all crops
df = await deral.condicao_lavouras()

# Filter by product
df = await deral.condicao_lavouras("soja")
df = await deral.condicao_lavouras("milho")
df = await deral.condicao_lavouras("trigo")
```

## Columns — `condicao_lavouras`

| Column | Type | Description |
|---|---|---|
| `produto` | str | Monitored crop; beans and corn preserve the season as `feijao_1`, `feijao_2`, `milho_1`, or `milho_2` |
| `data` | datetime64[ns] | Reference date published in the sheet |
| `condicao` | str | `boa`, `media` or `ruim`; planting and harvest progress come in the columns below, in the same record |
| `pct` | float | Percentage of the crop in that condition |
| `plantio_pct` | float | Planting progress (%) |
| `colheita_pct` | float | Harvest progress (%) |

## Published dates and percentages

`data` comes from the sheet's reference-date cell, including cells outside the
header, and is returned as `datetime64[ns]`. Excel dates, `dd/mm/yyyy`,
`dd-mm-yyyy` and `dd-mm-yy` are recognized; two-digit years use 2000+.
A sheet name never replaces a missing date: the parser raises `ParseError`
identifying the sheet.
An unreadable sheet stops parsing with `ParseError`; no partial result is returned.

The table is sorted by `produto`, by date in chronological order and by `condicao`: the last row
of each product is the most recent reference. `data` is `datetime64[ns]` (up to 1.1.0, `dd/mm/yyyy`
text); to filter one date, compare with `pd.Timestamp("2026-02-01")`: pandas reads the text
`"01/02/2026"` month first and matches January 2, with no warning.

The PC.xls footnote defines `"-"` as absolute zero. In percentage columns,
this dash, including surrounding whitespace, becomes `0.0`; empty cells remain null.
`source_method` reports the reader actually used, such as `httpx+xlrd` for
the September 2026 BIFF file, including any fallback reader.

## Products

8 crops published in the February and September 2026 editions:
cafe, cevada, feijao_1, feijao_2, milho_1 (summer crop), milho_2 (second crop), soja, trigo.

Oats, sugarcane, canola and cassava have parser aliases, but do not appear in these weekly
report editions. The source and the dataset accept the eight crops above, the `milho` and
`feijao` filters, which select both published seasons, and agrobr synonyms (`"Soja"`,
`"soybean"`, `"milho 2ª safra"`). Any other name raises `InvalidParameterError` with the list,
before any request. The dataset advertises the eight crops above plus the `feijao` and `milho` filters (10 values),
whose availability varies by edition.

## Risk Note

DERAL publishes data in Excel spreadsheets (PC.xls). The layout may change
without notice between crop seasons. The parser reads the condition sheets
(one row per crop, with the ruim, média, boa, plantada and colhida columns) and
skips the others. Drastic format changes may require a parser update.

## MetaInfo

```python
df, meta = await deral.condicao_lavouras("soja", return_meta=True)
print(meta.source)  # "deral"
print(meta.source_method)  # "httpx+xlrd"
```

## Source

- URL: `https://www.agricultura.pr.gov.br/system/files/publico/Safras/PC.xls`
- Format: Excel (.xls)
- Update: weekly
- Coverage: Paraná

## Reading the PC.xls workbook

The PC.xls published in February and September 2026 is BIFF/XLS: 26 sheets, 438
condition records and 730 numeric condition, planting and harvest cells. The
`.xlsx` extension of an older file does not describe its actual format.

Percentages in these editions are percentage points (0–100), with no conversion
from percent-formatted fractions. Phenological stage and commercialization
columns, potato rows and second-season soybean rows are outside the current
contract. A sheet reporting a holiday without observations produces no records
or zeros. The sheet named `18-12-2017` publishes 08/01/2018 as its reference;
the date comes from the cell, as published. When a dated sheet name (`dd-mm-yy` or
`dd-mm-yyyy`) differs from the cell date, as here and in `19-09-2021`, with 20/09/2021,
parsing follows the cell and warns in `validation_warnings` and `UserWarning`.

Parser 2 requires the Ruim, Média, Boa, Plantada and Colhida headers in tables
containing several crops. A missing header raises `ParseError` in the source,
and the dataset also raises `ParseError` ("Todas as fontes falharam por layout", with the
source reason in `errors`), preventing partial success containing only historical sheets.
The contract is version 2.0: `data` changed from `dd/mm/yyyy` text to `datetime64[ns]`.
