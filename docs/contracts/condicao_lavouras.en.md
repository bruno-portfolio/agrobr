# Contract: condicao_lavouras

Paraná crop conditions — SEAB/DERAL.

## Schema

| Column | Type | Nullable | Unit | Constraints |
|--------|------|----------|------|-------------|
| `produto` | STRING | No | — | normalized DERAL key |
| `data` | STRING | No | — | dd/mm/yyyy |
| `condicao` | STRING | No | — | boa, media, ruim, plantio, colheita |
| `pct` | FLOAT | Yes | % | 0-100 |
| `plantio_pct` | FLOAT | Yes | % | 0-100 |
| `colheita_pct` | FLOAT | Yes | % | 0-100 |

**PK:** `(produto, data, condicao)`

## Products

8 crops: cafe, cevada, feijao_1, feijao_2, milho_1, milho_2, soja, trigo.

Oats, sugarcane, canola, cassava and aggregate corn/bean totals have parser aliases,
but do not appear in the February and September 2026 editions of the weekly report. Individual crop availability varies by edition.

## Geographic scope

Data covers exclusively the state of Paraná (PR).

## Normalization

Each record carries the condition (`boa`, `media` or `ruim`) and, in the same row, the crop's
planting and harvest progress. No 2.0.0 code path produces `plantio` or `colheita` in `condicao`:
the only source is the DERAL PC.xls, which publishes only good, average and poor. Contract 1.0
still allows them, so that an external frame validated against it is not rejected.

The source normalizes each sheet's published reference date to `dd/mm/yyyy`;
sheet names such as `Atual` and `Anterior` are never used as dates.
Without a recognizable published reference, parsing raises `ParseError`
identifying the sheet. The PC.xls footnote defines `"-"` as absolute zero;
it becomes `0.0` in `pct`, `plantio_pct` and `colheita_pct`. Empty cells remain null.

The table is sorted by `produto`, by date in chronological order and by `condicao`: the last row
of each product is the most recent reference. `data` stays `dd/mm/yyyy` text, as in the contract,
so `max()` or a text sort of that column is not chronological; convert with
`pd.to_datetime(df["data"], format="%d/%m/%Y")`.

## Example

```python
from agrobr import datasets

# All crops
df = await datasets.condicao_lavouras()

# Soybeans only
df = await datasets.condicao_lavouras("soja")

# With metadata
df, meta = await datasets.condicao_lavouras(return_meta=True)
```

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
containing several crops. A missing header raises `ParseError` in the source
and `SourceUnavailableError` with the reason in the dataset, preventing partial
success containing only historical sheets. The contract remains at version 1.0.
