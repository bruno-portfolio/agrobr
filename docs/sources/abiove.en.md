# ABIOVE — Soybean Complex Exports

> ABIOVE is a private association. No specific reuse license was located for Soy Complex statistics; the classification is zona_cinza. Distinguish extracted values from spreadsheet structure, text and reports. Crediting ABIOVE, file, period and transformations documents provenance but does not replace any required permission. The category records the absence of a published license.

Associação Brasileira das Indústrias de Óleos Vegetais. Monthly export data
for soybean grain, meal, oil and maize.

## API

```python
from agrobr import abiove

# Soybean complex exports
df = await abiove.exportacao(ano=2024)

# Filter by product
df = await abiove.exportacao(ano=2024, produto="grao")

# Filter by month
df = await abiove.exportacao(ano=2024, mes=6)

# Monthly aggregation (sums all products)
df = await abiove.exportacao(ano=2024, agregacao="mensal")
```

## Columns — `exportacao`

| Column | Type | Description |
|---|---|---|
| `ano` | int | Reference year |
| `mes` | int | Month (1-12) |
| `produto` | str | Product (grao, farelo, oleo, milho); in the monthly aggregation, the filtered product, or total without a filter |
| `volume_ton` | float | Exported volume (tonnes) |
| `receita_usd_mil` | float | FOB revenue (thousand USD) |

Monthly product tables use the year selected from the header. Weight published
in thousand tonnes is converted to tonnes; average price per tonne is not FOB
revenue. Comparisons between Brazil and the soybean complex are excluded from
product rows. The `datasets.exportacao` fallback converts weight to kg and
revenue to USD, as required by the dataset contract.

Each monthly edition (`exp_YYYYMM.xlsx`) carries the edition year and the previous one, and ABIOVE
revises months already published. agrobr delivers the latest number: it reads the newest edition that
publishes the requested year and records which one in `MetaInfo.source_details["edicao"]`.
`edicao="YYYY-MM"` reads a specific edition (a month's original, for instance). See the
[API](../api/abiove.md).

## Products

- `grao` — Soybean grain
- `farelo` — Soybean meal
- `oleo` — Soybean oil
- `milho` — Maize

## MetaInfo

```python
df, meta = await abiove.exportacao(ano=2024, return_meta=True)
print(meta.source)  # "abiove"
print(meta.source_method)  # "httpx+openpyxl"
print(meta.source_details["edicao"])  # {"arquivo": "exp_202608.xlsx", "mes": "2026-08"}
```

## Risk Note

ABIOVE publishes data in Excel spreadsheets. The layout may vary between years.
The agrobr parser reads the published layout (product sections in rows, with
the months in the label column) and rejects with `ParseError` what it does not
recognize, without guessing products or columns.

## Source

- URL: `https://abiove.org.br/estatisticas/`
- Format: Excel (.xlsx)
- Update: monthly
- History: 2010+
- License: `zona_cinza` — no reuse license located.
