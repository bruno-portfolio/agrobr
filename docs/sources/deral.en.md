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
| `produto` | str | Monitored crop |
| `data` | str | Reference date (dd/mm/yyyy) |
| `condicao` | str | Condition: "boa", "media", "ruim" |
| `pct` | float | Percentage of the crop in that condition |
| `plantio_pct` | float | Planting progress (%) |
| `colheita_pct` | float | Harvest progress (%) |

## Products

soja, milho, milho_1 (verão), milho_2 (safrinha), trigo, feijao,
feijao_1, feijao_2, mandioca, cana, cafe, aveia, cevada, canola.

## Risk Note

DERAL publishes data in Excel spreadsheets (PC.xls). The layout may change
without notice between crop seasons. The parser automatically detects the
products in the spreadsheet tabs and extracts conditions and progress. Drastic
format changes may require a parser update.

## MetaInfo

```python
df, meta = await deral.condicao_lavouras("soja", return_meta=True)
print(meta.source)  # "deral"
print(meta.source_method)  # "httpx+openpyxl"
```

## Source

- URL: `https://www.agricultura.pr.gov.br/system/files/publico/Safras/PC.xls`
- Format: Excel (.xls)
- Update: weekly
- Coverage: Paraná
