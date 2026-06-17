# ABIOVE — Soybean Complex Exports

> **License:** No public terms of use located. Formal authorization
> requested in Feb/2026 — awaiting reply.
> Classification: `zona_cinza`

!!! note "Authorization pending"
    Formal authorization for redistribution of data was requested from ABIOVE
    in February/2026. Awaiting reply. Verify directly with ABIOVE before
    commercial use.

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
| `produto` | str | Product (grao, farelo, oleo, milho) |
| `volume_ton` | float | Exported volume (tonnes) |
| `receita_usd_mil` | float | FOB revenue (thousand USD) |

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
```

## Risk Note

ABIOVE publishes data in Excel spreadsheets. The layout may vary between years.
The agrobr parser automatically detects the header position.

## Source

- URL: `https://abiove.org.br/estatisticas/`
- Format: Excel (.xlsx)
- Update: monthly
- History: 2010+
- License: `zona_cinza` — authorization requested (Feb/2026)
