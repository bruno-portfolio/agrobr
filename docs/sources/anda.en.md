# ANDA — Fertilizers

> **License:** No public terms of use located. Formal authorization
> requested in Feb/2026 — awaiting reply.
> Classification: `zona_cinza`

!!! note "Authorization pending"
    Formal authorization for redistribution of data was requested from ANDA
    in February/2026. Awaiting reply. Verify directly with ANDA before
    commercial use.

Associação Nacional para Difusão de Adubos. Fertilizer delivery data by
state and month.

## Installation

ANDA requires `pdfplumber` as an optional dependency:

```bash
pip install agrobr[pdf]
```

## API

```python
from agrobr import anda

# Fertilizer deliveries by state/month
df = await anda.entregas(ano=2024)

# Filter by state
df = await anda.entregas(ano=2024, uf="MT")

# Monthly aggregation (sums all states)
df = await anda.entregas(ano=2024, agregacao="mensal")
```

## Columns — `entregas`

| Column | Type | Description |
|---|---|---|
| `ano` | int | Year |
| `mes` | int | Month (1-12) |
| `uf` | str | State |
| `produto_fertilizante` | str | Fertilizer type |
| `volume_ton` | float | Delivered volume (tonnes) |

## Risk Note

ANDA publishes data in PDF. The layout may change without notice between years.
The agrobr parser automatically detects the orientation of the tables
(states in rows vs columns), and also supports the "Principais
Indicadores" layout (aggregated national data with months/values in cells
concatenated with `\n`). Drastic format changes may require a parser update.

The client automatically detects the actual data year in the PDF
(based on the link text, not the upload URL) and passes it to the parser,
avoiding a mismatch when the requested year has no PDF of its own and the
fallback points to the most recent year available.

In agrobr-insights, ANDA data is handled with dynamic weighting: when it
looks distorted, its weight in the SCI is automatically reduced.

## MetaInfo

```python
df, meta = await anda.entregas(ano=2024, return_meta=True)
print(meta.source)  # "anda"
print(meta.source_method)  # "httpx+pdfplumber"
```

## Source

- URL: `https://anda.org.br/recursos/`
- Format: PDF/Excel
- Update: monthly
- History: 2010+
- License: `zona_cinza` — authorization requested (Feb/2026)
