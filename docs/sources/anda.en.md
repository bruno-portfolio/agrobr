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
| `produto_fertilizante` | str | Always `total`; the source does not publish deliveries broken down by formulation |
| `volume_ton` | float | Delivered volume (tonnes) |

## Risk Note

ANDA publishes data in PDF. The layout may change without notice between years.
The available delivery bulletins contain only aggregated fertilizer totals.
Therefore, `produto="total"` is the only accepted value; formulations such as
`ureia`, `map`, or `kcl` raise `InvalidParameterError` before download.

The agrobr parser automatically detects the orientation of the tables
(states in rows vs columns), and also supports the "Principais
Indicadores" layout (aggregated national data with months/values in cells
concatenated with `\n`). Drastic format changes may require a parser update.

There is no year fallback. If no PDF link matches the requested year, the client
raises `InvalidParameterError` and reports the years available on the site.
For the selected link, the actual year is extracted from the link text or file
name and passed to the parser.

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
