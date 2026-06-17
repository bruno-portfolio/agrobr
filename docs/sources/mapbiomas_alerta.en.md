# MapBiomas Alerta — Deforestation Alerts

## Overview

| Item | Detail |
|------|---------|
| Provider | MapBiomas |
| Data | Deforestation alerts with geometry |
| Access | GraphQL API |
| Format | JSON (GraphQL) |
| Authentication | Token (env AGROBR_MAPBIOMAS_ALERTA_TOKEN) |
| License | Free (attribution required) |
| Features | ~500K+ alerts |

## Access via GraphQL

| Parameter | Value |
|-----------|-------|
| Endpoint | `https://plataforma.alerta.mapbiomas.org/api/v2/graphql` |
| Auth | Bearer token |

## Usage Example

```python
import asyncio
from agrobr import mapbiomas_alerta

async def main():
    # Tabular alerts (requires token)
    df = await mapbiomas_alerta.alertas(
        token="seu-token",
        start_date="2024-01-01",
        end_date="2024-06-30",
    )

    # Filter by detection source
    df = await mapbiomas_alerta.alertas(
        sources=["DETER", "SAD"],
        start_date="2024-01-01",
    )

    # Filter by bounding box
    df = await mapbiomas_alerta.alertas(
        bbox=(-56, -16, -54, -14),
    )

    # With WKT geometry (requires geopandas)
    gdf = await mapbiomas_alerta.alertas_geo(
        start_date="2024-01-01",
        end_date="2024-03-31",
    )

    # With metadata
    df, meta = await mapbiomas_alerta.alertas(return_meta=True)

    # Polars
    df = await mapbiomas_alerta.alertas(as_polars=True)

    # Info (date range + last publication)
    info = await mapbiomas_alerta.alerta_info()

asyncio.run(main())
```

## Columns

| Column | Type | Description |
|--------|------|-----------|
| alert_code | str | Alert code |
| area_ha | float | Area in hectares |
| data_deteccao | datetime | Detection date |
| data_publicacao | datetime | Publication date |
| status | str | Alert status |
| fonte | str | Detection source (DETER, SAD, GLAD, SAD Caatinga) |
| lat | float | Latitude |
| lon | float | Longitude |
| geometry | Polygon | WKT geometry (alertas_geo only) |

## Limitations

- Requires an authentication token (env `AGROBR_MAPBIOMAS_ALERTA_TOKEN` or parameter `token=`)
- Pagination limited to 50 pages per query
- Throttle after 5 pages (3s delay)
