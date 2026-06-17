# FUNAI — Indigenous Lands

## Overview

| Item | Detail |
|------|---------|
| Provider | FUNAI (Fundacao Nacional dos Povos Indigenas) |
| Data | Indigenous Lands as polygons |
| Access | OGC WFS (GeoServer) |
| Format | CSV (tabular) / GeoJSON (geo) |
| Authentication | None |
| License | CC BY-ND 3.0 |
| Features | ~740 Indigenous lands |

## Access via WFS

| Parameter | Value |
|-----------|-------|
| Endpoint | `geoserver.funai.gov.br/geoserver/Funai/ows` |
| WFS Version | 2.0.0 |
| Layer | `Funai:tis_poligonais` |
| CRS | EPSG:4674 |

## Usage Example

```python
import asyncio
from agrobr import funai

async def main():
    # All Indigenous lands
    df = await funai.terras_indigenas()

    # Filter by state
    df = await funai.terras_indigenas(uf="MT")

    # Filter by phase
    df = await funai.terras_indigenas(fase="Regularizada")

    # With geometry (requires geopandas)
    gdf = await funai.terras_indigenas_geo(bbox=(-56, -16, -54, -14))

    # With metadata
    df, meta = await funai.terras_indigenas(return_meta=True)

asyncio.run(main())
```

## Columns

| Column | Type | Description |
|--------|------|-----------|
| codigo | int | Indigenous land code |
| nome | str | Indigenous land name |
| etnia | str | Predominant ethnicity |
| municipio | str | Seat municipality |
| uf | str | State (abbreviation) |
| area_ha | float | Area in hectares |
| fase | str | Process phase |
| modalidade | str | Indigenous land type |
| data_atualizacao | datetime | Update date |

## Phases

Regularizada, Homologada, Declarada, Delimitada, Em Estudo, Encaminhada RI.

## Limitations

- Only polygonal Indigenous lands (points and lines excluded)
- Data reflects the current state of the FUNAI GeoServer
- CC BY-ND 3.0: free use with attribution, no derivatives
