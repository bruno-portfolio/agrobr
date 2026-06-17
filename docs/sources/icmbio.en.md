# ICMBio — Federal Conservation Units

## Overview

| Item | Detail |
|------|---------|
| Provider | ICMBio (Instituto Chico Mendes de Conservacao da Biodiversidade) |
| Data | Federal conservation unit boundaries |
| Access | OGC WFS (INDE GeoServer) |
| Format | CSV (tabular) / GeoJSON (geo) |
| Authentication | None |
| License | Brazilian federal public data |
| Features | 344 federal conservation units |

## Access via WFS

| Parameter | Value |
|-----------|-------|
| Endpoint | `geoservicos.inde.gov.br/geoserver/ICMBio/ows` |
| WFS Version | 1.1.0 |
| Layer | `ICMBio:limiteucsfederais_a` |
| CRS | EPSG:4674 |

## Usage Example

```python
import asyncio
from agrobr import icmbio

async def main():
    # All federal conservation units
    df = await icmbio.ucs()

    # Filter by group (PI = strict protection, US = sustainable use)
    df = await icmbio.ucs(grupo="PI")

    # Filter by state (uses LIKE, works with multi-state units)
    df = await icmbio.ucs(uf="MT")

    # With geometry (requires geopandas)
    gdf = await icmbio.ucs_geo(bbox=(-56, -16, -54, -14))

    # With metadata
    df, meta = await icmbio.ucs(return_meta=True)

asyncio.run(main())
```

## Columns

| Column | Type | Description |
|--------|------|-----------|
| codigo | str | CNUC (unique code) |
| nome | str | Conservation unit name |
| categoria | str | Category abbreviation (PARNA, ESEC, FLONA, etc) |
| grupo | str | PI (strict protection) or US (sustainable use) |
| uf | str | State(s) covered (separated by ;) |
| bioma | str | IBGE biome |
| area_ha | float | Area in hectares |
| ano_criacao | Int64 | Creation year |
| ato_criacao | str | Legal act of creation |

## Limitations

- Only federal conservation units (344). State and municipal ones are not in this WFS.
- The `uf` field may contain multiple states (e.g., "MT;PA")
- Data reflects the current state of the INDE/ICMBio GeoServer
