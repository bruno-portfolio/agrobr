# EMBRAPA Solos/GeoInfo — Soil Profiles and Soil Map

> **License:** CC BY-NC 3.0 BR.
> Classification: `nc`

PronaSolos soil profiles and the soil map of Brazil via OGC WFS
from EMBRAPA GeoInfo.

## Overview

| Item | Detail |
|------|---------|
| Provider | EMBRAPA (Empresa Brasileira de Pesquisa Agropecuaria) |
| Data | Soil profiles (points) + soil map (polygons) |
| Access | OGC WFS (GeoServer) |
| Format | CSV (tabular) / GeoJSON (geo) |
| Authentication | None |
| License | CC BY-NC 3.0 BR |
| Features | ~34K profiles + ~2.8K polygons |

## Access via WFS

| Parameter | Value |
|-----------|-------|
| Endpoint | `geoinfo.dados.embrapa.br/geoserver/ows` |
| WFS Version | 2.0.0 |
| Profiles layer | `geonode:perfis_pronasolos_2020` |
| Map layer | `geonode:brasil_solos_5m_20201104` |
| CRS | EPSG:4674 |
| Pagination | Yes (count/startIndex) |

## Usage Example

```python
import asyncio
from agrobr import embrapa_solos

async def main():
    # Soil profiles (tabular)
    df = await embrapa_solos.perfis()

    # Filter by state
    df = await embrapa_solos.perfis(uf="MT")

    # Profiles with geometry (requires geopandas)
    gdf = await embrapa_solos.perfis_geo(bbox=(-56, -16, -54, -14))

    # Soil map (tabular)
    df = await embrapa_solos.mapa_solos()

    # Soil map with geometry
    gdf = await embrapa_solos.mapa_solos_geo(bbox=(-56, -16, -54, -14))

    # With metadata
    df, meta = await embrapa_solos.perfis(return_meta=True)

    # Polars
    df = await embrapa_solos.perfis(as_polars=True)

asyncio.run(main())
```

## Columns — Profiles

| Column | Type | Description |
|--------|------|-------------|
| fid | int | Profile identifier |
| uf | str | State (abbreviation) |
| municipio | str | Municipality |
| latitude | float | Latitude |
| longitude | float | Longitude |
| horizonte | str | Horizon symbol |
| profundidade | str | Depth |
| areia_total | float | Total sand content (g/kg) |
| silte | float | Silt content (g/kg) |
| argila | float | Clay content (g/kg) |
| ph_h2o | float | pH in water |
| carbono_organico | float | Organic carbon (g/kg) |
| ctc | float | Cation exchange capacity |
| saturacao_bases | float | Base saturation (V%) |
| aluminio | float | Exchangeable aluminum |
| fosforo | float | Available phosphorus |
| classe_textural | str | Textural class |
| nivel_levantamento | str | Survey level |
| uso_atual | str | Current land use |

## Columns — Soil Map

| Column | Type | Description |
|--------|------|-------------|
| fid | int | Polygon identifier |
| simbolos | str | SiBCS symbols |
| comp1 | str | Component 1 |
| comp2 | str | Component 2 |
| comp3 | str | Component 3 |
| legenda | str | Descriptive legend |
| area_km2 | float | Area in km2 |
| ordem1 | str | Soil order 1 |
| subordem1 | str | Suborder 1 |
| gdegrupo1 | str | Great group 1 |
| ordem2 | str | Soil order 2 |
| subordem2 | str | Suborder 2 |
| gdegrupo2 | str | Great group 2 |
| legenda_sinotica | str | Synoptic legend |
| classe_dom | str | Dominant class |

## Specifics

- **`_geo()` functions require [geo]**: `pip install agrobr[geo]` (geopandas)
- **Pagination**: 34K+ profiles require automatic pagination via WFS
- **CRS**: EPSG:4674 (SIRGAS 2000)
- **NC license**: commercial use requires authorization from EMBRAPA

## Limitations

- Profile coverage is not uniform (PronaSolos still in progress)
- Soil map at 1:5,000,000 scale (national view, not cadastral)
- CC BY-NC 3.0 BR: commercial redistribution requires authorization
