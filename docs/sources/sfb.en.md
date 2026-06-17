# SFB — Servico Florestal Brasileiro

## Overview

| Item | Detail |
|------|---------|
| Provider | SFB (Servico Florestal Brasileiro) |
| Data | Public forests (CNFP), forest concessions, National Forest Inventory (IFN) |
| Access | ArcGIS REST API |
| Format | JSON (tabular) / GeoJSON (geo) |
| Authentication | None |
| License | Public data |

## Layers

| Layer | Features | Geometry | Filters |
|-------|----------|-----------|---------|
| `cnfp` | ~20.8K polygons | Polygon | uf, bioma, categoria, bbox |
| `concessoes` | ~8 polygons | Polygon | uf, bbox |
| `ifn_conglomerados` | ~14.5K points | Point | uf, bioma, bbox |

## Access via ArcGIS REST

| Parameter | Value |
|-----------|-------|
| Base URL | `https://mapas.florestal.gov.br/server/rest/services` |
| CNFP Service | `Hosted/CNFP_v19_03_retificado_17072025/FeatureServer/9` |
| Concessoes Service | `Hosted/unidades_concessoes_florestais/FeatureServer/0` |
| IFN Service | `DadosAbertos-IFN/Conglomerado/FeatureServer/0` |
| Pagination | Automatic (2K features/page) |
| Throttle | 2s delay after 5 pages |

## Usage Example

```python
import asyncio
from agrobr import sfb

async def main():
    # CNFP — Cadastro Nacional de Florestas Publicas
    df = await sfb.cnfp(uf="AM")
    df = await sfb.cnfp(bioma="Amazonia", categoria="B")

    # CNFP with geometry
    gdf = await sfb.cnfp_geo(uf="PA")

    # Forest concessions
    df = await sfb.concessoes()
    gdf = await sfb.concessoes_geo()

    # IFN — National Forest Inventory (conglomerates)
    df = await sfb.ifn_conglomerados(uf="MG")
    df = await sfb.ifn_conglomerados(bioma="Cerrado")

    # IFN with geometry
    gdf = await sfb.ifn_conglomerados_geo(uf="SP")

    # Filter by bbox
    df = await sfb.cnfp(bbox=(-60, -10, -55, -5))

    # With metadata
    df, meta = await sfb.cnfp(uf="AM", return_meta=True)

    # Polars
    df = await sfb.cnfp(as_polars=True)

asyncio.run(main())
```

## Columns by Layer

### cnfp

| Column | Type | Description |
|--------|------|-------------|
| fid | int | Record ID |
| nome | str | Public forest name |
| uf | str | State (abbreviation) |
| bioma | str | Biome |
| categoria | str | Forest category |
| tipo | str | Type |
| governo | str | Government level |
| classe | str | Class |
| area_ha | float | Area in hectares |
| ano_criacao | int | Creation year |
| municipio | str | Municipality |

### concessoes

| Column | Type | Description |
|--------|------|-------------|
| fid | int | Record ID |
| nome | str | Unit name |
| uf | str | State (abbreviation) |
| bioma | str | Biome |
| area_ha | float | Area in hectares |
| ano_criacao | int | Creation year |
| grupo | str | Group |
| categoria | str | Category |

### ifn_conglomerados

| Column | Type | Description |
|--------|------|-------------|
| id | int | Record ID |
| codigo_lote | int | Lot code |
| lote | str | Lot |
| conglomerado | str | Conglomerate |
| uf | str | State (abbreviation) |
| municipio | str | Municipality |
| bioma | str | Biome |

## Specifics

- **CNFP service name**: includes the rectification date in the path (`CNFP_v19_03_retificado_17072025`)
- **Automatic pagination**: 2K features per page with connection reuse
- **Composite filters**: CNFP and IFN accept a bioma filter in addition to uf and bbox

## Limitations

- Data reflects the current state of the SFB ArcGIS Server
- Forest concessions have few records (~8 polygons)
- 2s throttle after 5 pages to avoid overloading the server
