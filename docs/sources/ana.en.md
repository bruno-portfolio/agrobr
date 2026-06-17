# ANA/SNIRH — Agencia Nacional de Aguas

## Overview

| Item | Detail |
|------|---------|
| Provider | ANA (Agencia Nacional de Aguas e Saneamento Basico) |
| Data | Hydrography, irrigation pivots, irrigation demand, water availability |
| Access | ArcGIS REST API |
| Format | JSON (tabular) / GeoJSON (geo) |
| Authentication | None |
| License | Public data |

## Layers

| Layer | Features | Geometry | bbox required |
|-------|----------|-----------|------------------|
| `hidrografia` | ~620K polylines | Polyline | Yes |
| `pivos_irrigacao` | ~19.9K polygons | Polygon | No |
| `demanda_irrigacao` | ~265K polygons | Polygon | Yes |
| `disponibilidade_hidrica` | ~42K polylines | Polyline | No |

## Access via ArcGIS REST

| Parameter | Value |
|-----------|-------|
| Base URL | `https://portal1.snirh.gov.br/server/rest/services/dados_abertos` |
| Pagination | Automatic (1K features/page) |
| Throttle | 2s delay after 5 pages |

## Usage Example

```python
import asyncio
from agrobr import ana

async def main():
    # Hidrografia (bbox required — large dataset)
    df = await ana.hidrografia(bbox=(-50, -20, -48, -18))

    # With geometry
    gdf = await ana.hidrografia_geo(bbox=(-50, -20, -48, -18))

    # Irrigation pivots
    df = await ana.pivos_irrigacao(uf="GO")

    # Pivots with geometry
    gdf = await ana.pivos_irrigacao_geo(uf="SP", bbox=(-50, -22, -48, -20))

    # Irrigation demand (bbox required)
    df = await ana.demanda_irrigacao(bbox=(-50, -20, -48, -18))

    # Water availability
    df = await ana.disponibilidade_hidrica(uf="MG")
    gdf = await ana.disponibilidade_hidrica_geo(bbox=(-46, -20, -44, -18))

    # With metadata
    df, meta = await ana.pivos_irrigacao(return_meta=True)

    # Polars
    df = await ana.pivos_irrigacao(as_polars=True)

    # Limit features
    df = await ana.hidrografia(bbox=(-50, -20, -48, -18), max_features=500)

asyncio.run(main())
```

## Columns by Layer

### hidrografia

| Column | Type | Description |
|--------|------|-------------|
| OBJECTID | int | Record ID |
| codigo_curso | str | Watercourse code |
| codigo_bacia | str | Basin code |
| nome_rio | str | River name |
| dominio | str | Domain |

### pivos_irrigacao

| Column | Type | Description |
|--------|------|-------------|
| OBJECTID | int | Record ID |
| codigo_municipio | str | Municipality code |
| municipio | str | Municipality |
| estado | str | State |
| regiao_hidro | str | Hydrographic region |
| area_ha | float | Area in hectares |

### demanda_irrigacao

| Column | Type | Description |
|--------|------|-------------|
| OBJECTID | int | Record ID |
| ID | int | ID |
| codigo_bacia | str | Basin code |
| versao | str | Version |
| vazao_max_mensal | float | Maximum monthly flow |
| vazao_mes_seco | float | Dry-month flow |
| vazao_mes_irrigacao | float | Irrigation-month flow |
| vazao_media_anual | float | Mean annual flow |

### disponibilidade_hidrica

| Column | Type | Description |
|--------|------|-------------|
| OBJECTID | int | Record ID |
| ID | int | ID |
| area_montante_km2 | float | Upstream area in km2 |
| disponibilidade_m3_s | float | Availability in m3/second |
| nome_rio | str | River name |
| dominio | str | Domain |
| versao | str | Version |

## Specifics

- **bbox required**: `hidrografia` and `demanda_irrigacao` require bbox (large datasets)
- **Automatic pagination**: 1K features per page with connection reuse
- **max_features**: optional parameter to limit the total number of features returned

## Limitations

- Hidrografia and irrigation demand require bbox (without a filter they would return hundreds of thousands of features)
- 2s throttle after 5 pages to avoid overloading the server
