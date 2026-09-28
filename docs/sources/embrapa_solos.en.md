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
| Format | WFS 2.0 GeoJSON (`application/json`) in both modes |
| Authentication | None |
| License | CC BY-NC 3.0 BR |
| Features | 34,464 horizon/layer records (~9K points) + 2,852 polygons |

## Access via WFS

| Parameter | Value |
|-----------|-------|
| Endpoint | `geoinfo.dados.embrapa.br/geoserver/ows` |
| WFS Version | 2.0.0 |
| Profiles layer | `geonode:perfis_pronasolos_2020` |
| Map layer | `geonode:brasil_solos_5m_20201104` |
| CRS | EPSG:4326 (declared by the WFS and the ISO metadata) |
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
| fid | int | Record identifier (horizon or layer) |
| uf | str | State (abbreviation) |
| municipio | str | Municipality |
| latitude | float | Latitude |
| longitude | float | Longitude |
| horizonte | str | Horizon symbol |
| profundidade | str | Depth |
| areia_total | str | Total sand (g/kg) |
| silte | str | Silt (g/kg) |
| argila | str | Clay (g/kg) |
| ph_h2o | str | pH in water |
| carbono_organico | str | Organic carbon (unit not declared by the source) |
| ctc | str | Cation exchange capacity (T) |
| saturacao_bases | str | Base saturation (V, %) |
| aluminio | str | Exchangeable aluminum |
| fosforo | str | Available phosphorus |
| classe_textural | str | Textural class |
| nivel_levantamento | str | Survey level |
| uso_atual | str | Current land use |

The table shows the main columns. The result has 85 columns, in the order of the `embrapa_solos.perfis` 2.0
contract: the 83 published attributes (under the names above or the layer's original name), `uf_original`
and `feature_id`. Each row is a horizon or layer; `codigo_pon` identifies the sampling point.

Laboratory values are the text published by Embrapa (the WFS declares `xsd:string`), without conversion:
numbers with a decimal point, sometimes with float32 noise (`4.400000095367432`), and the text `NULL` for
missing values. `fosforo` also carries censored values (`<1`, `<0.5`), decimal commas (`0,19`) and marks
such as `x`. Convert explicitly, e.g. `pd.to_numeric(df["argila"].replace("NULL", pd.NA))`; for `fosforo`,
handle the censored values and the comma first.

The WFS declares no units. Across the whole layer, sand + silt + clay add up to 1,000 (g/kg) in 99.2% of
the horizons with all three values; `saturacao_bases` = 100 x S / T and `ctc` = S + H + Al (columns
`valor_s`, `hidrogenio` and `aluminio`) in more than 98% of the horizons.

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
| ordem3 | str | Soil order 3 |
| subordem3 | str | Suborder 3 |
| gdegrupo3 | str | Great group 3 |
| feature_id | str | WFS feature identifier (text, not a key) |

## Specifics

- **`_geo()` functions require [geo]**: `pip install agrobr[geo]` (geopandas)
- **Pagination**: count/startIndex ordered by `fid`, with a 1-record overlap between pages. `max_registros` (default 50,000; 5,000 profiles and 3,000 polygons in the `_geo` functions) cuts the remote prefix; the `uf` and `ordem` filters are applied locally to that prefix and, when the cut leaves the selection partial, a `UserWarning` is raised (`max_registros=None` scans the whole layer)
- **CRS**: EPSG:4326, the default CRS of both layers in the WFS; `bbox` is also EPSG:4326
- **NC license**: commercial use requires authorization from EMBRAPA
- **Double-encoded text**: Embrapa publishes part of the profile text double-encoded (UTF-8 read as Latin-1: "AptidÃ£o", "SÃ£o Carlos"), in both the WFS JSON and CSV. agrobr repairs only text that round-trips through Latin-1 → UTF-8 and carries the signature ("Ã" or "Â" followed by a character between U+0080 and U+00BF). Legitimate text with "Ã" stays as is. The per-column count goes to `MetaInfo.validation_warnings`. Three cases the source publishes this way stay unrepaired and are counted separately in the same warning ("com a assinatura e sem reparo"): text cut at about 254 characters in the middle of a UTF-8 sequence, text with a published "�", and text with "€" (33 cells in DF, checked on 2026-09-26)

## Limitations

- Profile coverage is not uniform (PronaSolos still in progress)
- Soil map at 1:5,000,000 scale (national view, not cadastral)
- CC BY-NC 3.0 BR: commercial redistribution requires authorization
