# ANA/SNIRH — Agencia Nacional de Aguas

## Overview

| Item | Detail |
|------|---------|
| Provider | ANA (Agencia Nacional de Aguas e Saneamento Basico) |
| Data | Hydrography, irrigation pivots, irrigation demand, water availability |
| Access | ArcGIS REST MapServer API |
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

## Data vintage and reference

`pivos_irrigacao` and `pivos_irrigacao_geo` use the **2014** mapping produced by
ANA in partnership with Embrapa Milho e Sorgo, as stated in the
[official Pivos_Mapeados metadata](https://portal1.snirh.gov.br/arcgis/rest/services/DADOSABERTOS/Pivos_Mapeados/MapServer?f=pjson).

For the demand and availability layers, `versao` preserves the official
`DSVERSAO` field. The observed value `BHO 2013 versao 1.3 de 22/07/2014`
identifies the hydrographic base version; it does not, by itself, establish the
year of the demand estimates. Retrieval dates in `MetaInfo`, such as
`fetched_at`, record when data was fetched, not when the survey took place.
These layers do not represent real-time hydrological measurements.

## Access via ArcGIS REST

| Parameter | Value |
|-----------|-------|
| Base URL | `https://portal1.snirh.gov.br/server/rest/services/dados_abertos` |
| Service | `MapServer/0` |
| Pagination | Keyset: `orderByFields=OBJECTID` and `OBJECTID > last` (up to 1K features/page) |
| Throttle | 2s pause after the sixth page and each subsequent page |
| Read timeout | 180s |

Tabular functions request JSON with `returnGeometry=false`; `_geo` variants
request GeoJSON and retain geometry in EPSG:4326. The spatial `bbox` filter
applies to both formats.

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
    df = await ana.disponibilidade_hidrica(bbox=(-46, -20, -44, -18))
    gdf = await ana.disponibilidade_hidrica_geo(bbox=(-46, -20, -44, -18))

    # With metadata
    df, meta = await ana.pivos_irrigacao(return_meta=True)

    # Polars
    df = await ana.pivos_irrigacao(as_polars=True)

    # Limit features
    df = await ana.hidrografia(bbox=(-50, -20, -48, -18), max_registros=500)

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
| versao | str | Original DSVERSAO identifier of the hydrographic base |
| vazao_max_mensal | float | Maximum monthly withdrawal flow (m3/s) |
| vazao_mes_seco | float | Dry-month withdrawal flow (m3/s) |
| vazao_mes_irrigacao | float | Irrigation-month withdrawal flow (m3/s) |
| vazao_media_anual | float | Mean annual withdrawal flow (m3/s) |

### disponibilidade_hidrica

| Column | Type | Description |
|--------|------|-------------|
| OBJECTID | int | Record ID |
| ID | int | ID |
| area_montante_km2 | float | Upstream area in km2 |
| disponibilidade_m3_s | float | Availability in m3/second |
| nome_rio | str | River name |
| dominio | str | Domain |
| versao | str | Original DSVERSAO identifier of the hydrographic base |

## Specifics

- **bbox required**: `hidrografia` and `demanda_irrigacao` require bbox (large datasets)
- **Keyset pagination**: each page asks for up to 1K features ordered by `OBJECTID` and the next one continues from the last `OBJECTID` received, until the official count is reached. The Hidrografia server returns one feature fewer than requested on each page; with offset pagination the boundary feature was lost (1,046 of 1,047 in the test extract). If pagination stops before the official count, or a page does not advance the `OBJECTID`, the query raises `SourceUnavailableError` stating how many features are missing, instead of returning a partial result. An unreadable page (truncated JSON, proxy HTML) or a page without `OBJECTID` raises `ParseError`
- **max_registros**: positive integer limiting the returned features, or `None` for no cap. Zero, negative values, booleans and non-integers raise `InvalidParameterError` before collection. The previous `max_features` argument is no longer accepted
- **Required fields**: the tabular parser checks the fields configured in `required_cols` for every feature on every page; a missing field raises `ParseError`

| Layer | Required field in the official response | Normalized column |
|-------|-----------------------------------------|-------------------|
| `hidrografia` | `COCURSODAG` | `codigo_curso` |
| `pivos_irrigacao` | `NM_ESTADO` | `estado` |
| `demanda_irrigacao` | `COBACIA` | `codigo_bacia` |
| `disponibilidade_hidrica` | `DISPQ95` | `disponibilidade_m3_s` |

An official count of zero returns a valid empty result with the published columns;
in the `_geo` variants the empty result is also in EPSG:4326. A field containing a null
value differs from a missing field; null values and zeros are preserved.

`OBJECTID` and `ID` retain their source names and use nullable `Int64`. Measurements use `float64`; textual codes retain their published text. Text uses the native pandas dtype (`str` on pandas 3, `object` on pandas 2), and empty results have the same dtypes as populated results.

## Limitations

- Hidrografia and irrigation demand require bbox (without a filter they would return hundreds of thousands of features)
- Only pivots support a UF filter; all layers accept bbox and max_registros
- There is no year or date-range filter; each query uses the configured layer edition
- Queries count records before downloading pages; even a small max_registros can require waiting for the count
- Pages accumulate in memory before building the result; there is no streaming or persistent ANA cache
- A 2s pause follows the sixth page and each subsequent page to avoid overloading the server
