# ANA/SNIRH — Agencia Nacional de Aguas

## Overview

| Item | Detail |
|------|---------|
| Provider | ANA (Agencia Nacional de Aguas e Saneamento Basico) |
| Data | Hydrography, irrigation pivots, irrigation demand, water availability, water bodies |
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
| `massas_dagua` | ~241K polygons | Polygon | `uf` or `bbox` |

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

    # Water bodies (uf or bbox required)
    df = await ana.massas_dagua(uf="DF")
    gdf = await ana.massas_dagua_geo(bbox=(-47.6, -16.0, -47.5, -15.9))

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

## Water bodies (massas d'água)

`massas_dagua` and `massas_dagua_geo` read the polygons of lakes, ponds, reservoirs and river reaches drawn as polygons,
from ANA's official "Massas d'Água" theme (2019 version, 1:100,000 scale, [metadata in ANA's catalog](https://metadados.snirh.gov.br/geonetwork/srv/api/records/7d054e5a-8cc9-403c-9f1a-085fd933610c),
license: "O acesso ao dado é livre." — access to the data is free). `hidrografia` is a different layer: only the drainage
lines (BHO), no polygons.

| Item | Detail |
|------|--------|
| Service | `https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0` (the one the metadata points to) |
| Features | 240,899 polygons (the count of the 2019 national shapefile) on 2026-10-01 |
| Volume | DF: 228 features, ~0.9 MB of GeoJSON; RS: 33,467 features, ~140 MB at the DF average |
| Required filter | `uf` or `bbox` (the whole country exceeds 1 GB); both together = intersection |

- **`uf`**: the `nmufe` field holds the state name in upper case, with accents, and lists the states of a border feature
  separated by commas (`"DISTRITO FEDERAL, GOIÁS"`). The filter matches the whole name as a list item: `uf="MT"` does
  not match `MATO GROSSO DO SUL`, and `uf="DF"` matches the border feature. In the output, the `uf` column holds the
  abbreviations (`"DF/GO"`): filtering the result with `df["uf"] == "DF"` drops border features. 106 features have no
  state and only come through `bbox`. A name outside the registry leaves that feature's `uf` null and becomes a warning in
  `MetaInfo.validation_warnings`.
- **Pagination by `FID` range**: this server rejects `resultRecordCount` and `orderByFields` ("Pagination is not
  supported"). The query counts the features, reads the official `FID` list (`returnIdsOnly`) and requests each range of
  up to 1,000 `FID` in the `where` clause. Each page must return exactly the range's `FID`; a missing one, or a list that
  does not match the count, raises `SourceUnavailableError`. An unreadable page, a feature missing any of the 19 requested fields (the source sends all of them, nulls
  included) or an unreadable geometry raises `ParseError`.
  `max_registros` trims the `FID` list, in ascending order.
- **Published values**: the source publishes empty text as `" "` (returned as null) and many missing numbers as `0`
  (in DF, on 2026-10-01: `volume_hm3` is 0 in 198 of the 228 features, `codigo_snisb` in 188 and `codigo_trecho` in 222); zeros are preserved. `FID`
  is the service's internal key and is not returned; the official code is `codigo`.
- **Left out**: the developer's name (`nmemp`, which holds natural persons' names) is not requested from the service.
- The `dados_abertos/Massa_d_Agua` service on the portal of the other layers returned error 500 on 2026-10-01; that is why
  agrobr uses `SPR/Massa_dagua`. The metadata's national shapefile (426 MB, not split by state) is not downloaded.

| Column | Type | Source field | Description |
|--------|------|--------------|-------------|
| codigo | Int64 | `esp_cd` | Water body code |
| nome | str | `nmoriginal` | Original name |
| nome_alternativo | str | `nmalternat` | Alternative name |
| tipo | str | `detipomass` | `Natural` or `Artificial` |
| dominio | str | `dedominial` | Jurisdiction (`Federal` or `Estadual`), as in `hidrografia` |
| entidade_fiscalizadora | str | `defiscaliz` | Supervising body |
| uso_principal | str | `usoprinc` | Main water use |
| volume_hm3 | float | `nuvolumhm3` | Storage capacity (hm³) |
| area_km2 | float | `nuareakm2` | Area (km²) |
| area_ha | float | `nuareaha` | Area (ha) |
| perimetro_km | float | `nuperimkm` | Perimeter (km) |
| data_construcao | datetime | `dtreserv` | Reservoir construction date (`dd/mm/yyyy` at the source) |
| nome_rio | str | `nmriocomp` | River name |
| codigo_snisb | Int64 | `cod_snisb` | SNISB code |
| codigo_trecho | Int64 | `cotrecho` | BHO drainage reach code |
| uf | str | `nmufe` | State abbreviations, in alphabetical order and separated by `/` (`"DF/GO"`) |
| municipios | str | `nmmun` | Municipalities, as published (`"BRASÍLIA, FORMOSA"`) |
| fonte_geometria | str | `deversao` | Geometry origin (`FBDS (2017)`, `bho_massa_dagua_2019`…) |

## Specifics

- **bbox required**: `hidrografia` and `demanda_irrigacao` require bbox (large datasets)
- **Keyset pagination**: each page asks for up to 1K features ordered by `OBJECTID` and the next one continues from the last `OBJECTID` received, until the official count is reached. The Hidrografia server returns one feature fewer than requested on each page; with offset pagination the boundary feature was lost (1,046 of 1,047 in the test extract). If pagination stops before the official count, or a page does not advance the `OBJECTID`, the query raises `SourceUnavailableError` stating how many features are missing, instead of returning a partial result. An unreadable page (truncated JSON, proxy HTML) or a page without `OBJECTID` raises `ParseError`
- **max_registros**: positive integer limiting the returned features, or `None` for no cap. Zero, negative values, booleans and non-integers raise `InvalidParameterError` before collection. The previous `max_features` argument is no longer accepted
- **Required fields**: every field requested from the service (`outFields`) must be present in every feature of every page in the tabular functions, and in every page in the `_geo` variants. The service sends all of them, nulls included, so a missing field is a layout change and raises `ParseError` naming the field, instead of returning one column fewer

| Layer | Requested fields |
|-------|------------------|
| `hidrografia` | `OBJECTID`, `COCURSODAG`, `COBACIA`, `NORIOCOMP`, `DEDOMINIAL` |
| `pivos_irrigacao` | `OBJECTID`, `CD_GEOCMU`, `NM_MUNICIP`, `NM_ESTADO`, `REGIAO_HID`, `HECTARES` |
| `demanda_irrigacao` | `OBJECTID`, `ID`, `COBACIA`, `DSVERSAO`, `VZMAXMEN`, `VZMESSEC`, `VZMESIRR`, `VZMEDANO` |
| `disponibilidade_hidrica` | `OBJECTID`, `ID`, `NUAREAMONT`, `DISPQ95`, `NMRIO`, `DEDOMINIAL`, `DSVERSAO` |
| `massas_dagua` | the 19 fields in the water bodies column table |

An official count of zero returns a valid empty result with the published columns;
in the `_geo` variants the empty result is also in EPSG:4326. A field containing a null
value differs from a missing field; null values and zeros are preserved.

`OBJECTID` and `ID` retain their source names and use nullable `Int64`. Measurements use `float64`; textual codes retain their published text. Text uses the native pandas dtype (`str` on pandas 3, `object` on pandas 2), and empty results have the same dtypes as populated results.

## Limitations

- Hidrografia and irrigation demand require bbox (without a filter they would return hundreds of thousands of features)
- Only pivots and water bodies support a UF filter; all layers accept bbox and max_registros
- There is no year or date-range filter; each query uses the configured layer edition
- Queries count records before downloading pages; even a small max_registros can require waiting for the count
- Pages accumulate in memory before building the result; there is no streaming or persistent ANA cache
- A 2s pause follows the sixth page and each subsequent page to avoid overloading the server
