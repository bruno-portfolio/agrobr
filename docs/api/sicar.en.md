# SICAR (Cadastro Ambiental Rural)

Tabular rural property data from the CAR via the SICAR GeoServer WFS.

## imoveis

Individual rural property records (without geometry).

```python
import agrobr

df = await agrobr.alt.sicar.imoveis("DF")
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| uf | str | Yes | State abbreviation (e.g. "MT", "DF", "BA") |
| municipio | str | No | Partial municipality filter (case-insensitive). Mutually exclusive with `cod_municipio` |
| cod_municipio | int | No | Municipality IBGE code (e.g. 5107925). Mutually exclusive with `municipio` |
| status | str | No | AT, PE, SU or CA |
| tipo | str | No | IRU, AST or PCT |
| area_min | float | No | Minimum area in hectares |
| area_max | float | No | Maximum area in hectares |
| criado_apos | str | No | Minimum creation date (ISO, e.g. "2020-01-01") |
| atualizado_apos | str | No | Server-side filter for `data_atualizacao` after this date (ISO, e.g. "2026-06-07" or "2026-06-07T00:00:00"). The `data_atualizacao` column is not returned by the WFS (it comes back empty in the result). Unavailable for SP, RS, PR, SC, RJ, TO (the field does not exist in those WFS layers) |
| as_polars | bool | No | If True, returns a polars.DataFrame |
| return_meta | bool | No | If True, returns (DataFrame, MetaInfo) |

### Returned columns

| Column | Type | Description |
|--------|------|-------------|
| cod_imovel | str | Unique property code |
| status | str | AT/PE/SU/CA |
| data_criacao | datetime | Creation date |
| data_atualizacao | datetime | Last update (nullable) |
| area_ha | float | Area in hectares |
| condicao | str | Registration status (nullable) |
| uf | str | State abbreviation |
| municipio | str | Municipality name |
| cod_municipio_ibge | int | IBGE code |
| modulos_fiscais | float | Fiscal modules |
| tipo | str | IRU/AST/PCT |

### Examples

```python
# Active properties in Sorriso-MT
df = await agrobr.alt.sicar.imoveis(
    "MT", municipio="Sorriso", status="AT"
)

# Filter by IBGE code (avoids accent issues)
df = await agrobr.alt.sicar.imoveis("PA", cod_municipio=1508159)  # Uruara

# Large properties (>1000 ha) in DF
df = await agrobr.alt.sicar.imoveis("DF", area_min=1000)

# Registrations created after 2020
df = await agrobr.alt.sicar.imoveis(
    "GO", criado_apos="2020-01-01"
)

# Registrations updated after a date (useful to sync the base incrementally)
df = await agrobr.alt.sicar.imoveis(
    "MG", atualizado_apos="2026-06-07T00:00:00"
)

# With provenance metadata
df, meta = await agrobr.alt.sicar.imoveis("DF", return_meta=True)
print(meta.records_count, meta.fetch_duration_ms)
```

## resumo

Aggregated statistics by state or municipality.

```python
df = await agrobr.alt.sicar.resumo("MT")
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| uf | str | Yes | State abbreviation |
| municipio | str | No | Partial municipality filter (case-insensitive). Mutually exclusive with `cod_municipio` |
| cod_municipio | int | No | Municipality IBGE code. Mutually exclusive with `municipio` |
| as_polars | bool | No | If True, returns a polars.DataFrame |
| return_meta | bool | No | If True, returns (DataFrame, MetaInfo) |

### Return without municipality (state-level)

Uses `resultType=hits` (4 fast requests, no data download):

| Column | Type | Description |
|--------|------|-------------|
| total | int | Total properties |
| ativos | int | Properties with status AT |
| pendentes | int | Properties with status PE |
| suspensos | int | Properties with status SU |
| cancelados | int | Properties with status CA |

### Return with municipality

Fetches data and aggregates client-side:

| Column | Type | Description |
|--------|------|-------------|
| total | int | Total properties |
| ativos | int | Properties with status AT |
| pendentes | int | Properties with status PE |
| suspensos | int | Properties with status SU |
| cancelados | int | Properties with status CA |
| area_total_ha | float | Sum of areas |
| area_media_ha | float | Mean area |
| modulos_fiscais_medio | float | Mean fiscal modules |
| por_tipo_IRU | int | Rural properties |
| por_tipo_AST | int | Settlements |
| por_tipo_PCT | int | Indigenous lands |

### Examples

```python
# DF summary (fast, no download)
df = await agrobr.alt.sicar.resumo("DF")

# Sorriso-MT summary (with aggregation)
df = await agrobr.alt.sicar.resumo("MT", municipio="Sorriso")
```

## imoveis_geo

Individual records with geometry (MultiPolygon polygons). Requires `pip install agrobr[geo]`.

```python
import agrobr

gdf = await agrobr.alt.sicar.imoveis_geo("DF")
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| uf | str | Yes | State abbreviation (e.g. "MT", "DF", "BA") |
| municipio | str | No | Partial municipality filter (case-insensitive). Mutually exclusive with `cod_municipio` |
| cod_municipio | int | No | Municipality IBGE code (e.g. 5107925). Mutually exclusive with `municipio` |
| status | str | No | AT, PE, SU or CA |
| tipo | str | No | IRU, AST or PCT |
| area_min | float | No | Minimum area in hectares |
| area_max | float | No | Maximum area in hectares |
| criado_apos | str | No | Minimum creation date (ISO, e.g. "2020-01-01") |
| atualizado_apos | str | No | Server-side filter for `data_atualizacao` after this date (ISO, e.g. "2026-06-07" or "2026-06-07T00:00:00"). The `data_atualizacao` column is not returned by the WFS (it comes back empty in the result). Unavailable for SP, RS, PR, SC, RJ, TO (the field does not exist in those WFS layers) |
| max_features | int \| None | No | Limit on returned features. Default: 5000. `None` disables the limit |
| return_meta | bool | No | If True, returns (GeoDataFrame, MetaInfo) |

### Returned columns

| Column | Type | Description |
|--------|------|-------------|
| cod_imovel | str | Unique property code |
| status | str | AT/PE/SU/CA |
| data_criacao | datetime | Creation date |
| data_atualizacao | datetime | Last update (nullable) |
| area_ha | float | Area in hectares |
| condicao | str | Registration status (nullable) |
| uf | str | State abbreviation |
| municipio | str | Municipality name |
| cod_municipio_ibge | int | IBGE code |
| modulos_fiscais | float | Fiscal modules |
| tipo | str | IRU/AST/PCT |
| geometry | MultiPolygon | Property polygon (EPSG:4326) |

### Examples

```python
# Properties with geometry in DF
gdf = await agrobr.alt.sicar.imoveis_geo("DF")
gdf.plot()

# Filter by municipality (name)
gdf = await agrobr.alt.sicar.imoveis_geo(
    "MT", municipio="Sorriso", status="AT"
)

# Filter by IBGE code (avoids accent issues)
gdf = await agrobr.alt.sicar.imoveis_geo("PA", cod_municipio=1508159)

# With metadata
gdf, meta = await agrobr.alt.sicar.imoveis_geo("DF", return_meta=True)
```

### Notes

- Maximum 5,000 features per request (warning if truncated)
- Single request without pagination (controlled volume)
- CRS: EPSG:4326 (WGS84)

## imoveis_geo_stream

Iterates over the properties with geometry of a state in batches, without accumulating everything in
memory before you start using the data. Requires `pip install agrobr[geo]`.

```python
import agrobr

async for gdf in agrobr.alt.sicar.imoveis_geo_stream("MT"):
    print(len(gdf))
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| uf | str | Yes | State abbreviation (e.g. "MT", "DF", "BA") |
| municipio | str | No | Partial municipality filter (case-insensitive). Mutually exclusive with `cod_municipio` |
| cod_municipio | int | No | Municipality IBGE code (e.g. 5107925). Mutually exclusive with `municipio` |
| status | str | No | AT, PE, SU or CA |
| tipo | str | No | IRU, AST or PCT |
| area_min | float | No | Minimum area in hectares |
| area_max | float | No | Maximum area in hectares |
| criado_apos | str | No | Minimum creation date (ISO, e.g. "2020-01-01") |
| atualizado_apos | str | No | Server-side filter for `data_atualizacao` after this date (ISO, e.g. "2026-06-07" or "2026-06-07T00:00:00"). The `data_atualizacao` column is not returned by the WFS (it comes back empty in the result). Unavailable for SP, RS, PR, SC, RJ, TO (the field does not exist in those WFS layers) |

Each yielded item is a `GeoDataFrame` with the same columns as [`imoveis_geo`](#imoveis_geo).

### Examples

```python
# Accumulate the total properties of Sorriso-MT without keeping everything in memory
total = 0
async for gdf in agrobr.alt.sicar.imoveis_geo_stream("MT", municipio="Sorriso"):
    total += len(gdf)
print(total)
```

### Notes

- No `max_features` limit: pages until all the state's records are exhausted
- Each yield contains up to 10,000 features (one WFS page), downloaded sequentially with throttle
- Deduplicates `cod_imovel` across batches
- CRS: EPSG:4326 (WGS84)
- Async-only: `agrobr.sync` does not support async generators

## Synchronous Usage

```python
from agrobr import sync

df = sync.sicar.imoveis("DF")
gdf = sync.sicar.imoveis_geo("DF")
df = sync.sicar.resumo("MT", municipio="Sorriso")
```

## Data source

- **Provider:** Brazilian Forest Service (SFB) / SICAR
- **API:** WFS 2.0.0 (OGC GeoServer)
- **License:** CC-BY (federal government open data)
- **Update:** continuous (real-time registrations)
