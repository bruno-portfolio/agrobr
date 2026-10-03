# SICAR (Cadastro Ambiental Rural)

Tabular rural property data from the CAR via the SICAR GeoServer WFS.

## imoveis

Individual rural property records, using GeoJSON projected to attributes only. The tabular query does not require GeoPandas. Contract 2.1 requires UTC creation and update dates, including null columns and empty results.

The `atualizado_apos` filter supports millisecond precision. Additional zeros preserve the instant: `.212000` is sent as `.212`. Submillisecond fractions, such as `.212001`, raise `InvalidParameterError` before network access; GeoServer did not compare these representations with the expected ISO semantics. The same rule applies to both geometry APIs.

```python
import agrobr

df = await agrobr.alt.sicar.imoveis("DF")
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| uf | str | Yes | State abbreviation (e.g. "MT", "DF", "BA") |
| municipio | int \| str | No | 7-digit IBGE code (int or str) or the full municipality name, ignoring case and accents (`normalize.resolver_municipio`); it must belong to the state. An ambiguous or unknown name, or a fragment of a name (`"Santa Rita"` does not match `"Santa Rita do Sapucaí"`), raises `InvalidParameterError` listing the candidates. Filters by the code on the layer |
| status | str | No | AT, PE, SU or CA |
| tipo | str | No | IRU, AST or PCT |
| area_min | float | No | Minimum area in hectares |
| area_max | float | No | Maximum area in hectares |
| criado_apos | str | No | Valid `YYYY-MM-DD` date; creation on or after the cutoff (`>=`) |
| atualizado_apos | str | No | Update strictly after the cutoff (`>`), as an ISO date/datetime with optional fraction and `Z`/offset; without a timezone, interpreted as UTC. The field is requested where available. Unavailable in PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP and TO |
| as_polars | bool | No | If True, returns a polars.DataFrame |
| return_meta | bool | No | If True, returns (DataFrame, MetaInfo) |

Impossible dates, negative/nonfinite areas, reversed ranges and invalid types are rejected before network access. A municipality code is checked against the IBGE municipality register before network access: an unknown code or one from another state raises `InvalidParameterError`.

The filters query current records and do not reconstruct past versions. The [`cadastro_rural`](../contracts/cadastro_rural.md) dataset exposes the same tabular filters and rejects an active `deterministic` context.

Published occurrences sharing a `cod_imovel` are selected by the latest update when every
occurrence in the group has an update timestamp; otherwise by creation when all have creation;
otherwise by the highest numeric feature-ID suffix, which also breaks date ties. With
`return_meta=True`, warnings and `source_details["sicar"]` expose criteria and discarded
occurrences (up to 1,000 list entries, with a total count and truncation flag). Pagination rejects
repeated feature IDs. See the [full rule](../contracts/cadastro_rural.en.md#multiple-occurrences-and-provenance).

### Returned columns

| Column | Type | Description |
|--------|------|-------------|
| cod_imovel | str | Unique property code |
| status | str | AT/PE/SU/CA |
| data_criacao | datetime64[ns, UTC] | UTC instant of creation (nullable) |
| data_atualizacao | datetime64[ns, UTC] | UTC instant of update (nullable) |
| area_ha | float | Area in hectares |
| condicao | str | Registration status (nullable) |
| uf | str | State abbreviation |
| municipio | str | Municipality name |
| cod_municipio_ibge | int | IBGE code |
| modulos_fiscais | float | Fiscal modules |
| tipo | str | IRU/AST/PCT |
| cod_municipio | int | 7-digit IBGE code (same as `cod_municipio_ibge`); nullable |

### Examples

```python
# Active properties in Sorriso-MT
df = await agrobr.alt.sicar.imoveis(
    "MT", municipio="Sorriso", status="AT"
)

# Filter by IBGE code (avoids accent issues)
df = await agrobr.alt.sicar.imoveis("PA", municipio="Uruará")  # or municipio=1508159

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
| municipio | int \| str | No | 7-digit IBGE code (int or str) or the full municipality name, ignoring case and accents (`normalize.resolver_municipio`); it must belong to the state. An ambiguous or unknown name, or a fragment of a name (`"Santa Rita"` does not match `"Santa Rita do Sapucaí"`), raises `InvalidParameterError` listing the candidates. Filters by the code on the layer |
| as_polars | bool | No | If True, returns a polars.DataFrame |
| return_meta | bool | No | If True, returns (DataFrame, MetaInfo) |

### Return without municipality (state-level)

Uses `resultType=hits` (five queries: total and four statuses, without downloading records). The count is of **published features**: versions of the same `cod_imovel` in force on the layer count separately, so the total can exceed the number of properties and the sum of the municipality summaries, which count one version per `cod_imovel`. The output says so in `MetaInfo.source_details["sicar"]["unidade"] = "feicoes_publicadas"` and in a warning in `MetaInfo.validation_warnings` (with `return_meta=True`):

| Column | Type | Description |
|--------|------|-------------|
| total | int | Published features |
| ativos | int | Features with status AT |
| pendentes | int | Features with status PE |
| suspensos | int | Features with status SU |
| cancelados | int | Features with status CA |

### Return with municipality

Fetches data, applies the occurrence selection used by `imoveis()`, and aggregates client-side; selection warnings and details accompany `return_meta=True`:

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
| municipio | int \| str | No | 7-digit IBGE code (int or str) or the full municipality name, ignoring case and accents (`normalize.resolver_municipio`); it must belong to the state. An ambiguous or unknown name, or a fragment of a name (`"Santa Rita"` does not match `"Santa Rita do Sapucaí"`), raises `InvalidParameterError` listing the candidates. Filters by the code on the layer |
| status | str | No | AT, PE, SU or CA |
| tipo | str | No | IRU, AST or PCT |
| area_min | float | No | Minimum area in hectares |
| area_max | float | No | Maximum area in hectares |
| criado_apos | str | No | Minimum creation date (ISO, e.g. "2020-01-01") |
| atualizado_apos | str | No | Update strictly after the cutoff (`>`), as an ISO date/datetime with optional fraction and `Z`/offset; without a timezone, interpreted as UTC. The field is requested where available. Unavailable in PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP and TO |
| max_registros | int \| None | No | Limit on returned features. Default: 5000. `None` disables the limit |
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
| cod_municipio | int | 7-digit IBGE code (same as `cod_municipio_ibge`); nullable |
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
gdf = await agrobr.alt.sicar.imoveis_geo("PA", municipio=1508159)

# With metadata
gdf, meta = await agrobr.alt.sicar.imoveis_geo("DF", return_meta=True)
```

### Notes

- `max_registros=5000` is the default result limit; accepts a positive integer or `None`
- A result that stops at `max_registros` comes with a warning in `validation_warnings` and `UserWarning`, and `source_details["sicar"]` carries `truncado=True`, `max_registros` and `total_fonte`, the query total at the source (the WFS `numberMatched`); without that total, the warning says there may be more. In DF, the default returns 5,000 of 21,011 properties (2026-09-22 capture)
- Up to 10,000 features use one request; larger limits and `None` use pages of up to 10,000 features
- CRS: EPSG:4326 (WGS84). SICAR layers are published in SIRGAS 2000 (EPSG:4674); agrobr requests `srsName=EPSG:4326` and checks the CRS declared by every page with features (any other declaration raises `ParseError`). GeoServer performs the reprojection: in the 2026-09-22 captures, coordinates differ from the SIRGAS 2000 ones by at most 1e-8 degree. Empty results also carry the CRS
- Repeated occurrences of the same `cod_imovel` follow the [`imoveis()`](#imoveis) rule; with `return_meta=True`, `validation_warnings` and `source_details["sicar"]` record the discards. A repeated feature ID raises `ParseError`, as in tabular pagination
- Dates are UTC instants; a date without a timezone raises `ParseError`, as in the tabular path

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
| municipio | int \| str | No | 7-digit IBGE code (int or str) or the full municipality name, ignoring case and accents (`normalize.resolver_municipio`); it must belong to the state. An ambiguous or unknown name, or a fragment of a name (`"Santa Rita"` does not match `"Santa Rita do Sapucaí"`), raises `InvalidParameterError` listing the candidates. Filters by the code on the layer |
| status | str | No | AT, PE, SU or CA |
| tipo | str | No | IRU, AST or PCT |
| area_min | float | No | Minimum area in hectares |
| area_max | float | No | Maximum area in hectares |
| criado_apos | str | No | Minimum creation date (ISO, e.g. "2020-01-01") |
| atualizado_apos | str | No | Update strictly after the cutoff (`>`), as an ISO date/datetime with optional fraction and `Z`/offset; without a timezone, interpreted as UTC. The field is requested where available. Unavailable in PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP and TO |

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

- No `max_registros` limit: pages until all the state's records are exhausted
- Each yield corresponds to one WFS page (up to 10,000 features), downloaded sequentially with throttle. Occurrences of each page's last `cod_imovel` move to the next batch, because pages are sorted by `cod_imovel` and a repeated version may fall on the next page; the last batch holds only that code
- One occurrence per `cod_imovel`, by the [`imoveis()`](#imoveis) rule; a feature ID repeated across pages raises `ParseError`
- CRS: EPSG:4326 (WGS84), with the same check as [`imoveis_geo`](#imoveis_geo)
- Async-only: `agrobr.sync` does not support async generators

## Synchronous Usage

```python
from agrobr import sync

df = sync.sicar.imoveis("DF")
gdf = sync.sicar.imoveis_geo("DF")
df = sync.sicar.resumo("MT", municipio="Sorriso")
```

## Raw collection

To store the original WFS pages, with every version of each property and the manifest, use
[raw collection](bruto.md): `bruto.coletar("sicar", "imoveis", uf="DF", destino=...)`.

## Data source

- **Provider:** Brazilian Forest Service (SFB) / SICAR
- **API:** WFS 2.0.0 (OGC GeoServer)
- **License:** `livre` under the federal public-data framework; no CC BY license verified for the database. Preserve source and provenance; see [Licenses](../licenses.md#sicar).
- **Update:** continuous (real-time registrations)
