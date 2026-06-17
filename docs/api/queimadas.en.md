# Fires API/INPE

The Queimadas module provides access to satellite-detected fire hotspot data, published by INPE (National Institute for Space Research) via BDQueimadas.

## Functions

### `focos`

Retrieves satellite-detected fire hotspots in Brazil.

```python
async def focos(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `ano` | `int` | Year (e.g. 2024) |
| `mes` | `int` | Month (1-12) |
| `dia` | `int \| None` | Specific day (1-31). If None, fetches the whole month |
| `uf` | `str \| None` | Filter by state (e.g. "MT", "SP"). Case insensitive |
| `bioma` | `str \| None` | Filter by biome (e.g. "Amazonia", "Cerrado") |
| `satelite` | `str \| None` | Filter by satellite (e.g. "AQUA_M-T", "NOAA-20") |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns:
- `data`: Fire hotspot date (date)
- `hora_gmt`: GMT time (str, "HH:MM" format)
- `lat`: Latitude (float)
- `lon`: Longitude (float)
- `satelite`: Detecting satellite name (str)
- `municipio`: Municipality name (str)
- `municipio_id`: IBGE municipality code (Int64)
- `estado`: State name (str)
- `bioma`: Biome (str) — Amazonia, Cerrado, Mata Atlantica, Caatinga, Pampa, Pantanal
- `numero_dias_sem_chuva`: Days without precipitation (float)
- `precipitacao`: Precipitation in mm (float)
- `risco_fogo`: Fire risk index 0-1 (float)
- `frp`: Fire Radiative Power in MW (float)
- `uf`: State code (str, 2 characters)

**Example:**

```python
from agrobr import queimadas

# All fire hotspots in September/2024
df = await queimadas.focos(ano=2024, mes=9)

# Fire hotspots for a specific day
df = await queimadas.focos(ano=2024, mes=9, dia=15)

# Filter by state
df = await queimadas.focos(ano=2024, mes=9, uf="MT")

# Filter by biome
df = await queimadas.focos(ano=2024, mes=9, bioma="Cerrado")

# Filter by satellite
df = await queimadas.focos(ano=2024, mes=9, satelite="AQUA_M-T")

# Combine filters
df = await queimadas.focos(ano=2024, mes=9, uf="MT", bioma="Amazonia")

# With provenance metadata
df, meta = await queimadas.focos(ano=2024, mes=9, return_meta=True)
print(meta.source, meta.records_count)
```

---

### `focos_geo`

Same fire hotspots as `focos`, but with point geometry. Returns a `GeoDataFrame` with a `geometry` column (Point EPSG:4326).

Requires the optional dependency: `pip install agrobr[geo]`

```python
async def focos_geo(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    return_meta: bool = False,
) -> gpd.GeoDataFrame | tuple[gpd.GeoDataFrame, MetaInfo]
```

**Parameters:** identical to `focos` (without `as_polars`).

**Returns:**

`GeoDataFrame` with the same columns as `focos` plus:

- `geometry`: Point EPSG:4326 derived from `lon`/`lat`

**Example:**

```python
from agrobr import queimadas

gdf = await queimadas.focos_geo(ano=2024, mes=9, uf="MT")

# Geospatial join with biomes/municipalities
import geopandas as gpd
municipios = gpd.read_file("municipios.geojson")
focos_por_municipio = gpd.sjoin(gdf, municipios)
```

---

## Available Satellites

| Satellite | Description |
|----------|-----------|
| `AQUA_M-T` | AQUA MODIS (reference) |
| `AQUA_M-M` | AQUA MODIS (Morning) |
| `TERRA_M-T` | TERRA MODIS (Afternoon) |
| `TERRA_M-M` | TERRA MODIS (Morning) |
| `NOAA-20` | NOAA-20 VIIRS |
| `NOAA-21` | NOAA-21 VIIRS |
| `GOES-16` | GOES-16 (geostationary) |
| `GOES-19` | GOES-19 (geostationary) |
| `METOP-B` | MetOp-B |
| `METOP-C` | MetOp-C |
| `MSG-03` | Meteosat |
| `NPP-375` | Suomi NPP 375m |
| `NPP-375D` | Suomi NPP 375m (daytime) |

## Biomes

| Biome | Approximate Area |
|-------|-----------------|
| Amazonia | 4.2M km2 |
| Cerrado | 2.0M km2 |
| Mata Atlantica | 1.1M km2 |
| Caatinga | 844k km2 |
| Pampa | 176k km2 |
| Pantanal | 150k km2 |

## Synchronous Version

```python
from agrobr.sync import queimadas

df = queimadas.focos(ano=2024, mes=9, uf="MT")
df, meta = queimadas.focos(ano=2024, mes=9, return_meta=True)
gdf = queimadas.focos_geo(ano=2024, mes=9, uf="MT")
```
