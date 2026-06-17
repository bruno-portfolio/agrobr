# Deforestation (PRODES/DETER)

Deforestation data from INPE via TerraBrasilis — PRODES (annual consolidated) and DETER (near real-time alerts).

## `desmatamento.prodes()`

Annual consolidated deforestation data by polygon.

```python
import agrobr

df = await agrobr.desmatamento.prodes(bioma="Cerrado", ano=2022, uf="MT")
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `bioma` | `str` | No | Biome: "Amazonia", "Cerrado", "Caatinga", "Mata Atlantica", "Pantanal", "Pampa". Default: "Cerrado" |
| `ano` | `int` | No | Year (e.g. 2022). If None, all years |
| `uf` | `str` | No | Filter by state (e.g. "MT") |
| `as_polars` | `bool` | No | Return as polars.DataFrame |
| `return_meta` | `bool` | No | If True, returns `(DataFrame, MetaInfo)` |

### Returned Columns

| Column | Type | Description |
|--------|------|-------------|
| `ano` | int | Deforestation year |
| `uf` | str | State code (e.g. "MT") |
| `classe` | str | Coverage class (e.g. "desmatamento") |
| `area_km2` | float | Deforested area in km2 |
| `satelite` | str | Satellite used |
| `sensor` | str | Satellite sensor |
| `bioma` | str | Queried biome |

### Available Biomes (PRODES)

| Biome | GeoServer Workspace | Layer | Historical Series |
|-------|--------------------|----|---|
| Amazonia | prodes-amazon-nb | yearly_deforestation_biome | 2000+ |
| Cerrado | prodes-cerrado-nb | yearly_deforestation | 2000+ |
| Caatinga | prodes-caatinga-nb | yearly_deforestation | 2000+ |
| Mata Atlantica | prodes-mata-atlantica-nb | yearly_deforestation | 2000+ |
| Pantanal | prodes-pantanal-nb | yearly_deforestation | 2000+ |
| Pampa | prodes-pampa-nb | yearly_deforestation | 2000+ |

---

## `desmatamento.prodes_geo()`

PRODES data with geometry (polygons). Returns a `GeoDataFrame` with a `geometry` column (MultiPolygon EPSG:4326).

Requires the optional dependency: `pip install agrobr[geo]`

```python
import agrobr

gdf = await agrobr.desmatamento.prodes_geo(
    bioma="Cerrado",
    ano=2022,
    uf="MT",
)

# Geospatial join with CAR/SICAR
import geopandas as gpd
car = gpd.read_file("imoveis_car.geojson")
desmatamento_em_car = gpd.sjoin(gdf, car)
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `bioma` | `str` | No | Biome: "Amazonia", "Cerrado", "Caatinga", "Mata Atlantica", "Pantanal", "Pampa". Default: "Cerrado" |
| `ano` | `int` | No | Year (e.g. 2022). If None, all years |
| `uf` | `str` | No | Filter by state (e.g. "MT") |
| `return_meta` | `bool` | No | If True, returns `(GeoDataFrame, MetaInfo)` |

### Returned Columns

| Column | Type | Description |
|--------|------|-------------|
| `ano` | int | Deforestation year |
| `uf` | str | State code (e.g. "MT") |
| `classe` | str | Coverage class (e.g. "desmatamento") |
| `area_km2` | float | Deforested area in km2 |
| `satelite` | str | Satellite used |
| `sensor` | str | Satellite sensor |
| `bioma` | str | Queried biome |
| `geometry` | geometry | MultiPolygon EPSG:4326 |

### Notes

- Default `maxFeatures=10000` — use filters (uf, ano) to reduce volume
- A warning is logged automatically if the response reaches the feature limit (possible truncation)
- The geometry column in GeoServer is `geom` for all 6 biomes (uniform)

---

## `desmatamento.deter()`

Near real-time daily deforestation alerts.

```python
import agrobr

df = await agrobr.desmatamento.deter(
    bioma="Amazônia",
    uf="PA",
    data_inicio="2024-01-01",
    data_fim="2024-06-30",
)
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `bioma` | `str` | No | Biome: "Amazonia", "Cerrado". Default: "Amazonia" |
| `uf` | `str` | No | Filter by state (e.g. "PA") |
| `data_inicio` | `str` | No | Start date YYYY-MM-DD |
| `data_fim` | `str` | No | End date YYYY-MM-DD |
| `classe` | `str` | No | Filter by alert class |
| `as_polars` | `bool` | No | Return as polars.DataFrame |
| `return_meta` | `bool` | No | If True, returns `(DataFrame, MetaInfo)` |

### Returned Columns

| Column | Type | Description |
|--------|------|-------------|
| `data` | date | Alert date |
| `classe` | str | Alert type (DESMATAMENTO_CR, DEGRADACAO, MINERACAO, etc.) |
| `uf` | str | State code |
| `municipio` | str | Municipality name |
| `municipio_id` | int | IBGE municipality code |
| `area_km2` | float | Area in km2 |
| `satelite` | str | Satellite used |
| `sensor` | str | Satellite sensor |
| `bioma` | str | Queried biome |

### DETER Classes

| Class | Description |
|--------|-----------|
| `DESMATAMENTO_CR` | Clear-cut deforestation |
| `DESMATAMENTO_VEG` | Deforestation with secondary vegetation |
| `DEGRADACAO` | Forest degradation |
| `MINERACAO` | Mining activity |
| `CICATRIZ_DE_QUEIMADA` | Burn scar |
| `CS_DESORDENADO` | Disordered selective logging |
| `CS_GEOMETRICO` | Geometric selective logging |

---

## `desmatamento.deter_geo()`

DETER alerts with geometry (polygons). Returns a `GeoDataFrame` with a `geometry` column (MultiPolygon EPSG:4326).

Requires the optional dependency: `pip install agrobr[geo]`

```python
import agrobr

gdf = await agrobr.desmatamento.deter_geo(
    bioma="Amazônia",
    uf="PA",
    data_inicio="2024-01-01",
    data_fim="2024-06-30",
)

# Geospatial join with CAR/SICAR
import geopandas as gpd
car = gpd.read_file("imoveis_car.geojson")
alertas_em_reserva = gpd.sjoin(gdf, car[car["tipo"] == "RESERVA_LEGAL"])
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `bioma` | `str` | No | Biome: "Amazonia", "Cerrado". Default: "Amazonia" |
| `uf` | `str` | No | Filter by state (e.g. "PA") |
| `data_inicio` | `str` | No | Start date YYYY-MM-DD |
| `data_fim` | `str` | No | End date YYYY-MM-DD |
| `classe` | `str` | No | Filter by alert class |
| `return_meta` | `bool` | No | If True, returns `(GeoDataFrame, MetaInfo)` |

### Returned Columns

| Column | Type | Description |
|--------|------|-------------|
| `data` | date | Alert date |
| `classe` | str | Alert type (DESMATAMENTO_CR, DEGRADACAO, MINERACAO, etc.) |
| `uf` | str | State code |
| `municipio` | str | Municipality name |
| `municipio_id` | int | IBGE municipality code |
| `area_km2` | float | Area in km2 |
| `satelite` | str | Satellite used |
| `sensor` | str | Satellite sensor |
| `bioma` | str | Queried biome |
| `geometry` | geometry | MultiPolygon EPSG:4326 |

### Notes

- Default `maxFeatures=10000` — use filters (uf, data_inicio/data_fim, classe) to reduce volume
- A warning is logged automatically if the response reaches the feature limit (possible truncation)
- Approximate volume: ~1.1 KB per feature with geometry

---

## Synchronous Usage

```python
from agrobr import sync

df = sync.desmatamento.prodes(bioma="Cerrado", ano=2022, uf="MT")
gdf_prodes = sync.desmatamento.prodes_geo(bioma="Cerrado", ano=2022, uf="MT")
df_deter = sync.desmatamento.deter(bioma="Amazônia", uf="PA", data_inicio="2024-01-01")
gdf = sync.desmatamento.deter_geo(bioma="Amazônia", uf="PA", data_inicio="2024-01-01")
```

## Data Source

- **PRODES**: Satellite Monitoring Program for the Amazon Forest and other Brazilian Biomes
- **DETER**: Near Real-Time Deforestation Detection System
- **Provider**: INPE — National Institute for Space Research
- **API**: TerraBrasilis GeoServer (WFS)
- **License**: Federal government public data — free to use with attribution
