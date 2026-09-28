# EMBRAPA Solos API

The embrapa_solos module provides soil profiles and the pedological map of Brazil via the EMBRAPA GeoInfo WFS.

## Functions

### `perfis`

PronaSolos soil profiles (tabular).

```python
async def perfis(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = 50000,
    tamanho_pagina: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-----------|
| `uf` | `str \| None` | Filter by state (abbreviation, e.g. "MT") |
| `bbox` | `tuple \| None` | Bounding box (lon_min, lat_min, lon_max, lat_max) |
| `max_registros` | `int \| None` | Cap on occurrences read in `fid` order (default 50,000; `None` reads the whole layer). Local filters only see this prefix; a cut that leaves the selection partial raises a `UserWarning` |
| `tamanho_pagina` | `int \| None` | Occurrences per page: default 250 (profiles) / 500 (map), maximum 1000; with geometry (`_geo` or `bbox`), 100 / 25, maximum 100 |
| `as_polars` | `bool` | Return a polars DataFrame |
| `return_meta` | `bool` | Return a (DataFrame, MetaInfo) tuple |

**Returns:** DataFrame with 85 columns (`embrapa_solos.perfis` 2.0 contract), one row per horizon or layer. It starts with `fid`, `uf`, `municipio`, `latitude`, `longitude`, `horizonte`, `profundidade`, `areia_total`, `silte`, `argila`, `ph_h2o`, `carbono_organico`, `ctc`, `saturacao_bases`, `aluminio`, `fosforo`, `classe_textural`, `nivel_levantamento`, `uso_atual`, followed by the other published attributes, `uf_original` and `feature_id`. Laboratory values are the published text (including `NULL`); see the [source page](../sources/embrapa_solos.en.md#columns-profiles)

**Example:**

```python
from agrobr import embrapa_solos

# All profiles
df = await embrapa_solos.perfis()

# Mato Grosso profiles
df = await embrapa_solos.perfis(uf="MT")
```

---

### `perfis_geo`

Soil profiles with geometry (GeoDataFrame). Requires `pip install agrobr[geo]`.

```python
async def perfis_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = 5000,
    tamanho_pagina: int | None = None,
    return_meta: bool = False,
) -> gpd.GeoDataFrame | tuple[gpd.GeoDataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-----------|
| `uf` | `str \| None` | Filter by state (abbreviation) |
| `bbox` | `tuple \| None` | Bounding box (lon_min, lat_min, lon_max, lat_max) |
| `max_registros` | `int \| None` | Cap on occurrences read in `fid` order (default 5,000; `None` reads the whole layer). Local filters only see this prefix; a cut that leaves the selection partial raises a `UserWarning` |
| `tamanho_pagina` | `int \| None` | Occurrences per page: default 250 (profiles) / 500 (map), maximum 1000; with geometry (`_geo` or `bbox`), 100 / 25, maximum 100 |
| `return_meta` | `bool` | Return a (GeoDataFrame, MetaInfo) tuple |

**Returns:** GeoDataFrame (Point, EPSG:4326) with the same columns as `perfis()` + `geometry`

**Example:**

```python
from agrobr import embrapa_solos

# Profiles with geometry in a bbox
gdf = await embrapa_solos.perfis_geo(bbox=(-56, -16, -54, -14))
```

---

### `mapa_solos`

Pedological map of Brazil — SiBCS classification (tabular).

```python
async def mapa_solos(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = 50000,
    tamanho_pagina: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-----------|
| `ordem` | `str \| None` | Filter by the order of the 1st component (`ordem1`; contains, case-insensitive), applied locally to the prefix read. E.g. "LATOSSOLO" |
| `bbox` | `tuple \| None` | Bounding box (lon_min, lat_min, lon_max, lat_max) |
| `max_registros` | `int \| None` | Cap on occurrences read in `fid` order (default 50,000; `None` reads the whole layer). Local filters only see this prefix; a cut that leaves the selection partial raises a `UserWarning` |
| `tamanho_pagina` | `int \| None` | Occurrences per page: default 250 (profiles) / 500 (map), maximum 1000; with geometry (`_geo` or `bbox`), 100 / 25, maximum 100 |
| `as_polars` | `bool` | Return a polars DataFrame |
| `return_meta` | `bool` | Return a (DataFrame, MetaInfo) tuple |

**Returns:** DataFrame with columns: `fid`, `simbolos`, `comp1`, `comp2`, `comp3`, `legenda`, `area_km2`, `ordem1`, `subordem1`, `gdegrupo1`, `ordem2`, `subordem2`, `gdegrupo2`, `legenda_sinotica`, `classe_dom`, `ordem3`, `subordem3`, `gdegrupo3`, `feature_id`

**Example:**

```python
from agrobr import embrapa_solos

# Full map
df = await embrapa_solos.mapa_solos()

# Latosols
df = await embrapa_solos.mapa_solos(ordem="LATOSSOLO")
```

---

### `mapa_solos_geo`

Pedological map with geometry (GeoDataFrame). Requires `pip install agrobr[geo]`.

```python
async def mapa_solos_geo(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = 3000,
    tamanho_pagina: int | None = None,
    return_meta: bool = False,
) -> gpd.GeoDataFrame | tuple[gpd.GeoDataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-----------|
| `ordem` | `str \| None` | Filter by the order of the 1st component (`ordem1`; contains, case-insensitive), applied locally to the prefix read |
| `bbox` | `tuple \| None` | Bounding box (lon_min, lat_min, lon_max, lat_max) |
| `max_registros` | `int \| None` | Cap on occurrences read in `fid` order (default 3,000; `None` reads the whole layer). Local filters only see this prefix; a cut that leaves the selection partial raises a `UserWarning` |
| `tamanho_pagina` | `int \| None` | Occurrences per page: default 250 (profiles) / 500 (map), maximum 1000; with geometry (`_geo` or `bbox`), 100 / 25, maximum 100 |
| `return_meta` | `bool` | Return a (GeoDataFrame, MetaInfo) tuple |

**Returns:** GeoDataFrame (MultiPolygon, EPSG:4326) with the same columns as `mapa_solos()` + `geometry`

**Example:**

```python
from agrobr import embrapa_solos

# Soil polygons in a bbox
gdf = await embrapa_solos.mapa_solos_geo(bbox=(-56, -16, -54, -14))
```

## Synchronous Version

```python
from agrobr.sync import embrapa_solos

df = embrapa_solos.perfis(uf="MT")
df = embrapa_solos.mapa_solos()
```

## Notes

- Source: [EMBRAPA GeoInfo](https://geoinfo.dados.embrapa.br) — license `nc` (CC BY-NC 3.0 BR)
- The `_geo()` functions require `pip install agrobr[geo]` (geopandas)
- 34,464 horizon/layer records (~9K points, PronaSolos 2020) and 2,852 pedological polygons
- Automatic WFS pagination
- `bbox` and GeoDataFrame in EPSG:4326, the default CRS of both layers in the WFS
