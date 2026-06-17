# API EMBRAPA Solos

O modulo embrapa_solos fornece perfis de solo e mapa pedologico do Brasil via WFS EMBRAPA GeoInfo.

## Funcoes

### `perfis`

Perfis de solo PronaSolos (tabular).

```python
async def perfis(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `uf` | `str \| None` | Filtro por UF (sigla, ex: "MT") |
| `bbox` | `tuple \| None` | Bounding box (lon_min, lat_min, lon_max, lat_max) |
| `as_polars` | `bool` | Retorna polars DataFrame |
| `return_meta` | `bool` | Retorna tupla (DataFrame, MetaInfo) |

**Retorno:** DataFrame com colunas: `fid`, `uf`, `municipio`, `latitude`, `longitude`, `horizonte`, `profundidade`, `areia_total`, `silte`, `argila`, `ph_h2o`, `carbono_organico`, `ctc`, `saturacao_bases`, `aluminio`, `fosforo`, `classe_textural`, `nivel_levantamento`, `uso_atual`

**Exemplo:**

```python
from agrobr import embrapa_solos

# Todos os perfis
df = await embrapa_solos.perfis()

# Perfis do Mato Grosso
df = await embrapa_solos.perfis(uf="MT")
```

---

### `perfis_geo`

Perfis de solo com geometria (GeoDataFrame). Requer `pip install agrobr[geo]`.

```python
async def perfis_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: bool = False,
) -> gpd.GeoDataFrame | tuple[gpd.GeoDataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `uf` | `str \| None` | Filtro por UF (sigla) |
| `bbox` | `tuple \| None` | Bounding box (lon_min, lat_min, lon_max, lat_max) |
| `return_meta` | `bool` | Retorna tupla (GeoDataFrame, MetaInfo) |

**Retorno:** GeoDataFrame (Point, EPSG:4326) com mesmas colunas de `perfis()` + `geometry`

**Exemplo:**

```python
from agrobr import embrapa_solos

# Perfis com geometria em bbox
gdf = await embrapa_solos.perfis_geo(bbox=(-56, -16, -54, -14))
```

---

### `mapa_solos`

Mapa pedologico do Brasil — classificacao SiBCS (tabular).

```python
async def mapa_solos(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `ordem` | `str \| None` | Filtro por ordem de solo (contains, case-insensitive). Ex: "LATOSSOLO" |
| `bbox` | `tuple \| None` | Bounding box (lon_min, lat_min, lon_max, lat_max) |
| `as_polars` | `bool` | Retorna polars DataFrame |
| `return_meta` | `bool` | Retorna tupla (DataFrame, MetaInfo) |

**Retorno:** DataFrame com colunas: `fid`, `simbolos`, `comp1`, `comp2`, `comp3`, `legenda`, `area_km2`, `ordem1`, `subordem1`, `gdegrupo1`, `ordem2`, `subordem2`, `gdegrupo2`, `legenda_sinotica`, `classe_dom`

**Exemplo:**

```python
from agrobr import embrapa_solos

# Mapa completo
df = await embrapa_solos.mapa_solos()

# Latossolos
df = await embrapa_solos.mapa_solos(ordem="LATOSSOLO")
```

---

### `mapa_solos_geo`

Mapa pedologico com geometria (GeoDataFrame). Requer `pip install agrobr[geo]`.

```python
async def mapa_solos_geo(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    return_meta: bool = False,
) -> gpd.GeoDataFrame | tuple[gpd.GeoDataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `ordem` | `str \| None` | Filtro por ordem de solo (contains, case-insensitive) |
| `bbox` | `tuple \| None` | Bounding box (lon_min, lat_min, lon_max, lat_max) |
| `return_meta` | `bool` | Retorna tupla (GeoDataFrame, MetaInfo) |

**Retorno:** GeoDataFrame (MultiPolygon, EPSG:4326) com mesmas colunas de `mapa_solos()` + `geometry`

**Exemplo:**

```python
from agrobr import embrapa_solos

# Poligonos de solo em bbox
gdf = await embrapa_solos.mapa_solos_geo(bbox=(-56, -16, -54, -14))
```

## Versao Sincrona

```python
from agrobr.sync import embrapa_solos

df = embrapa_solos.perfis(uf="MT")
df = embrapa_solos.mapa_solos()
```

## Notas

- Fonte: [EMBRAPA GeoInfo](https://geoinfo.dados.embrapa.br) — licenca `nc` (CC BY-NC 3.0 BR)
- Funcoes `_geo()` requerem `pip install agrobr[geo]` (geopandas)
- ~34K perfis de solo (PronaSolos 2020), ~2.8K poligonos pedologicos
- Paginacao WFS automatica
- CRS de consulta BBOX: EPSG:4674 (SIRGAS 2000); GeoDataFrame retornado em EPSG:4326
