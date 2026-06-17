# NASA POWER API

The NASA POWER module provides global gridded climate data from NASA — temperature, precipitation, radiation, humidity and wind. An alternative to INMET that requires no token.

## Functions

### `clima_ponto`

Climate data for a geographic point (latitude/longitude).

```python
async def clima_ponto(
    lat: float,
    lon: float,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "diario",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `lat` | `float` | Latitude (-90 to 90) |
| `lon` | `float` | Longitude (-180 to 180) |
| `inicio` | `str \| date` | Start date (YYYY-MM-DD) |
| `fim` | `str \| date` | End date (YYYY-MM-DD) |
| `agregacao` | `str` | `"diario"` (default) or `"mensal"` |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns (daily): `data`, `lat`, `lon`, `temp_media`, `temp_max`, `temp_min`, `precip_mm`, `umidade_rel`, `radiacao_mj`, `vento_ms`

With `agregacao="mensal"`, the aggregated columns are renamed: `mes` (timestamp), `precip_acum_mm`, `temp_media`, `temp_max_media`, `temp_min_media`, `umidade_media`, `radiacao_media_mj`, `vento_medio_ms` (plus `lat`/`lon`).

**Example:**

```python
from agrobr import nasa_power

# Daily climate for Sorriso-MT
df = await nasa_power.clima_ponto(
    lat=-12.55, lon=-55.72,
    inicio="2024-01-01", fim="2024-03-31"
)

# Monthly climate
df = await nasa_power.clima_ponto(
    lat=-12.55, lon=-55.72,
    inicio="2023-01-01", fim="2023-12-31",
    agregacao="mensal"
)
```

---

### `clima_uf`

Climate data aggregated by state (uses the state centroid).

```python
async def clima_uf(
    uf: str,
    ano: int,
    agregacao: str = "mensal",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `uf` | `str` | State code (e.g. "MT", "SP") |
| `ano` | `int` | Reference year |
| `agregacao` | `str` | `"diario"` or `"mensal"` (default) |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Example:**

```python
from agrobr import nasa_power

df = await nasa_power.clima_uf("MT", 2024)
```

## Synchronous Version

```python
from agrobr.sync import nasa_power

df = nasa_power.clima_ponto(lat=-12.55, lon=-55.72, inicio="2024-01-01", fim="2024-03-31")
df = nasa_power.clima_uf("MT", 2024)
```

## Notes

- Data from [NASA POWER](https://power.larc.nasa.gov/) — `livre` license
- Uses centroid coordinates for `clima_uf()` — for precise analyses, use `clima_ponto()` with specific coordinates
- Alternative to INMET for those without a token
