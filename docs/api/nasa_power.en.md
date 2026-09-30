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
    *,
    as_polars: bool = False,
    return_meta: bool = False,
    parameters: list[str] | None = None,
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
| `parameters` | `list[str] \| None` | NASA POWER codes to request (1 to 20, no repeats), from the `parametros()` catalog; `None` requests all |

**Returns:**

DataFrame with columns (daily): `data`, `lat`, `lon`, `temp_media`, `temp_max`, `temp_min`, `precip_mm`, `umidade_rel`, `radiacao_mj`, `vento_ms`

With `agregacao="mensal"`, the aggregated columns are renamed: `mes` (timestamp), `precip_acum_mm`, `temp_media`, `temp_max_media`, `temp_min_media`, `umidade_media`, `radiacao_media_mj`, `vento_medio_ms` (plus `lat`/`lon`). `dias`, `data_inicio` and `data_fim` give the days of the month with any valid parameter. A month cut by the requested period is partial and not extrapolated: from 2025-01-15 to 2025-02-05, February comes with `dias=5` and 17.81 mm, against 28 days and 52.33 mm for the whole month (schema 1.2).

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

Climate data for a fixed representative point configured for the state.

```python
async def clima_uf(
    uf: str,
    ano: int,
    agregacao: str = "mensal",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
    parameters: list[str] | None = None,
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
| `parameters` | `list[str] \| None` | NASA POWER codes to request (1 to 20, no repeats), from the `parametros()` catalog; `None` requests all |

**Example:**

```python
from agrobr import nasa_power

df = await nasa_power.clima_uf("MT", 2024)
```

`as_polars`, `return_meta` and `parameters` are keyword-only.

---

### `parametros`

Catalog of the AG community daily codes that agrobr reads, with no network access.

```python
def parametros() -> pd.DataFrame
```

Columns: `codigo` (the NASA POWER parameter name, the value accepted in `parameters`), `coluna` and `unidade` (daily
output), `coluna_mensal`, `unidade_mensal` and `agregacao_mensal` (monthly output), `comunidade` and `frequencia_origem`.

```python
from agrobr import nasa_power

nasa_power.parametros()[["codigo", "coluna", "unidade"]]
df = await nasa_power.clima_ponto(-12.55, -55.72, "2024-01-01", "2024-01-31", parameters=["T2M", "PRECTOTCORR"])
```

## Synchronous Version

```python
from agrobr.sync import nasa_power

df = nasa_power.clima_ponto(lat=-12.55, lon=-55.72, inicio="2024-01-01", fim="2024-03-31")
df = nasa_power.clima_uf("MT", 2024)
```

## Notes

- Data from [NASA POWER](https://power.larc.nasa.gov/) — `livre` license
- Uses fixed representative coordinates for `clima_uf()` — for precise analyses, use `clima_ponto()` with specific coordinates
- Alternative to INMET for those without a token

## Aggregation and missing measurements

Rainfall is accumulated over time per station. INMET computes each state's monthly value as the arithmetic mean of the totals of stations with valid rainfall on every day of the month, not the sum across stations; `estacoes_chuva` and `estacoes_chuva_parciais` count the stations included and left out. `num_estacoes` counts stations present. NASA POWER uses a fixed representative state point. Its coordinates are retained by the dataset; this is not a territorial mean or a verified centroid, and does not establish one shared spatial cell for all variables. The default NASA day uses [LST](https://power.larc.nasa.gov/docs/services/api/temporal/daily/#time-standards), while INMET uses UTC; the dataset records this distinction in `base_tempo`.

Groups with no measurements remain null: missing data does not mean 0 mm. Totals use available measurements only, without filling or extrapolating missing hours/days; `dias`, `data_inicio` and `data_fim` give each month's coverage; check them against the calendar before comparing totals. INMET daily radiation also preserves entirely missing groups. The monthly `clima` 3.1 contract permits null precipitation and temperatures; daily `clima_estacao` and hourly `clima_estacao_horaria` are both 1.0. Monthly `lat`/`lon` retain the NASA point, and `agregacao_espacial` distinguishes `ponto_grade` from INMET `estacoes`.
