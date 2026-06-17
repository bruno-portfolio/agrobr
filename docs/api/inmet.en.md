# INMET API

The INMET module provides weather data from 600+ stations of the National Institute of Meteorology.

## Token

Observational data via apitempo requires a token:

```bash
export AGROBR_INMET_TOKEN=your_token
```

Listing stations works without a token — and `historico()` (below) covers closed years with no token at all, via the portal's dadoshistoricos.

## Functions

### `historico`

Hourly data for a full year of a station, **without a token**, via the portal's public yearly ZIP (`portal.inmet.gov.br/uploads/dadoshistoricos`).

```python
async def historico(
    codigo: str,
    ano: int,
    agregacao: str = "horario",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `codigo` | `str` | Station code (e.g. `"A701"`) |
| `ano` | `int` | Year (2000+) |
| `agregacao` | `str` | `"horario"` (default) or `"diario"` |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

Same schema as `estacao()`: `data`, `hora_utc`, `estacao`, `uf`, `temperatura`,
`temperatura_max/min`, `umidade(_max/_min)`, `precipitacao_mm`, `pressao_hpa`,
`vento_ms/dir/rajada_ms`, `radiacao_kj_m2`, `ponto_orvalho`.

**Example:**

```python
from agrobr import inmet

df = await inmet.historico("A701", 2025)                       # 8,760 hours
df = await inmet.historico("A001", 2025, agregacao="diario")   # 365 days
```

> The yearly ZIP is ~100 MB (all stations) and is process-cached —
> the second station of the same year re-downloads nothing.

### `estacoes`

Lists available weather stations.

```python
async def estacoes(
    tipo: str = "T",
    uf: str | None = None,
    apenas_operantes: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `tipo` | `str` | `"T"` for automatic, `"M"` for conventional |
| `uf` | `str \| None` | Filter by state |
| `apenas_operantes` | `bool` | If True, returns only active stations |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `codigo`, `nome`, `uf`, `situacao`, `tipo`, `latitude`, `longitude`, `altitude`, `inicio_operacao`

---

### `estacao`

Observational data for a specific station.

```python
async def estacao(
    codigo: str,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "horario",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `codigo` | `str` | Station code (e.g. `"A001"`) |
| `inicio` | `str \| date` | Start date (YYYY-MM-DD) |
| `fim` | `str \| date` | End date (YYYY-MM-DD) |
| `agregacao` | `str` | `"horario"` (default) or `"diario"` |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with weather observations (temperature, precipitation, humidity, wind, radiation, pressure).

---

### `clima_uf`

Climate aggregated by state from all of the state's stations.

```python
async def clima_uf(
    uf: str,
    ano: int,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `uf` | `str` | State code (e.g. `"MT"`, `"SP"`) |
| `ano` | `int` | Reference year |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `mes`, `uf`, `precip_acum_mm`, `temp_media`, `temp_max_media`, `temp_min_media`, `num_estacoes`

**Example:**

```python
from agrobr import inmet

# List MT stations
est = await inmet.estacoes(uf="MT")

# Single-station data
df = await inmet.estacao("A001", inicio="2024-01-01", fim="2024-01-31")

# Monthly climate by state
df = await inmet.clima_uf("MT", 2024)
```

## Synchronous Version

```python
from agrobr.sync import inmet

est = inmet.estacoes(uf="MT")
df = inmet.clima_uf("MT", 2024)
```

## Notes

- Source: [INMET](https://portal.inmet.gov.br) — free license
- 600+ automatic and conventional stations
- For data without a token, use [NASA POWER](nasa_power.md) as an alternative
```
