# NASA POWER - Global Climate Data

## Overview

| Field | Value |
|-------|-------|
| **Institution** | NASA / LaRC |
| **Website** | [power.larc.nasa.gov](https://power.larc.nasa.gov) |
| **agrobr access** | REST API (JSON), no authentication |
| **Replaces** | INMET (API down since Jan/2026) |

## Data Origin

### Source

- **URL**: `https://power.larc.nasa.gov/api/temporal/daily/point`
- **Format**: JSON
- **Access**: Public, no authentication restrictions
- **Coverage**: Global, point queries, since 1981
- **Community**: AG (Agroclimatology)

## Available Parameters

| NASA parameter | agrobr name | Unit | Description |
|----------------|-------------|---------|-----------|
| `T2M` | `temp_media` | C | Mean temperature at 2m |
| `T2M_MAX` | `temp_max` | C | Maximum temperature at 2m |
| `T2M_MIN` | `temp_min` | C | Minimum temperature at 2m |
| `PRECTOTCORR` | `precip_mm` | mm/day | Corrected precipitation |
| `RH2M` | `umidade_rel` | % | Relative humidity at 2m |
| `ALLSKY_SFC_SW_DWN` | `radiacao_mj` | MJ/m2/day | Incident solar radiation |
| `WS2M` | `vento_ms` | m/s | Wind speed at 2m |

## Usage

Long queries are split into chunks. If a chunk fails after retries, the entire call raises `SourceUnavailableError`; earlier chunks are not returned as a complete series. Both `clima_ponto` and `clima_uf` accept only `agregacao="diario"` or `"mensal"`, validated before network access. This does not guarantee observations for every variable and day: source-reported missing measurements remain null.

### Point data (lat/lon)

```python
import asyncio
from agrobr import nasa_power

async def main():
    # Daily data for Sorriso-MT
    df = await nasa_power.clima_ponto(
        lat=-12.6, lon=-56.1,
        inicio="2024-01-01", fim="2024-01-31"
    )
    print(df)

    # Monthly aggregation
    df = await nasa_power.clima_ponto(
        lat=-12.6, lon=-56.1,
        inicio="2024-01-01", fim="2024-12-31",
        agregacao="mensal"
    )

    # With metadata
    df, meta = await nasa_power.clima_ponto(
        lat=-12.6, lon=-56.1,
        inicio="2024-01-01", fim="2024-01-31",
        return_meta=True
    )

asyncio.run(main())
```

### Data by state

Uses a fixed representative point per state, chosen by agrobr (`UF_COORDS`). It is not the state's official centroid.

```python
# Monthly climate for MT in 2024
df = await nasa_power.clima_uf("MT", ano=2024)

# Daily
df = await nasa_power.clima_uf("MT", ano=2024, agregacao="diario")

# With metadata
df, meta = await nasa_power.clima_uf("MT", ano=2024, return_meta=True)
```

## Schema - Daily

| Column | Type | Description |
|--------|------|-----------|
| `data` | datetime | Observation date |
| `lat` | float | Point latitude |
| `lon` | float | Point longitude |
| `uf` | str | State abbreviation (when using clima_uf) |
| `temp_media` | float | Mean temperature (C) |
| `temp_max` | float | Maximum temperature (C) |
| `temp_min` | float | Minimum temperature (C) |
| `precip_mm` | float | Precipitation (mm/day) |
| `umidade_rel` | float | Relative humidity (%) |
| `radiacao_mj` | float | Solar radiation (MJ/m2/day) |
| `vento_ms` | float | Wind speed (m/s) |

## Schema - Monthly

| Column | Type | Description |
|--------|------|-----------|
| `mes` | datetime | First day of the month |
| `uf` | str | State abbreviation |
| `precip_acum_mm` | float | Accumulated precipitation (mm) |
| `temp_media` | float | Mean temperature (C) |
| `temp_max_media` | float | Mean of the maximums (C) |
| `temp_min_media` | float | Mean of the minimums (C) |
| `umidade_media` | float | Mean relative humidity (%) |
| `radiacao_media_mj` | float | Mean radiation (MJ/m2/day) |
| `vento_medio_ms` | float | Mean wind (m/s) |
| `dias` | int | Days of the month with any valid parameter |
| `data_inicio` | datetime | First of those days |
| `data_fim` | datetime | Last of those days |
| `lat` | float | Point latitude |
| `lon` | float | Point longitude |

## Available States

All 27 Brazilian states have a fixed representative point configured.
For precise analyses, use `clima_ponto()` with exact coordinates.

## Note on Spatial Resolution

NASA POWER combines products with their own spatial characteristics; a point query does not establish one resolution for all variables. For large states
like MT or PA, the central point may not represent the whole climatic
variability of the state well. For detailed regional analyses, query multiple
points with `clima_ponto()`.

## Cache

There is no local cache: every call downloads the data from NASA POWER.

## Updating

| Aspect | Value |
|---------|-------|
| **Frequency** | Data with ~2 days lag |
| **History** | Since 1981 |
| **Resolution** | Daily |

## Aggregation and missing measurements

Rainfall is accumulated over time per station. INMET computes each state's monthly value as the arithmetic mean of the totals of stations with valid rainfall on every day of the month, not the sum across stations; `estacoes_chuva` and `estacoes_chuva_parciais` count the stations included and left out. `num_estacoes` counts stations present. NASA POWER uses a fixed representative state point. Its coordinates are retained by the dataset; this is not a territorial mean or a verified centroid, and does not establish one shared spatial cell for all variables. The default NASA day uses [LST](https://power.larc.nasa.gov/docs/services/api/temporal/daily/#time-standards), while INMET uses UTC; the dataset records this distinction in `base_tempo`.

NASA POWER data is the MERRA-2 reanalysis at a grid point, not a station, up to the previous month; in the current month it is the low-latency GEOS-IT, which NASA later replaces with MERRA-2, and recent radiation comes from FLASHFlux, which SYN1deg replaces ([NASA POWER sources](https://power.larc.nasa.gov/docs/methodology/data/sources/); NASA recommends ending trend analysis 2 months earlier). `source_details` carries the sources each query header declares (`fontes` and `fontes_por_bloco`) and the low-latency stretch (`periodos_baixa_latencia`, with its `origem` and whether it is `exato`), and `validation_warnings` carries one warning per source. The header only gives the sources of the window: in a block with both, the GEOS-IT start comes from NASA's rule (MERRA-2 closes by month) when the block has a single day 1; with more than one, the warning covers the block and says the header does not split by day. Compared with INMET in 2025 (check of 2026-09-26), annual rainfall came out 41% lower in DF and 27% lower in MT, and monthly mean temperature up to 2.9 °C higher. In `datasets.clima` with `fonte=None`, a route change changes the nature of the data: see the [`clima` contract](../contracts/clima.en.md).

Groups with no measurements remain null: missing data does not mean 0 mm. Totals use available measurements only, without filling or extrapolating missing hours/days; `dias`, `data_inicio` and `data_fim` give each month's coverage; check them against the calendar before comparing totals. INMET daily radiation also preserves entirely missing groups. The monthly `clima` 3.1 contract permits null precipitation and temperatures; daily `clima_estacao` and hourly `clima_estacao_horaria` are both 1.0. Monthly `lat`/`lon` retain the NASA point, and `agregacao_espacial` distinguishes `ponto_grade` from INMET `estacoes`.
