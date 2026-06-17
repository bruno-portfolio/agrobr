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
- **Coverage**: Global, 0.5 degree grid, since 1981
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

Uses the state's central coordinates as a representative point.

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
| `lat` | float | Point latitude |
| `lon` | float | Point longitude |

## Available States

All 27 Brazilian states have central coordinates mapped.
For precise analyses, use `clima_ponto()` with exact coordinates.

## Note on Spatial Resolution

NASA POWER provides data on a 0.5 degree grid (~55km). For large states
like MT or PA, the central point may not represent the whole climatic
variability of the state well. For detailed regional analyses, query multiple
points with `clima_ponto()`.

## Cache

| Aspect | Value |
|---------|-------|
| **TTL** | 24 hours |
| **Maximum stale** | 30 days |
| **Policy** | Fixed TTL |

## Updating

| Aspect | Value |
|---------|-------|
| **Frequency** | Data with ~2 days lag |
| **History** | Since 1981 |
| **Resolution** | Daily |
