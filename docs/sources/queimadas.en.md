# Queimadas/INPE - BDQueimadas

## Overview

| Field | Value |
|-------|-------|
| **Institution** | INPE — Instituto Nacional de Pesquisas Espaciais |
| **Website** | [queimadas.dgi.inpe.br](https://queimadas.dgi.inpe.br) |
| **agrobr access** | Direct (public CSVs) |

## Data Origin

### Source

- **URL**: `https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/`
- **Format**: CSV (latin-1 or UTF-8), ZIP for historical data
- **Access**: Public, no authentication
- **Granularity**: Daily (`focos_diario_br_YYYYMMDD.csv`) and monthly (cascading fallback)

## Available Data

### Fire Hotspots

Satellite detection of heat spots (hot spots) over the Brazilian territory:

- Geographic coordinates (lat/lon)
- GMT date and time of detection
- Detecting satellite (13 satellites)
- Municipality and state
- Biome (6 Brazilian biomes)
- Indicators: days without rain, precipitation, fire risk, FRP

### Coverage

- **Temporal**: Since 2003 (annual data); monthly since 2023; direct CSV since 2024
- **Spatial**: Entire Brazilian territory
- **Frequency**: Daily (updated several times a day)

### Cascading fallback (monthly)

The INPE server changed the organization of historical data. The client tries in order:

| Period | Format | URL |
|---------|---------|-----|
| 2024+ | monthly `.csv` | `mensal/Brasil/focos_mensal_br_YYYYMM.csv` |
| 2023 | monthly `.zip` | `mensal/Brasil/focos_mensal_br_YYYYMM.zip` |
| 2003-2022 | annual `.zip` | `anual/Brasil_todos_sats/focos_br_todos-sats_YYYY.zip` |

For annual data, the full CSV of the year is downloaded and filtered by the requested month.

## Usage

### Monthly Hotspots

```python
import asyncio
from agrobr import queimadas

async def main():
    # All hotspots of September/2024
    df = await queimadas.focos(ano=2024, mes=9)
    print(f"{len(df)} focos detectados")

    # Filter by state
    df = await queimadas.focos(ano=2024, mes=9, uf="MT")

    # Filter by biome
    df = await queimadas.focos(ano=2024, mes=9, bioma="Cerrado")

    # With metadata
    df, meta = await queimadas.focos(ano=2024, mes=9, return_meta=True)
    print(meta.source, meta.records_count)

asyncio.run(main())
```

### Daily Hotspots

```python
# Hotspots of a specific day
df = await queimadas.focos(ano=2024, mes=9, dia=15)
```

### Combined Filters

```python
# Hotspots in Mato Grosso in the Amazon by reference satellite
df = await queimadas.focos(
    ano=2024, mes=9,
    uf="MT",
    bioma="Amazonia",
    satelite="AQUA_M-T",
)
```

## Schema

| Column | Type | Description |
|--------|------|-----------|
| `data` | date | Detection date |
| `hora_gmt` | str | GMT time (HH:MM) |
| `lat` | float | Latitude (-35 to 6) |
| `lon` | float | Longitude (-74 to -30) |
| `satelite` | str | Satellite name |
| `municipio` | str | Municipality name |
| `municipio_id` | Int64 | IBGE code |
| `estado` | str | State name |
| `uf` | str | State abbreviation (2 characters) |
| `bioma` | str | Brazilian biome |
| `numero_dias_sem_chuva` | float | Days without precipitation |
| `precipitacao` | float | Precipitation (mm) |
| `risco_fogo` | float | Risk index (0-1) |
| `frp` | float | Fire Radiative Power (MW) |

In some months the source publishes hotspots outside the contract: an equal copy comes out once; a repeated key that differs
only in FRP comes out as 1 row with a null `frp`; one that differs in another column is dropped from the result; and a negative
FRP comes out null. Each case comes with a warning and the count. The 7 months of 2023–2025 where this happened and the rules
are in the [contract](../contracts/queimadas.md#negative-frp-and-repeated-hotspot).

## Satellites

INPE monitors fire hotspots with 13 satellites. The reference satellite is
AQUA_M-T (MODIS), used in the official statistics for having the longest and
most consistent time series.

Without `satelite=`, `focos()` returns the hotspots of every satellite, and the count adds up each one's detections: in August
2025 there were 594,309 hotspots in total and 18,451 from AQUA_M-T. INPE's per-state statistics use only the reference
satellite; to compare with them, pass `satelite="AQUA_M-T"`.

## Cache

There is no local cache: every call downloads the data from INPE.

## Updating

| Aspect | Value |
|---------|-------|
| **Frequency** | Daily |
| **Reference satellite** | AQUA_M-T, passing around 13:30 and 01:30 nominal local time; `hora_gmt` is in GMT |

The monthly file of the current month and the daily file of the current day are partial and change during the period:
`focos()` warns and says in `source_details` up to which hotspot the file goes and its `Last-Modified`. See the
[contract](../contracts/queimadas.md#partial-current-month).

## Historical files

Legacy CSVs with `latitude`, `longitude`, and `data_pas` are normalized to the current output schema. When no monthly file exists, `focos()` falls back to the annual ZIP. For 2020, the download is around 81 MB and the uncompressed CSV is 584 MB; account for the memory needed to process the full year as well.
