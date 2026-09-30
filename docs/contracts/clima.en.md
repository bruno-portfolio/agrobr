# clima

Monthly climate by state or daily and hourly station observations. State queries accept years from 1981 through the current year (by the Brasília date); public INMET archives start in 2000 and may be incomplete.

## Selection and routing

| `fonte` | State | Station |
|---|---|---|
| `None` | INMET API → INMET ZIP → NASA POWER | INMET API → INMET ZIP |
| `"inmet"` | Observational API only | Observational API only |
| `"inmet_historico"` | Public ZIP only | Public ZIP only |
| `"nasa_power"` | Representative state point | Invalid |

An explicit source never activates another source. Automatic state routing skips the INMET API and archives for years before 2000, which go straight to NASA; `fonte="inmet"` or `"inmet_historico"` with those years raises `InvalidParameterError`. A state with no observations in the ZIP can fall back to NASA; rows with null measurements remain valid.

```python
from agrobr import datasets

monthly, meta = await datasets.clima("GO", 2001, return_meta=True)
daily = await datasets.clima(
    estacao="A001", inicio="2000-12-30", fim="2001-01-02",
    fonte="inmet_historico",
)
hourly = await datasets.clima(
    estacao="A001", inicio="2001-01-01", fim="2001-01-02",
    agregacao="horario", fonte="inmet_historico", as_polars=True,
)
```

`uf` and `ano` retain their positional arguments; other arguments are keyword-only. `return_meta=True` returns `(frame, MetaInfo)`. `as_polars=True` converts after contract validation and requires the Polars extra.

In state mode, an omitted `agregacao` or `"mensal"` returns months; `"diario"` and `"horario"` raise `InvalidParameterError` (up to 1.1.0 the default was `"diario"`, which state mode accepted while returning months). Station mode defaults to daily data, requires inclusive `inicio` and `fim`, and accepts only `"diario"` and `"horario"`. Do not combine a station with state/year selectors.

## Monthly contract `CLIMA_V3` — 3.1

Registry key: `clima`. PK: `[mes, uf]`. Version 2.0 remains available as a historical contract. Version 3.0 allows null temperatures in addition to nullable precipitation. Version 3.1 adds the INMET rainfall station counts and the month's daily coverage, in the last five columns.

| Column | Type | Nullable | Unit or meaning |
|---|---|---|---|
| `mes` | DATE | No | First day of month |
| `uf` | STRING | No | Requested state |
| `precip_acum_mm` | FLOAT | Yes | mm |
| `temp_media` | FLOAT | Yes | °C |
| `temp_max_media` | FLOAT | Yes | °C |
| `temp_min_media` | FLOAT | Yes | °C |
| `num_estacoes` | INTEGER | Yes | Stations with rows in month; null for NASA |
| `umidade_media` | FLOAT | Yes | %, available in NASA aggregates |
| `radiacao_media_mj` | FLOAT | Yes | MJ/m², NASA daily mean |
| `vento_medio_ms` | FLOAT | Yes | m/s, available in NASA aggregates |
| `fonte` | STRING | No | `inmet` or `nasa_power` |
| `lat` | FLOAT | Yes | NASA point latitude; null for INMET aggregates |
| `lon` | FLOAT | Yes | NASA point longitude; null for INMET aggregates |
| `agregacao_espacial` | STRING | Yes | `estacoes` or `ponto_grade` |
| `base_tempo` | STRING | Yes | INMET `UTC`; NASA `LST` |
| `estacoes_chuva` | INTEGER | Yes | INMET stations with valid rainfall on every day of the month, the only ones in the mean; null for NASA |
| `estacoes_chuva_parciais` | INTEGER | Yes | INMET stations with valid rainfall on only some days, left out of the mean; null for NASA |
| `dias` | INTEGER | Yes | Days of the month with at least one valid daily value |
| `data_inicio` | DATE | Yes | First of those days |
| `data_fim` | DATE | Yes | Last of those days |

The columns from `lat` on are optional in the contract and populated by the dataset. The PK excludes source: concatenating results for the same state/month requires retaining query identity outside this contract.

## Daily contract `CLIMA_ESTACAO_V1` — 1.0

Registry key: `clima_estacao`. PK: `[data, estacao]`.

| Column | Type | Nullable |
|---|---|---|
| `data` | DATE | No |
| `estacao` | STRING | No |
| `uf` | STRING | Yes |
| `temp_media` | FLOAT | Yes |
| `temp_max` | FLOAT | Yes |
| `temp_min` | FLOAT | Yes |
| `precipitacao_mm` | FLOAT | Yes |
| `umidade_media` | FLOAT | Yes |
| `radiacao_total_kj_m2` | FLOAT | Yes |

Temperature uses °C, precipitation mm, humidity % and radiation kJ/m². Dates identify UTC days.

## Hourly contract `CLIMA_ESTACAO_HORARIA_V1` — 1.0

Registry key: `clima_estacao_horaria`. PK: `[data, hora_utc, estacao]`. `data` and `estacao` are required; `hora_utc` is a string from `0000` through `2300` at hourly intervals. `uf` is nullable.

The 13 measurements are nullable FLOATs: `temperatura`, `temperatura_max`, `temperatura_min`, `ponto_orvalho` (°C); `umidade`, `umidade_max`, `umidade_min` (%); `precipitacao_mm` (mm); `pressao_hpa` (hPa); `vento_ms`, `vento_rajada_ms` (m/s); `vento_dir` (degrees); `radiacao_kj_m2` (kJ/m²).

## Aggregation, space and missing data

INMET daily rainfall and radiation sum valid hours. Monthly state rainfall is the arithmetic mean of the totals of stations with valid rainfall on every day of the month (`estacoes_chuva`). A day counts with at least 1 valid hour, and a missing hour counts as no rain, so the total of a complete station can be underestimated (in GO, January 2026, 830 hours are missing across the 18 complete stations, 6.2% of the hours). A station with an incomplete month is left out and counted in `estacoes_chuva_parciais`; with no complete station, `precip_acum_mm` is null, with a `UserWarning` and the same message in `MetaInfo.validation_warnings`. Monthly temperatures average valid daily records; stations with more valid days may carry more weight. `num_estacoes` counts stations with rows, including rows without valid measurements.

Entirely missing groups remain null. Missing hours and days are not filled, extrapolated or converted to zero. `dias`, `data_inicio` and `data_fim` give each month's daily coverage: for INMET, the days with valid rainfall or temperature at some station; for NASA, the days with any valid parameter. A partial month, such as the current one, comes with `dias` below the number of days in the month and is not extrapolated. For GO in December 2001, A003 has rainfall on all 31 days (270.8 mm) and A002 on 28 (170.4 mm): the monthly value is 270.8 mm, with `estacoes_chuva=1` and `estacoes_chuva_parciais=1`.

NASA uses a configured representative state point, retained in `lat`/`lon`; it is not a territorial mean, a verified centroid or a guarantee that variables share one spatial grid cell. Its default day uses [LST](https://power.larc.nasa.gov/docs/services/api/temporal/daily/#time-standards), while INMET uses UTC. The two routes have different spatial and temporal interpretations.

NASA POWER is not station observation: it is the MERRA-2 reanalysis at a grid point up to the previous month and, in the current month, the low-latency GEOS-IT, which NASA later replaces with MERRA-2 ([NASA POWER sources](https://power.larc.nasa.gov/docs/methodology/data/sources/)). The GEOS-IT stretch comes in `source_details["periodos_baixa_latencia"]` and in a warning in `validation_warnings` (see the [source](../sources/nasa_power.md)). The difference from INMET stations can be large. In 2025, through both explicit routes: in DF, 1,226.6 mm at INMET (5 stations) × 728.4 mm at NASA (−41%), with monthly mean temperature 1.2 to 2.5 °C higher; in MT, 1,383.4 × 1,005.2 mm (−27%), with temperature up to 2.9 °C higher. On the automatic route, years before 2000, or with no state observations in the ZIP, come from NASA: a series built with `fonte=None` may show a step that is not climate. The `fonte` column and `MetaInfo.selected_source` state the route of each result.

## Provenance and coverage

`MetaInfo.selected_source` distinguishes `inmet`, `inmet_historico` and `nasa_power`; `attempted_sources` lists only attempted routes. The monthly `fonte` column remains `inmet` for both INMET routes.

`source_details` preserves access, time basis, aggregation, requested period and methods by variable. ZIP results include resources with URL/SHA-256/size/members/fetch time/cache status, station metadata from each edition and station coverage: first/last observed day, observed hours, calendar hours and valid counts by measurement. Selected members with no rows in the period have zero observed hours and null observation dates. Calendar and measurement completeness indicators, years without members, identical duplicates removed and warnings expose limitations; they do not certify scientific quality.

The API aggregates stations currently marked `Operante`; ZIP routing selects annual members, including stations currently marked `Pane`. See the [source documentation](../sources/inmet.md) for caching and failures.

In a `deterministic` context, the snapshot selects the year when omitted. It does not truncate observations at the snapshot date or freeze the published edition. `source_details.deterministic` states these limits; a hash identifies received bytes, not an archive retrievable as of a past date.

Each `source_details.stations` item includes `layout_fingerprint`: a SHA-256 signature of normalized metadata keys and column headers, with signature-format and parser versions. Measurement and station values are excluded from this signature. It supports layout comparisons; the resource hash identifies the complete bytes.

## Access

INMET observational API requires a token; [official annual ZIPs](https://portal.inmet.gov.br/dadoshistoricos) and NASA are public. The repository classification is `livre`; see [licenses](../licenses.md) for its scope.
