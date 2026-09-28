# INMET API

The module provides a station catalog, an authenticated observational API and public historical archives of automatic stations. Source functions do not automatically switch between API, ZIP and NASA; routing belongs to [datasets.clima](../contracts/clima.md).

## Access and return values

`estacao()` and `clima_uf()` use the observational API and require `AGROBR_INMET_TOKEN`. `estacoes()` and the three historical functions below work without a token. Configure a token only when using the observational API:

```bash
export AGROBR_INMET_TOKEN=your_token
```

All functions accept `as_polars=False` and `return_meta=False`. Pandas is the default; `as_polars=True` requires the Polars extra. With `return_meta=True`, results are `(frame, MetaInfo)`.

## `historico_periodo`

```python
async def historico_periodo(
    codigo: str,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "horario",
    as_polars: bool = False,
    return_meta: bool = False,
)
```

Queries one station across the annual ZIPs covering an inclusive interval. `codigo` identifies an automatic station such as `A001`; dates accept `date` or `YYYY-MM-DD`. The interval must be ordered, from 2000 through the current year. `agregacao` accepts `"horario"` and `"diario"`.

```python
from agrobr import inmet

df, meta = await inmet.historico_periodo(
    "A001", "2000-12-30", "2001-01-02",
    agregacao="diario", return_meta=True,
)
```

Years without a station member are recorded in `meta.source_details["coverage"]["missing_station_years"]`; results may be partial or typed empty frames. This does not establish that a station did not exist or that no observations exist outside the published archive. Transport, ZIP or layout failures abort the query instead of returning only successful years.

## `historico_uf`

```python
async def historico_uf(
    uf: str,
    ano: int,
    as_polars: bool = False,
    return_meta: bool = False,
)
```

Returns monthly state climate using annual ZIP members, including stations currently marked `Pane`. It does not filter through the current operating-station catalog. Years must be between 2000 and the current year.

```python
df, meta = await inmet.historico_uf("GO", 2001, return_meta=True)
```

Columns: `mes` (first day of month), `uf`, `precip_acum_mm`, `temp_media`, `temp_max_media`, `temp_min_media`, `num_estacoes`, `estacoes_chuva`, `estacoes_chuva_parciais`, `dias`, `data_inicio` and `data_fim`. A state with no observations returns a typed empty frame and diagnostics from the source API; in the dataset, this allows automatic fallback.

## `historico`

```python
async def historico(
    codigo: str,
    ano: int,
    agregacao: str = "horario",
    as_polars: bool = False,
    return_meta: bool = False,
)
```

The existing annual API remains available from 2000 through the current year, using the same historical observations. Current and past years may be incomplete; there is no guarantee of 8,760 hours or 365 days. If the station member is absent from the ZIP, this annual function retains `SourceUnavailableError`.

```python
df = await inmet.historico("A001", 2001, agregacao="diario")
```

## Observation schemas

Hourly results contain `data`, `hora_utc`, `estacao`, `uf` and 13 measurements: `temperatura`, `temperatura_max`, `temperatura_min`, `umidade`, `umidade_max`, `umidade_min`, `precipitacao_mm`, `pressao_hpa`, `vento_ms`, `vento_dir`, `vento_rajada_ms`, `radiacao_kj_m2`, `ponto_orvalho`. `hora_utc` uses `HH00`, from `0000` through `2300`.

Daily results contain `data`, `estacao`, `uf`, `temp_media`, `temp_max`, `temp_min`, `precipitacao_mm`, `umidade_media`, `radiacao_total_kj_m2`. All measurements allow missing values. INMET dates/hours use UTC; temperature uses °C, rainfall mm, pressure hPa, humidity %, wind m/s, direction degrees and radiation kJ/m².

## `estacoes`

```python
async def estacoes(
    tipo: str = "T",
    uf: str | None = None,
    apenas_operantes: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
)
```

Lists the current catalog: `tipo="T"` for automatic or `"M"` for conventional stations. The default keeps only `Operante`; use `apenas_operantes=False` to include other statuses. Columns are `codigo`, `nome`, `uf`, `situacao`, `tipo`, `latitude`, `longitude`, `altitude`, `inicio_operacao`. Current status does not describe status in each historical year.

## `estacao`

```python
async def estacao(
    codigo: str,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "horario",
    as_polars: bool = False,
    return_meta: bool = False,
)
```

Authenticated observations for one station, with an inclusive interval and hourly or daily aggregation. Long intervals are split into chunks; an acquisition failure in any chunk aborts the query. Authenticated HTTP 204 and naturally missing measurements are not converted to zero.

## `clima_uf`

```python
async def clima_uf(
    uf: str,
    ano: int,
    as_polars: bool = False,
    return_meta: bool = False,
)
```

Aggregates the observational API monthly for automatic stations currently operating in the state. Returns the same twelve monthly columns as `historico_uf()`. Station selection can differ between these routes.

## Aggregation and provenance

Daily rainfall sums valid hours; daily radiation preserves missing values in the same way. Monthly state rainfall is the arithmetic mean of the totals of stations with valid rainfall on every day of the month (`estacoes_chuva`). A day counts with at least 1 valid hour, and a missing hour counts as no rain, so the total of a complete station can be underestimated (in GO, January 2026, 830 hours are missing across the 18 complete stations, 6.2% of the hours). A station with an incomplete month is left out and counted in `estacoes_chuva_parciais`; with no complete station, `precip_acum_mm` is null, with a `UserWarning` and the same message in `MetaInfo.validation_warnings`. Monthly temperatures average valid daily records, which does not necessarily give equal weights to stations. `num_estacoes` counts stations with rows in the month. Groups with no valid measurements stay null; periods are never filled or extrapolated. `dias`, `data_inicio` and `data_fim` give the days of the month with valid rainfall or temperature at some station.

Historical `source_details` includes access, UTC, requested period, aggregation, methods by variable, resources with URL/SHA-256/bytes/members/fetch time/cache status and station metadata from that edition. Coverage reports observed and expected hours, valid values by variable, first/last observed day, years without members and warnings. Members without rows in the requested period remain explicit with zero hours. Automatic observations are raw; parser validation is not meteorological quality control.

The process ZIP cache is limited to 256 MiB, with a 1-hour TTL for the current year and 24 hours for earlier years. Years can be reused while retained and valid; archives larger than the limit are not cached. Hashes identify received bytes and do not freeze the edition on the portal.

## Dataset and sync access

`datasets.clima` uses API → ZIP → NASA for state queries and API → ZIP for stations. An explicit `fonte` is exclusive. The monthly contract is 3.1; daily and hourly station contracts are 1.0. The monthly `fonte` column remains `inmet` for archives, while `meta.selected_source` is `inmet_historico`.

NASA retains representative point coordinates and uses LST; INMET uses UTC and multiple stations in state aggregates. In a `deterministic` context, the dataset selects the snapshot year when omitted, without truncating observations or freezing editions. See the [full contract](../contracts/clima.md).

```python
from agrobr.sync import inmet

df = inmet.historico_periodo("A001", "2000-12-30", "2001-01-02", agregacao="diario")
```

Official sources: [annual archives](https://portal.inmet.gov.br/dadoshistoricos), [automatic-station catalog](https://portal.inmet.gov.br/paginas/catalogoaut). See [source access and limits](../sources/inmet.md).
