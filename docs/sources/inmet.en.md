# INMET — Meteorology

Brazil's National Institute of Meteorology publishes observations, station catalogs and historical archives. agrobr provides two distinct routes: the observational API and annual ZIPs of automatic stations. ZIPs do not represent the entire archive or all [BDMEP](https://bdmep.inmet.gov.br/) products.

## Access

| Service | Authentication | Functions |
|---|---|---|
| Current station catalog | Public | `estacoes` |
| Observational API | `AGROBR_INMET_TOKEN` | `estacao`, `clima_uf` |
| Annual automatic-station ZIPs | Public, no token | `historico`, `historico_periodo`, `historico_uf` |

```bash
export AGROBR_INMET_TOKEN=your_token
```

The variable is required only for observational access. A missing or rejected token raises `SourceUnavailableError`; secrets must not appear in published URLs, logs or reports. The INMET API requires the token in the URL path (`/token/<route>/<token>`): agrobr masks it in the httpx and httpcore logs, the response, the redirect history and the error, but a proxy, firewall or access log outside the process sees the full URL. The public [estacoes/T catalog](https://apitempo.inmet.gov.br/estacoes/T) does not require this token.

The [official historical catalog](https://portal.inmet.gov.br/dadoshistoricos) offers years from 2000 through the current year. On September 6, 2026 there were 27 ZIPs, with 2026 labeled through August 31, 2026. This does not guarantee full-year coverage, daily ZIP updates or immutable past years. The [official access guidance](https://portal.inmet.gov.br/noticias/saiba-como-acessar-os-dados-meteorol%25C3%25B3gicos-dispon%25C3%25ADveis-no-site-do-inmet) describes access, UTC timestamps and raw automatic observations without meteorological quality control.

## Public queries

```python
from agrobr import inmet, datasets

hours = await inmet.historico("A001", 2001)
days, meta = await inmet.historico_periodo(
    "A001", "2000-12-30", "2001-01-02",
    agregacao="diario", return_meta=True,
)
monthly = await inmet.historico_uf("GO", 2001)
monthly, meta = await datasets.clima("GO", 2001, fonte="inmet_historico", return_meta=True)
```

Annual and interval historical functions accept years from 2000 through the current year. Source station functions default to hourly data; `agregacao="diario"` returns days. `historico_uf` returns months, with `mes` as the first-day date rather than an integer. All accept `as_polars` and `return_meta`. See [signatures and columns](../api/inmet.md).

## Spatial selection and formulas

`clima_uf` queries automatic stations currently marked `Operante` in the API. `historico_uf` uses annual archive members and metadata, including stations currently marked `Pane`. For example, A003/Morrinhos has observations in the [2001 ZIP](https://portal.inmet.gov.br/uploads/dadoshistoricos/2001.zip) despite this current catalog status. Current status and absent members do not establish activation or closure dates.

Daily rainfall and radiation sum valid hourly measurements. Monthly state rainfall is the arithmetic mean of the totals of stations with valid rainfall on every day of the month; stations with an incomplete month are left out (`estacoes_chuva_parciais`) and, with no complete station, the value is null, with a warning. For GO in December 2001, A003 has rainfall on all 31 days (270.8 mm) and A002 on 28 (170.4 mm): the monthly value is 270.8 mm. A day counts with at least 1 valid hour, and a missing hour counts as no rain, so the total of a complete station can be underestimated (in GO, January 2026, 830 hours are missing across the 18 complete stations, 6.2% of the hours). Monthly temperatures average valid daily records without guaranteeing equal station weights. `num_estacoes` counts stations with rows in a month, not sensors with complete coverage; `dias`, `data_inicio` and `data_fim` give the days of the month with valid rainfall or temperature.

Missing hours/days are never imputed or extrapolated. Entirely missing measurement groups stay null: A001 on May 20, 2000 has 24 rows without valid measurements in the [2000 ZIP](https://portal.inmet.gov.br/uploads/dadoshistoricos/2000.zip), and rainfall, temperature and radiation remain null. Layout and contract validation do not turn raw observations into a meteorologically quality-controlled series.

## Acquisition, caching and failures

Historical queries download the required annual ZIPs and select relevant CSVs. Sizes vary by year; modern archives can contain tens of MB. The in-process memory cache has a total limit of 256 MiB, with a 1-hour TTL for the current year and 24 hours for earlier years, evicting least recently used entries. Archives larger than the limit are not retained. Concurrent queries for a year in the same event loop share acquisition; this implementation does not provide persistent revision caching or conditional ETag requests.

HTTP/transport failures, invalid ZIPs, CRC or layout errors abort the historical query; successful years are not presented as a complete acquisition after an error. Identical repeated observations are counted once; conflicting station/date/hour records cause an error.

`historico_periodo` diagnoses years without members and may return partial observations or a typed empty frame. Annual `historico` retains an error when its station member is absent. `historico_uf` may return a typed empty frame if the archive has no state observations. None of these results establishes that other INMET services lack data.

The observational API splits long intervals into chunks of up to 365 days; acquisition failures abort the query. Authenticated HTTP 204 and naturally missing values represent missing observations without inventing zero rainfall.

## Provenance and editions

With `return_meta=True`, `source_details` records access, UTC, requested period, aggregation and spatial methods by variable. ZIP access also preserves each resource's URL, SHA-256, bytes, members, fetch time and cache status; station metadata belongs to the queried edition. Current catalog altitude and coordinates do not silently replace historical metadata.

`stations[].layout_fingerprint` identifies each CSV layout using a SHA-256 of normalized metadata keys and column headers, excluding values. It records signature and parser versions and is distinct from the complete ZIP hash.

`coverage` reports observed hours, requested calendar, valid counts by measurement, first/last observed day and years without members. Selected stations without rows in the interval remain explicit with zero hours. `complete_calendar` and `complete_measurements` distinguish row presence from measurement availability; warnings and identical-duplicate counts supplement these diagnostics. These fields do not certify scientific quality or coverage of the entire archive.

## Dataset layer

Without `fonte`, `datasets.clima` tries API → ZIP → NASA for states and API → ZIP for stations. An explicit source is exclusive; NASA cannot serve station mode. A state without ZIP observations can trigger automatic fallback, while null measurements in existing rows remain valid. Archives before 2000 are not an eligible route.

The monthly `clima` contract is 3.1; daily `clima_estacao` and hourly `clima_estacao_horaria` are 1.0. The monthly `fonte` column stays `inmet`; `selected_source="inmet_historico"` identifies ZIP access. For NASA, `lat`/`lon` preserve a fixed representative state point without asserting a centroid or one common cell across variables. `agregacao_espacial` distinguishes `estacoes` and `ponto_grade`; `base_tempo` distinguishes INMET UTC from [NASA LST](https://power.larc.nasa.gov/docs/services/api/temporal/daily/#time-standards).

A dataset snapshot selects the year when omitted but neither truncates results at that date nor freezes source editions. A hash identifies received bytes; it does not provide as-of retrieval. See [contracts and examples](../contracts/clima.md).

## License and limits

The repository classifies INMET as `livre` in [licenses](../licenses.md). Official evidence of public, free access was not treated as an additional, specific redistribution license.

Catalog information above is dated, not permanent counts.
