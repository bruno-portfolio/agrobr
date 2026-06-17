# INMET — Meteorology

Instituto Nacional de Meteorologia. Climate data from 600+ stations.

## Authentication

The INMET observational data API requires an authentication token.
Set the `AGROBR_INMET_TOKEN` environment variable before using it:

```bash
export AGROBR_INMET_TOKEN="seu-token-aqui"
```

Without the token, observational data requests raise `SourceUnavailableError`
with instructions (the API returns empty HTTP 204 when unauthenticated). An invalid token
raises `SourceUnavailableError` ("Token INMET inválido").
The station listing (`/estacoes/T`, `/estacoes/M`) works without a token.

To obtain the token, consult the INMET portal (https://portal.inmet.gov.br) — there is no
self-service flow; formal alternative: a request via Fala.BR/LAI (falabr.cgu.gov.br).

**Without a token**: `inmet.historico(codigo, ano)` covers closed years (2000+) via the
public annual ZIPs from dadoshistoricos, with the same schema as `estacao()` —
no authentication required.

## API

```python
from agrobr import inmet

# List automatic stations
df = await inmet.estacoes(tipo="T", uf="MT")

# Hourly data for a station
df = await inmet.estacao("A001", inicio="2024-01-01", fim="2024-01-31")

# Daily data
df = await inmet.estacao("A001", inicio="2024-01-01", fim="2024-01-31", agregacao="diario")

# Monthly climate aggregated by state (all stations)
df = await inmet.clima_uf("MT", ano=2024)
```

## Columns — `clima_uf`

| Column | Type | Description |
|---|---|---|
| `mes` | int | Month (1-12) |
| `uf` | str | State of the station |
| `precip_acum_mm` | float | Accumulated precipitation (mm) |
| `temp_media` | float | Mean temperature (C) |
| `temp_max_media` | float | Mean maximum temperature (C) |
| `temp_min_media` | float | Mean minimum temperature (C) |
| `num_estacoes` | int | Stations used in the aggregation |

## MetaInfo

```python
df, meta = await inmet.clima_uf("MT", ano=2024, return_meta=True)
print(meta.source)  # "inmet"
```

## Technical notes

- **Token required**: Observational data requires `AGROBR_INMET_TOKEN`.
  Without a token the API returns empty HTTP 204; the client converts it into
  `SourceUnavailableError` with instructions. The token goes into the request path
  (the API's `/token/...` scheme) and never appears in logs or error messages.
- The client sends a browser User-Agent (Chrome 120) on all
  requests. The INMET API drops connections without a browser User-Agent.
- The API automatically splits long periods into chunks of 365 days.
- Concurrency limited to 5 simultaneous stations per state.
- Data endpoint: `/estacao/{inicio}/{fim}/{codigo}` — with token,
  `/token/estacao/{inicio}/{fim}/{codigo}/{token}` (updated Jun/2026).
- For climate data without a token, [NASA POWER](nasa_power.md) is a
  functional alternative (`from agrobr import nasa_power`).

## Source

- API: `https://apitempo.inmet.gov.br`
- Update: daily
- History: 2000+
