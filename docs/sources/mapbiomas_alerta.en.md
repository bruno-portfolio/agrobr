# MapBiomas Alerta — Deforestation Alerts

## Overview

| Item | Detail |
|------|---------|
| Provider | MapBiomas |
| Data | Deforestation alerts with geometry |
| Access | GraphQL API |
| Format | JSON (GraphQL) |
| Authentication | Token (env AGROBR_MAPBIOMAS_ALERTA_TOKEN) |
| License | Free (attribution required) |
| Volume | 533,140 alerts published since 2019 (09/26/2026) |

## Access via GraphQL

| Parameter | Value |
|-----------|-------|
| Endpoint | `https://plataforma.alerta.mapbiomas.org/api/v2/graphql` |
| Auth | Bearer token |

The token is personal and expires. It comes from the API's `signIn` mutation, with the e-mail and password of the platform
account. With an expired or invalid token, the API answers "Token de acesso inválido", and agrobr raises
`SourceUnavailableError` with that message plus a hint to replace the token in `AGROBR_MAPBIOMAS_ALERTA_TOKEN` or in the
`token=` argument, without the token value. HTTP 401 or 403 comes out the same way ("credencial recusada"). `alerta_info()` is public and uses no token:
there, a refusal comes out as "acesso recusado (consulta sem token)", without the token hint.

## Usage Example

```python
import asyncio
from agrobr import mapbiomas_alerta

async def main():
    # Alerts detected in the period (requires token)
    df = await mapbiomas_alerta.alertas(
        token="your-token",
        inicio="2025-01-01",
        fim="2025-01-31",
    )

    # Last month's alerts: filter by publication
    df = await mapbiomas_alerta.alertas(
        inicio="2026-08-01",
        fim="2026-08-31",
        tipo_data="publicacao",
    )

    # Filter by detection source (API enum values)
    df = await mapbiomas_alerta.alertas(
        sources=["DeterbAmazonia", "Sad"],
        inicio="2025-01-01",
        fim="2025-01-31",
    )

    # Filter by box (minlon, minlat, maxlon, maxlat)
    df = await mapbiomas_alerta.alertas(
        bbox=(-55, -8, -50, -3),
        inicio="2025-01-01",
        fim="2025-01-31",
    )

    # Whole collection, without the default cap of 5,000
    df = await mapbiomas_alerta.alertas(
        inicio="2025-01-01",
        fim="2025-12-31",
        max_registros=None,
    )

    # With WKT geometry (requires geopandas)
    gdf = await mapbiomas_alerta.alertas_geo(
        inicio="2025-01-01",
        fim="2025-01-31",
    )

    # With metadata
    df, meta = await mapbiomas_alerta.alertas(inicio="2025-01-01", return_meta=True)

    # Polars
    df = await mapbiomas_alerta.alertas(inicio="2025-01-01", as_polars=True)

    # Info (date range + last publication)
    info = await mapbiomas_alerta.alerta_info()

asyncio.run(main())
```

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `inicio`, `fim` | str \| date \| datetime \| None | None | `date`, `datetime` (the time is dropped) or text `YYYY-MM-DD` or `DD/MM/YYYY` (agrobr sends ISO). A start after the end, or any other type or format, raises `InvalidParameterError` before the network. Without `inicio`, the API starts at 2019-01-01 |
| `tipo_data` | str | `"deteccao"` | `"deteccao"` filters the detection date; `"publicacao"`, the publication date. Anything else raises `InvalidParameterError` |
| `sources` | list[str] \| None | None | API enum values: `DeterbAmazonia`, `DeterCerrado`, `DeterPantanal`, `Glad`, `IefMg`, `InemaBa`, `ProdesAmazonia`, `ProdesCerrado`, `ProdesMataAtlantica`, `ProdesPampa`, `ProdesPantanal`, `ProdesCaatinga`, `Sad`, `SadCaatinga`, `SadCerrado`, `SadMataAtlantica`, `SadPampa`, `SadPantanal`, `SipamSar`, `SiradX`, `SosAtlas` and `SosInpe`, and `All` (every source). A value outside the enum (including the `fonte` column names, such as `DETERB-AMAZONIA`) raises `InvalidParameterError` before the network |
| `bbox` | tuple \| None | None | `(minlon, minlat, maxlon, maxlat)` in degrees |
| `max_registros` | int \| None | 5000 | Row cap. Above it, the alerts with the lowest codes come out, with a warning in `validation_warnings` and `UserWarning` ("N de M alertas"). `None` brings the whole collection |

## Detection × publication

An alert is published months after it is detected. As of 09/26/2026, across the 27,368 alerts detected from September 2024
to February 2025: publication came a median of 152 days after detection; 90% within 205 days, 95% within 223 and **99% within
293 days** (the maximum was 607). That is why a recent period by detection comes back almost empty: August 2026 had 0 alerts by
detection and 2,118 by publication.

With `tipo_data="deteccao"` and a period ending less than 293 days before today (or without `fim`), the result carries the
warning "este período ainda ganha alertas enquanto a publicação chega" (the period still gains alerts as publication arrives),
in `validation_warnings` and `UserWarning`. For "last month's alerts", use `tipo_data="publicacao"`.

## Pagination and completeness

agrobr paginates in pages of 500, ordered by alert code (`ALERT_CODE ASC`). The API's default order, by detection date, allows
ties, and pages repeated and dropped alerts (January 2025 came out with 4,196 of the 4,633). Each query is checked against the
announced `totalCount`:

- a code repeated across pages, or fewer alerts than announced, raises `ParseError` (repeat the query);
- a `totalCount` that changes during pagination (publication during the query) comes out as a warning;
- `meta.source_details` carries the `tipo_data`, the `total_anunciado` and, in `corpos`, the URL, SHA-256 and size of each
  page. With a single page, the SHA and size also go to `raw_content_hash` and `raw_content_size`.

## Columns

| Column | Type | Description |
|--------|------|-------------|
| alert_code | Int64 | Alert code |
| area_ha | float | Area in hectares |
| data_deteccao | datetime | Detection date |
| data_publicacao | datetime | Publication date |
| status | str | Alert status (`published`) |
| fonte | str | Detection sources as published, separated by ", " (e.g. `DETERB-AMAZONIA, SAD`) |
| lat | float | Latitude |
| lon | float | Longitude |
| geometry | Polygon | WKT geometry (alertas_geo only; an invalid WKT becomes a null geometry, and the alert stays) |

A query without alerts returns the same columns and dtypes (`alert_code` as `Int64`, dates as `datetime64[ns]`, `area_ha`,
`lat` and `lon` as `float64` and text in the default dtype of the installed pandas); `alertas_geo` keeps the `EPSG:4326` CRS.
`alerta_info()` raises `ParseError` when the response lacks `alertDateRange` or `lastAlertPublication` with their fields,
instead of returning empty dictionaries.

## Limitations

- Requires an authentication token (env `AGROBR_MAPBIOMAS_ALERTA_TOKEN` or `token=` parameter), which expires
- The query does not filter by state or municipality; the API has `territoryIds`, which agrobr does not expose
- Throttle after 5 pages (3s wait)
