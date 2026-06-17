# INCRA — Quilombola Territories

!!! warning "Breaking change — legacy code"
    Previous versions accepted phases in a humanized format (`"Titulada"`,
    `"Em Titulacao"`, `"Decreto Publicado"`, `"RTID em Elaboracao"`,
    `"RTID Publicado"`). These values **never matched** the real data from the
    CMR/FUNAI server (which returns them in UPPERCASE), but the CQL filter was
    silently applied by the server and returned an empty result with no error
    — a silent functional bug.

    **As of this version, these values raise `ValueError`.** Migration:

    | Before | After |
    |-------|--------|
    | `fase="Titulada"` | `fase="TITULADO"` |
    | `fase="Em Titulacao"` | `fase="PORTARIA"` |
    | `fase="Decreto Publicado"` | `fase="DECRETO"` |
    | `fase="RTID em Elaboracao"` | `fase="RTID"` |
    | `fase="RTID Publicado"` | `fase="TITULO PARCIAL"` |

    Anyone using the old phase format was silently getting an empty DataFrame
    — incorrect behavior. The new error makes the inconsistency explicit.
    See [Filters](#filters) below for the canonical list.

## Overview

| Item | Detail |
|------|---------|
| Provider | INCRA (Instituto Nacional de Colonizacao e Reforma Agraria) |
| Data | Quilombola territories |
| Access | OGC WFS (CMR/FUNAI GeoServer) |
| Format | CSV (tabular) / GeoJSON (geo) |
| Authentication | None |
| License | Brazilian federal public data |
| Features | ~426 territories |

## Access via WFS

| Parameter | Value |
|-----------|-------|
| Endpoint | `cmr.funai.gov.br/geoserver/ows` |
| WFS Version | 1.0.0 |
| Layer | `CMR-PUBLICO:lim_quilombolas_a` |
| CRS | EPSG:4674 |

The layer is hosted on FUNAI's CMR server, not on INCRA.

## Usage Example

```python
import asyncio
from agrobr import incra

async def main():
    # All quilombola territories
    df = await incra.quilombolas()

    # Filter by state
    df = await incra.quilombolas(uf="BA")

    # Filter by process phase
    df = await incra.quilombolas(fase="TITULADO")

    # Combine filters
    df = await incra.quilombolas(uf="BA", fase="TITULADO")

    # With geometry (requires geopandas)
    gdf = await incra.quilombolas_geo(bbox=(-42, -15, -40, -13))

    # With metadata
    df, meta = await incra.quilombolas(return_meta=True)

asyncio.run(main())
```

## Filters

Parameters accepted by `quilombolas()` and `quilombolas_geo()`:

| Parameter | Type | Description |
|-----------|------|-----------|
| `uf` | str \| None | State abbreviation (case-insensitive) |
| `fase` | str \| None | Process phase (see table below) |
| `bbox` | tuple\[float, float, float, float\] \| None | (minlon, minlat, maxlon, maxlat) in EPSG:4674 |

### Valid phases

| Value | Meaning |
|-------|-------------|
| `CCDRU` | Concession of Real Right of Use |
| `DECRETO` | Expropriation decree published |
| `PORTARIA` | Recognition ordinance published |
| `RTID` | Technical Report of Identification and Delimitation |
| `TITULADO` | Territory with definitive title issued |
| `TITULO ANULADO` | Title annulled by court decision |
| `TITULO PARCIAL` | Partial titling (part of the territory) |

Values outside this list raise `ValueError`.

!!! note "Client-side filters"
    The `uf` and `fase` filters are applied **after** the download (the
    CMR/FUNAI server does not honor `CQL_FILTER` on these fields). The full
    dataset (~426 territories) is downloaded on every call, regardless of the
    filters. Use `bbox` to reduce the response size on the server.

## Columns

| Column | Type | Description |
|--------|------|-----------|
| codigo | str | Territory code |
| nome | str | Community name |
| municipio | str | Municipality |
| uf | str | State (abbreviation) |
| area_ha | float | Area in hectares |
| familias | Int64 | Number of families (nullable) |
| fase | str | Process phase |
| titulado | str | "T" (titled) or "F" (not titled) |
| data_publicacao | datetime | Publication date |
| data_titulo | datetime | Title date (nullable) |

## Limitations

- Data hosted on the FUNAI/CMR server, not INCRA
- Limit of 1500 features per request. When reached, the log
  `incra_quilombolas_truncated` (or `incra_quilombolas_geo_truncated`) is emitted
- The `uf`/`fase` filters are client-side (they do not reduce network traffic)
- Some dates may be missing (nullable)
- The `familias` field is nullable (not all records have this information)
- The `codigo` field (cd_quilomb) is nullable: ~63% of the records are
  pre-registration territories identified by CMR/FUNAI that have not yet
  received an official INCRA code. Use `df["codigo"].notna()` to filter only
  registered territories
- Invalid geometries in the GeoJSON are repaired via `shapely.validation.make_valid`
  with the log `incra_quilombolas_geo_repaired`
