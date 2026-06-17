# IBAMA — Environmental Embargoes

## Overview

| Item | Detail |
|------|---------|
| Provider | IBAMA (Instituto Brasileiro do Meio Ambiente e dos Recursos Naturais Renovaveis) |
| Data | Embargo terms for environmental infractions (SIFISC) |
| Access | CSV dump download (dadosabertos.ibama.gov.br) |
| Format | Zipped CSV (~47 MB, ~170 MB uncompressed) with WKT geometries |
| Authentication | None |
| License | ODbL (Open Database License) |
| Records | ~114K embargo terms, monthly update |

> The siscom.ibama.gov.br GeoServer WFS was decommissioned by the source in 2026.
> Access has migrated to the official SIFISC dump on the open data platform.

## Access

| Parameter | Value |
|-----------|-------|
| URL | `dadosabertos.ibama.gov.br/dados/SIFISC/termo_embargo/termo_embargo/termo_embargo_csv.zip` |
| Update | Monthly (Last-Modified on the server) |
| Filters | `uf` and `bbox` applied client-side after the download |

## Usage Example

```python
import asyncio
from agrobr import ibama

async def main():
    # All embargoes in Brazil
    df = await ibama.embargos()

    # Filter by state
    df = await ibama.embargos(uf="MT")

    # With WKT geometry (requires geopandas — [geo] extra)
    gdf = await ibama.embargos_geo(uf="RR")
    gdf = await ibama.embargos_geo(bbox=(-56, -16, -54, -14))

    # With metadata
    df, meta = await ibama.embargos(return_meta=True)

    # Polars
    df = await ibama.embargos(as_polars=True)

asyncio.run(main())
```

## Columns

| Column | Type | Description |
|--------|------|-----------|
| seq_tad | str | Term identifier in SIFISC (empty in records from the AIE Mob system) |
| numero_tad | str | Embargo Term number |
| data_embargo | datetime | Embargo date |
| num_processo | str | Administrative process number |
| descricao | str | Embargo/infraction description |
| codigo_municipio | str | Municipality IBGE code |
| municipio | str | Municipality |
| uf | str | State (abbreviation) |
| latitude / longitude | float | Term coordinates |
| area_embargada_ha | float | Embargoed area in hectares |
| nome_imovel | str | Property name |
| status | str | Form status (Lavrado, Cancelado, ...) |
| cancelado | bool | Term cancelled |
| data_desembargo | datetime | Disembargo date (NaT if active) |

`embargos_geo` adds `geometry` (Polygon/MultiPolygon, EPSG:4326) parsed from the WKT
of the dump itself — only records with a polygon.

## Particularities

- **PII excluded**: the source dump carries the name and CPF/CNPJ of the embargoed
  party; agrobr does not expose those fields (project policy). Anyone who needs them for
  compliance can download the raw CSV from the source.
- **Dirty dates in the source**: some records have impossible dates (e.g., year 2925);
  they are preserved when syntactically valid and become `NaT` when they do not parse.
- **bbox** filters by the point (lat/lon) of the term, not by polygon intersection.
- **Geo with no filter**: `embargos_geo()` without `uf`/`bbox` parses WKT for all of Brazil
  — slow; a warning is emitted.

## Limitations

- Geometry present in part of the records (embargoes without a polygon are left out of the geo)
- Full download (~47 MB) on every call — filters are client-side
- ODbL: free use with attribution to the source
