# ICMBio — Federal Conservation Units

## Overview

| Item | Detail |
|------|---------|
| Provider | ICMBio (Instituto Chico Mendes de Conservacao da Biodiversidade) |
| Data | Federal conservation unit boundaries |
| Access | OGC WFS (INDE GeoServer) |
| Format | CSV (tabular) / GeoJSON (geo) |
| Authentication | None |
| License | Brazilian federal public data |
| Features | 347 units in the 2026-09-18 capture (variable count) |

## Access via WFS

| Parameter | Value |
|-----------|-------|
| Endpoint | `geoservicos.inde.gov.br/geoserver/ICMBio/ows` |
| WFS Version | 1.1.0 |
| Layer | `ICMBio:limiteucsfederais_a` |
| CRS | EPSG:4674 in the layer; `ucs_geo` requests `srsName=EPSG:4326` |

## Usage Example

```python
import asyncio
from agrobr import icmbio

async def main():
    # Federal units in the ICMBio layer, without private reserves (RPPN)
    df = await icmbio.ucs()

    # Filter by group (PI = strict protection, US = sustainable use)
    df = await icmbio.ucs(grupo="PI")

    # Filter by state (uses LIKE, works with multi-state units)
    df = await icmbio.ucs(uf="MT")

    # With geometry (requires geopandas)
    gdf = await icmbio.ucs_geo(bbox=(-56, -16, -54, -14))

    # With metadata
    df, meta = await icmbio.ucs(return_meta=True)

asyncio.run(main())
```

## Columns

| Column | Type | Description |
|--------|------|-----------|
| codigo | str | Published CNUC code; duplicates are preserved |
| nome | str | Conservation unit name |
| categoria | str | Category abbreviation (PARNA, ESEC, FLONA, etc) |
| grupo | str | PI (strict protection) or US (sustainable use) |
| uf | str | State(s) covered (separated by /) |
| bioma | str | Text published by ICMBio, in upper case (e.g. `CERRADO E MATA ATLÂNTICA (LEI 11.428)`) |
| area_ha | float | Area in hectares |
| ano_criacao | Int64 | Creation year |
| ato_criacao | str | Legal act of creation |

## Limitations

- Only federal conservation units, without private reserves (RPPN); the current count varies. State and municipal units and private reserves are not in this WFS: for the full CNUC, use [`cnuc.ucs`](cnuc.md).
- The `uf` field may contain multiple states (e.g., "MT/PA")
- `area_ha` is the whole unit's area, not the part inside the state: `uf="SP"` returns 22 units, 7 of them shared with another state and with their full area (the APA das Ilhas e Várzeas do Rio Paraná, SP/PR/MS, comes with 1,005,181 ha). Summing `area_ha` by state counts those units more than once.
- Data reflects the current state of the INDE/ICMBio GeoServer

## Layer content

On 2026-09-18, the layer had 347 units without filters, 50 with `bioma="Cerrado"` and 22 with `uf="SP"`, overlapping selections with 347 distinct CNUC codes. The contract has no primary key and does not remove potential future duplicates.

`areahaalb` is preserved as `area_ha`, without summation or scale conversion; `criacaoano` is an attribute of a unit, not a layer edition. Compound state/biome labels remain intact: 43 units in the unfiltered query span multiple states. No fields were blank on that date; area and year remain nullable.

The WFS returns 11 fields; `FID` and `ogc_fid` are omitted from public output. The layer's XSD declares 22 properties, 13 of them outside the tabular contract, including geometry. Agreement with `numberOfFeatures` requested before the CSV is recorded as `count_reconciled`, without claiming a transactional snapshot.

`ucs_geo` requests `srsName=EPSG:4326` and checks the CRS declared by the body:
if another one comes back (the layer's native CRS is EPSG:4674), it raises
`ParseError` instead of publishing the coordinates labeled as 4326.

In `ucs_geo`, an area or year out of numeric format raises `ParseError`, as in `ucs`. The `uf`, `grupo` and `bioma`
filters apply to the downloaded layer (in `ucs`, they go into the server query): `MetaInfo` records the selection in
`source_details["query"]`, the filters applied to the result in `source_details["filtros_locais"]` and the acquisition
time in `fetched_at`/`fetch_timestamp`.
