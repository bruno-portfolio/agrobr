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
    # All federal conservation units
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
| bioma | str | IBGE biome |
| area_ha | float | Area in hectares |
| ano_criacao | Int64 | Creation year |
| ato_criacao | str | Legal act of creation |

## Limitations

- Only federal conservation units; the current count varies. State and municipal ones are not in this WFS.
- The `uf` field may contain multiple states (e.g., "MT/PA")
- `area_ha` is the whole unit's area, not the part inside the state: `uf="SP"` returns 22 units, 7 of them shared with another state and with their full area (the APA das Ilhas e Várzeas do Rio Paraná, SP/PR/MS, comes with 1,005,181 ha). Summing `area_ha` by state counts those units more than once.
- Data reflects the current state of the INDE/ICMBio GeoServer

## Reconciliation of the current layer

The 2026-09-18 capture contains 347 units without filters, 50 with
`bioma="Cerrado"` and 22 with `uf="SP"`. These overlap within the same layer:
419 checked occurrences, representing 347 distinct CNUC codes in this capture.
They are neither independent sources nor evidence for another historical date.
The contract has no primary key and does not remove potential future duplicates.

All nine output columns were compared cell by cell against complete CSV bodies,
including first and last records, through both the source and the dataset.
`areahaalb` is preserved as `area_ha`, without summation or scale conversion;
`criacaoano` is an attribute of a unit, not a layer edition. Compound state/biome
labels remain intact: 43 units in the unfiltered body span multiple states.
No fields were blank in these CSVs; area/year remain nullable nonetheless.

The WFS returned 11 fields, including `FID` and `ogc_fid`, retained in oracle
locators and omitted from public output. The XSD inventory covers all 22
properties, with explicit decisions for the 13 outside the tabular contract,
including geometry. The N1 checker compares this inventory and CSV structure;
it neither changes data nor replaces actual acquisition. Agreement with
`numberOfFeatures` requested before the CSV is recorded as `count_reconciled`,
without claiming a transactional snapshot.

Portable evidence: `tests/golden_data/reconciliacao_r11_20260918/icmbio/`.
The replay uses complete official bodies and actual HTTP parameters. Parser 3
and contract 1.0 remain unchanged. Geometry reconciliation is outside this
tabular variant.

`ucs_geo` requests `srsName=EPSG:4326` and checks the CRS declared by the body:
if another one comes back (the layer's native CRS is EPSG:4674), it raises
`ParseError` instead of publishing the coordinates labeled as 4326. Evidence:
`tests/golden_data/icmbio/crs_20260923/`.
