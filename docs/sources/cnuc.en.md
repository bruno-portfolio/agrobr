# CNUC — National Register of Conservation Units

## Overview

| Item | Detail |
|------|--------|
| Provider | MMA (Ministry of the Environment and Climate Change), Department of Protected Areas |
| Data | Boundaries and attributes of federal, state and municipal conservation units, including private reserves (RPPN) |
| Access | WFS 2.0 (MapServer behind the CNUC portal) |
| Format | GML 3.2 (tabular and geo) |
| Authentication | None |
| License | CC-BY ("Unidades de Conservação" dataset on the MMA open data portal) |
| Features | 3,450 units with a boundary on 2026-10-01 (count varies) |

## WFS access

| Parameter | Value |
|-----------|-------|
| Portal | `cnuc.mma.gov.br` |
| Endpoint | `cnuc-mapserv.mma.gov.br/cgi-bin/mapserv?MAP=/var/www/storage/app/mapfiles/ucs.map` |
| WFS version | 2.0.0 |
| Layer | `ms:ucs_selected` |
| CRS | EPSG:4674 in the layer; `ucs_geo` requests `urn:ogc:def:crs:EPSG::4326` |
| Open data | `dados.mma.gov.br/dataset/unidadesdeconservacao` (half-yearly CSV and license) |

The MapServer address is the one the CNUC portal itself uses for its map. The `MAP` parameter points to an internal server path: if MMA reorganizes the server, the URL changes without notice and the query raises `SourceUnavailableError`.

## Usage example

```python
import asyncio
from agrobr import cnuc

async def main():
    # Units from all 3 government levels in Sergipe, including private reserves
    df = await cnuc.ucs(uf="SE")

    # Federal private reserves only
    df = await cnuc.ucs(
        esfera="federal", categoria="Reserva Particular do Patrimônio Natural"
    )

    # Units covering a municipality (full name or IBGE code)
    df = await cnuc.ucs(municipio="Aracaju")

    # With geometry (requires geopandas)
    gdf = await cnuc.ucs_geo(uf="SE", esfera="municipal")

    # With metadata
    df, meta = await cnuc.ucs(bbox=(-38.5, -11.6, -36.3, -9.3), return_meta=True)

asyncio.run(main())
```

## Functions and parameters

`cnuc.ucs(*, uf=None, municipio=None, esfera=None, categoria=None, grupo=None, bioma=None, bbox=None, max_registros=None, as_polars=False, return_meta=False)` returns the table. `cnuc.ucs_geo(...)` takes the same filters, without `as_polars`, and returns a `GeoDataFrame` in EPSG:4326.

| Parameter | Values | Where it filters |
|-----------|--------|------------------|
| `uf` | state code | on the server by state name; the result matches the exact code inside `uf` |
| `municipio` | full name or 7-digit IBGE code | by the municipality's state on the server; each published municipality is compared with the IBGE register |
| `esfera` | `federal`, `estadual`, `municipal` | on the server |
| `categoria` | the 12 published management categories, case- and accent-insensitive | on the server |
| `grupo` | `PI`, `US` | on the server |
| `bioma` | Amazônia, Caatinga, Cerrado, Mata Atlântica, Pampa, Pantanal | on the result, through the `bioma` column |
| `bbox` | (min lon, min lat, max lon, max lat) in EPSG:4326 | on the server |
| `max_registros` | positive integer | the first units ordered by `codigo` |

A value outside the domain raises `InvalidParameterError` before any network call, listing the valid values. The 12 categories: Área de Proteção Ambiental, Área de Relevante Interesse Ecológico, Estação Ecológica, Floresta, Monumento Natural, Parque, Refúgio de Vida Silvestre, Reserva Biológica, Reserva de Desenvolvimento Sustentável, Reserva de Fauna, Reserva Extrativista and Reserva Particular do Patrimônio Natural.

The query counts the units on the server before downloading. Above 10,000 units in the table, or 600 in `ucs_geo`, it raises `ResourceLimitError` without downloading anything; the largest state (RJ) has 568. `bioma` alone does not reduce the download, because it filters the result. When no filter is applied on the result (`uf`, `municipio` and `bioma`), `max_registros` also reduces the download on the server.

## Columns

| Column | Type | Description |
|--------|------|-------------|
| codigo | str | CNUC code |
| nome | str | Unit name |
| esfera | str | `federal`, `estadual` or `municipal` |
| categoria | str | Management category, as published |
| grupo | str | `PI` (strict protection) or `US` (sustainable use) |
| categoria_iucn | str | Published IUCN category |
| uf | str | State codes in alphabetical order, separated by `/` |
| municipios | str | Published list, `NAME (UF), …` |
| bioma | str | Biomes with published area in the unit, separated by `/` |
| area_ha | float | Area in hectares |
| data_criacao | datetime64[ns] | Creation date |
| ato_criacao | str | Creation act |
| orgao_gestor | str | Managing agency |
| qualidade_poligono | str | Polygon from the legal description, estimate or schematic representation |
| wdpa_id | str | World Database on Protected Areas identifier |

`ucs_geo` adds `geometry` (Polygon or MultiPolygon).

## Limitations

- Only units with a boundary registered in CNUC. Of the 3,576 units in the CNUC CSV register of July 2026, 170 were not in the layer on 2026-10-01: 167 have the area taken from the legal act, that is, no polygon in CNUC, and 3 have a polygon-based area, such as the Cristópolis National Forest (BA), which is in the CSV and in ICMBio.
- The layer's buffer zones (61 on 2026-10-01, `limite='za'`) are left out.
- The source cuts `municipios` at 200 characters, with `...`, in 20 units. With `municipio=`, units in the state whose list was cut before naming the municipality are left out and listed in the warning.
- Three spellings in the layer do not match the IBGE register (`GRÃO PARÁ (SC)`, `SANTO ANTÔNIO DO LEVERGER (MT)` and `SÃO THOMÉ DAS LETRAS (MG)`); those units are also left out of the `municipio=` filter and listed in the warning. Warnings reach `MetaInfo.validation_warnings` and are raised as `UserWarning`.
- `area_ha` is the area of the whole unit, not the part inside the state.
- `bioma` is derived from the published per-biome areas: marine area is left out, and 65 units have a null `bioma`.
- The layer is current, without historical editions: `deterministic` does not apply.

## Layer content

On 2026-10-01 the layer had 3,511 features: 3,450 units and 61 buffer zones. By government level: 1,086 federal (736 private reserves), 1,458 state (669 private reserves) and 906 municipal (21 private reserves). No CNUC code repeated. Of the 347 federal units in the ICMBio layer (`icmbio.ucs`), 346 are here; CNUC already had 4 federal sustainable-development reserves of 2026-09-08 that ICMBio did not publish yet.

The server count (`resultType=hits`) comes before the download and is reconciled with the number of features received (`count_reconciled` in `MetaInfo`), without a transactional snapshot. CNUC is updated by the managing agencies, unit by unit.

## Relation to ICMBio

`icmbio.ucs` and the `unidades_conservacao_federais` dataset keep the ICMBio layer on INDE: federal units only, without private reserves. For the three government levels and private reserves, use `cnuc.ucs` or the `unidades_conservacao` dataset.
