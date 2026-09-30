# MapBiomas

## Overview

| Field | Value |
|-------|-------|
| **Provider** | MapBiomas Project — multi-institutional network |
| **Data** | Land cover and use, transitions between classes |
| **Access** | Public XLSX download from official MapBiomas repositories |
| **Format** | Direct XLSX or XLSX inside ZIP; municipal coverage is streamed with openpyxl |
| **Authentication** | None |
| **License** | CC BY 4.0 with MapBiomas attribution; classified as `livre` |
| **Time series** | 1985-2025 (collection 11); 1985-2024 (collection 10) |

## Data Origin

MapBiomas is a multi-institutional collaborative project that produces annual maps of land cover and use of Brazil from Landsat satellite imagery (30m resolution). The data is generated via automatic classification using Google Earth Engine.

agrobr accesses **tabular statistics** of areas in hectares by class, biome, state and municipal intersection, published as XLSX spreadsheets. The municipal publication's `READ_ME` defines the biome × state × municipality intersection, from 1985 to 2025 in Collection 11. These APIs do not return rasters. [Official coverage publication](https://brasil.mapbiomas.org/iniciativas-e-produtos/cobertura-e-uso-da-terra/cobertura-30m/cobertura/).

Use requires attribution including the source, collection, access date and link, following the project's citation format. The official FAQ declares CC BY 4.0. [Access, license and citation](https://brasil.mapbiomas.org/faq/?tema=dados).

## Collections

MapBiomas publishes annual collections with methodological improvements:

| Collection | Date | Period |
|---------|------|---------|
| 11 (current) | August 2026 | 1985-2025 |
| 10 | August 2025 | 1985-2024 |

The `colecao` parameter accepts `10` and `11`. The default `None` uses collection 11. The selected collection determines the file, worksheet, legend and year limit; unsupported collections raise `InvalidParameterError` before download. Every release revises the historical series. Pinning a collection identifies the edition but does not freeze its file: retain bytes and hashes for reproducibility. A new collection released by MapBiomas needs a new agrobr version; until then, the default stays at 11.

Both APIs (`cobertura` and `transicao`) and the `datasets.uso_do_solo` dataset preserve this selection. With `return_meta=True`, `meta.data_sources` identifies `mapbiomas_colecao_10` or `mapbiomas_colecao_11`, and `meta.source_url` records the requested file.

## Data Structure

### Coverage

Data in wide format: one column per year with area in hectares for each biome x state x class combination. The series starts in 1985 and ends in 2025 for collection 11 or 2024 for collection 10.

After parsing, agrobr converts it to long format: one row per biome x state x class x year combination.

### Municipal coverage and territorial identity

`cobertura(nivel="municipio")` preserves `municipio` and `geocodigo`, from the published `geocode` column. The code is seven ASCII digits as text; it does not guarantee membership in the current IBGE municipal catalog. Lagoa Mirim and Lagoa dos Patos appear as territorial entities. A geocode may occur in multiple states, and these intersections remain separate. The code does not justify correcting the state or automatically aggregating areas.

In 5 Amazonian border states, the published sum of municipalities exceeds the published state figure for the same collection, year and class, by the same amount in 1985, 2000 and 2025 (Collection 11):

| State | Σ municipalities − state, all classes | Forest Formation (class 3), 2025 |
|---|---:|---:|
| RR | +20,059 ha | +19,589 ha (0.14%) |
| AM | +20,316 ha | +18,615 ha (0.015%) |
| PA | +9,671 ha | +8,976 ha (0.011%) |
| AP | +5,851 ha | +5,695 ha (0.057%) |
| AC | +4,078 ha | +3,996 ha (0.029%) |

In the other 22 states, the difference stays below 1 ha (in RS, with its lagoons, +0.985 ha). agrobr passes both publications through as published, and their `READ_ME` does not address the difference. For a state total, use `nivel="estado"` rather than summing municipalities.

The `municipio` filter accepts the full name (case- and accent-insensitive, with `uf` to disambiguate, via `normalize.resolver_municipio`) or the seven-digit geocode, and always selects by geocode; a partial name is refused with the candidates. In Collection 11, the parser validates every row and all 41 years before returning, even when the request selects one municipality, class or year. Missing, non-finite, negative or incompatible areas abort parsing; zeros remain. Entirely empty rows are counted without fabricating observations.

The municipal parser streams the workbook and accumulates only selected output, retaining source year→row order. This reduces memory for filtered requests, but not download size or population validation. An unfiltered query still materializes the full long result. Fingerprints and statistics describe published structure and values, not scientific map accuracy.

Collection 10 has 40 years from 1985 to 2024 and repeated territorial rows with distinct areas. Municipal output from both collections preserves `id_registro`, the published numeric `ID`, to retain each row without aggregation or deduplication. The ID is local to its publication/collection, accepts zero and must not be treated as a stable link across revisions. The key includes biome, state, geocode, class, ID and year. The parser checks ID uniqueness across the population; repeated territorial combinations with distinct IDs remain valid and are counted in `source_details["territorial_keys"]`.

Class codes alone are insufficient for comparing collections. In the municipal Collection 10 table, class 13 is `Other non Forest Formations`, returned as **Outras Formações não Florestais**; in 11, it is **Mosaico Herbáceo-Arbustivo** (Herbaceous-Shrub Mosaic). Class 0 in both means not observed. Each cut uses its own collection's legend; the state and municipal cuts share that same legend.

### Transition

Area in hectares of transition between class pairs for each time period. Includes annual, five-year and other published intervals. The full period is 1985-2025 in collection 11 and 1985-2024 in collection 10.

## Usage Example

```python
import agrobr

# Cerrado coverage in 2020
df = await agrobr.mapbiomas.cobertura(bioma="Cerrado", ano=2020)

# Pasture (class 15) in Goias
df = await agrobr.mapbiomas.cobertura(bioma="Cerrado", uf="Goiás", classe_id=15)

# Forest→pasture transition in Cerrado
df = await agrobr.mapbiomas.transicao(
    bioma="Cerrado",
    classe_de_id=3,   # Forest Formation
    classe_para_id=15, # Pasture
    periodo="2019-2020",
)

# With metadata
df, meta = await agrobr.mapbiomas.cobertura(
    bioma="Cerrado", ano=2020, return_meta=True
)
print(meta.records_count, meta.fetch_duration_ms)
```

## Limitations

- Tabular data only (statistics). Geospatial data (rasters/GEE) is left for a future version
- The selected XLSX is downloaded in full on every call. Filters reduce the returned DataFrame size, without reducing the download.
- Municipality level is available through `cobertura(nivel="municipio")`, with a substantially larger file than the state-level spreadsheet. No integrated local cache yet.
- `classe` uses the Portuguese label from the collection's own `LEGEND_CODE` worksheet, without the hierarchical numbering and with the published spelling and qualifiers (for example, `Algodão (beta)` and `Parque eólico (beta)`). Classes that appear in the data without a Portuguese label in the legend use an agrobr translation of the English label in the rows: 0 → `Não observado` (`Not Observed`) in both collections, 75 → `Usina Fotovoltaica` (`Photovoltaic Project`) and 13 → `Outras Formações não Florestais` (`Other non Forest Formations`) in 10, 13 → `Mosaico Herbáceo-Arbustivo` (`Herbaceous-Shrub Mosaic`) in 11. `nivel_0` preserves published text. A published code outside the known legend comes out with a null label (`classe`, or `classe_de`/`classe_para` in the transition), the published `classe_id` and a warning (`UserWarning` and `meta.validation_warnings`) listing the codes. Up to 1.1.0, the state cut returned `Classe {id}`. A class cell without an integer code is a workbook defect: parsing fails with `ParseError`, giving the row and the published value.
- Tabular parsing does not infer territorial equivalence across collections, cadastral corrections or current validity of names/codes. Hierarchical classes should not be summed indiscriminately.

## Municipal file provenance

`meta.raw_content_hash` and `meta.raw_content_size` identify the HTTP body actually downloaded. `meta.source_details["acquisition"]` contains requested and final URLs, collection time, status and technical headers, plus a download confirmation when required. For a ZIP body, `member` identifies the extracted XLSX name, hash, CRC and size separately from its outer archive. A direct XLSX has no fabricated member.

Other `source_details` fields include collection, worksheet, layout fingerprint, population and output counts, annual statistics and geocodes spanning multiple states. See the [uso_do_solo municipal contract](../contracts/uso_do_solo.en.md#municipal-level) for row identity.

## Cache and Updating

- There is no local cache: every call downloads the corresponding spreadsheet from the source.
- MapBiomas publishes one new collection per year with retroactively recalculated data.
- Specifying filters is recommended to reduce the DataFrame size.

## Links

- [MapBiomas Brasil](https://brasil.mapbiomas.org)
- [Estatisticas](https://brasil.mapbiomas.org/estatisticas/)
- [Legenda](https://brasil.mapbiomas.org/codigos-de-legenda/)
- [Citation (FAQ)](https://brasil.mapbiomas.org/faq/?tema=dados)
