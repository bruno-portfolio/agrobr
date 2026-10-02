# Contract: uso_do_solo

Land cover and use (MapBiomas) — annual cover and transitions between classes.

## Modes

| `tipo=` / `nivel=` | Contract | Source |
|---------|----------|--------|
| `"cobertura"` / `"estado"` (defaults) | `MAPBIOMAS_COBERTURA_V2` | MapBiomas |
| `"cobertura"` / `"municipio"` | `MAPBIOMAS_COBERTURA_MUNICIPAL_V1` | MapBiomas |
| `"transicao"` / `"estado"` | `MAPBIOMAS_TRANSICAO_V2` | MapBiomas |

## Schema: Cover

| Column | Type | Nullable | Unit | Constraints |
|--------|------|----------|------|-------------|
| `bioma` | STRING | No | — | valid biome |
| `uf` | STRING | No | — | state code |
| `classe_id` | INTEGER | No | — | MapBiomas LULC code |
| `classe` | STRING | Yes | — | Null only for a code outside the legend |
| `nivel_0` | STRING | Yes | — | — |
| `ano` | INTEGER | No | — | ≥ 1985 |
| `area_ha` | FLOAT | No | ha | ≥ 0 |

**PK:** `(bioma, uf, classe_id, ano)`

## Schema: Transition

| Column | Type | Nullable | Unit | Constraints |
|--------|------|----------|------|-------------|
| `bioma` | STRING | No | — | valid biome |
| `uf` | STRING | No | — | state code |
| `classe_de_id` | INTEGER | No | — | LULC code |
| `classe_de` | STRING | Yes | — | Null only for a code outside the legend |
| `classe_para_id` | INTEGER | No | — | LULC code |
| `classe_para` | STRING | Yes | — | Null only for a code outside the legend |
| `periodo` | STRING | No | — | YYYY-YYYY format |
| `area_ha` | FLOAT | No | ha | ≥ 0 |

**PK:** `(bioma, uf, classe_de_id, classe_para_id, periodo)`

## Municipal level

Coverage with `nivel="municipio"` validates its own `mapbiomas.cobertura_municipal` 1.1 contract. Key validation is no longer skipped in this mode. The state schema remains separate.

| Column | Physical pandas type | Nullable | Meaning |
|--------|----------------------|----------|---------|
| `bioma` | str | No | Published biome |
| `uf` | str | No | State code of the published intersection |
| `municipio` | str | No | Published territorial name, without surrounding spaces |
| `classe_id` | Int64 | No | Class code |
| `classe` | str | Yes | SDK-normalized label for the collection; not a literal legend transcription; null only for a code outside the legend |
| `nivel_0` | str | No | Published text category, including `Undefined` |
| `ano` | Int64 | No | Reference year |
| `area_ha` | float64 | No | Finite non-negative area in hectares; zero is preserved |
| `geocodigo` | str | No | Published `geocode`, seven ASCII digits as text |
| `cod_municipio` | Int64 | Yes | IBGE municipality code taken from `geocodigo`; null when the code has no state prefix |
| `id_registro` | Int64 | No | Non-negative original `ID`, scoped to its collection and resource |

Text uses the installed pandas default dtype: `str` on pandas 3 and `object` on pandas 2.

The key is `(bioma, uf, geocodigo, classe_id, id_registro, ano)` within one collection and resource. A code may identify intersections in multiple states; this does not justify removing the state from the key. `geocodigo` also includes entities such as lakes, without promising membership in the current IBGE municipal catalog. `id_registro` is copied from the original `ID`, including zero; it is not a generated ordinal or a stable identity across publications.

All 41 years from 1985 to 2025 and every identified row in Collection 11 are validated before returning, including data outside the selected filters. Only selected output rows are accumulated. Missing areas, incompatible types, non-finite/negative values or repeated full keys cause an error; there is no imputation or silent deduplication. Selections without matches return an empty DataFrame with the same types.

`municipio` accepts the full name (case- and accent-insensitive, with `uf` to disambiguate) or the seven-digit code, and selects by `geocodigo`; it requires `nivel="municipio"`. A partial name, a name shared by several municipalities without `uf` and a code absent from the resource raise `InvalidParameterError`. A `classe_id` outside the classes published in the collection also raises, with the list. See [source parameters and provenance](../api/mapbiomas.en.md#municipal-coverage-in-collection-11).

Collection 10 uses the same eleven-column municipal schema, with 40 years from 1985 to 2024. Its resource contains rows with the same territorial combination and distinct areas, preserved through their original IDs. There is no automatic summation or deduplication; a repeated published ID causes an error before filtering. `source_details["territorial_keys"]` describes territorial repetitions without classifying them as geographic errors.

Interpret `classe_id` together with its collection: municipal class 13 means **Outras Formações não Florestais** (Other non-Forest Formations) in 10 and **Mosaico Herbáceo-Arbustivo** (Herbaceous-Shrub Mosaic) in 11. Class 0 means not observed in both. Contract identity does not establish semantic equivalence across collections. Within one collection the legend is the same for the state and municipal cuts: class 0 is returned as `Não observado` in both. A published code outside the known legend comes out with a null label (`classe`, or `classe_de`/`classe_para` in the transition), the published `classe_id` and a warning (`UserWarning` and `meta.validation_warnings`) listing the codes. Up to 1.1.0, the state cut returned `Classe {id}`. A class cell without an integer code is a workbook defect: parsing fails with `ParseError`, giving the row and the published value.

The sum of a municipality's classes is the area mapped by MapBiomas, not IBGE's territorial area. In the 93 coastal municipalities with a bay or island, it falls below IBGE's area, which includes inland waters (35% below in Florianópolis). Fernando de Noronha does not appear.

In 5 Amazonian border states, the published sum of municipalities exceeds the published state figure for the same collection, year and class, by the same amount in 1985, 2000 and 2025 (Collection 11):

| State | Σ municipalities − state, all classes | Forest Formation (class 3), 2025 |
|---|---:|---:|
| RR | +20,059 ha | +19,589 ha (0.14%) |
| AM | +20,316 ha | +18,615 ha (0.015%) |
| PA | +9,671 ha | +8,976 ha (0.011%) |
| AP | +5,851 ha | +5,695 ha (0.057%) |
| AC | +4,078 ha | +3,996 ha (0.029%) |

In the other 22 states, the difference stays below 1 ha (in RS, with its lagoons, +0.985 ha). agrobr passes both publications through as published, and their `READ_ME` does not address the difference. For a state total, use `nivel="estado"` rather than summing municipalities.

## Collections

`colecao=11` selects Collection 11 (1985–2025); `colecao=10` identifies Collection 10 (1985–2024). The default `None` uses Collection 11. The dataset forwards the selection to the source without mixing collections. Municipal, state and transition schemas have their own contracts.

Each collection revises the full historical series. For reproducibility, pin `colecao` and retain the bytes and hashes identified by `meta.source_details["acquisition"]`, along with `meta.source_url`, `meta.fetched_at` and `meta.data_sources`. A collection does not freeze a revision, and there is no integrated local cache.

## Selection and errors

`nivel="uf"` is an alias of `nivel="estado"` and returns the same result. The dataset uses MapBiomas alone, without fallback to another institution. Coverage accepts `bioma`, `uf`, `ano`, `classe_id`, `nivel`, `municipio` and `colecao`. Transition accepts `bioma`, `uf`, `periodo`, `classe_de_id`, `classe_para_id` and `colecao`, at state level only. Filters for the other mode and unknown options are rejected rather than ignored.

`as_polars` and `return_meta` require booleans. Polars conversion takes place after contract validation. There is no `produto` or `use_cache`. The `deterministic` context is rejected before acquisition because arbitrary historical snapshot selection is unavailable. Acquisition errors surface through the dataset layer as `SourceUnavailableError` and layout errors as `ParseError`, both with details in `errors`; an invalid workbook does not produce a partial result.

## Example

```python
from agrobr import datasets

# Cover by state
df = await datasets.uso_do_solo(tipo="cobertura", bioma="Cerrado", ano=2022)

# Cover by municipality
df = await datasets.uso_do_solo(
    tipo="cobertura", nivel="municipio", municipio="Sorriso", uf="MT",
    ano=2025, colecao=11,
)

# Transitions between classes
df = await datasets.uso_do_solo(tipo="transicao", periodo="2020-2021")

# With metadata
df, meta = await datasets.uso_do_solo(tipo="cobertura", return_meta=True)
```
