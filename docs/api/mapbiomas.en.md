# MapBiomas (Land Cover and Use)

Tabular data from the MapBiomas Project — area (ha) by land cover and use class, biome and state, with an annual historical series since 1985.

## `mapbiomas.cobertura()`

Area by land cover and use class x biome x state x year.

```python
import agrobr

df = await agrobr.mapbiomas.cobertura(bioma="Cerrado", ano=2020, uf="GO")
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `bioma` | `str` | No | Biome: "Amazonia", "Cerrado", "Caatinga", "Mata Atlantica", "Pampa", "Pantanal". If None, all |
| `uf` | `str` | No | State code or full state name (e.g. `"MT"`, `"Mato Grosso"`). Case and accents are optional; invalid values raise `InvalidParameterError` with the valid codes, before download |
| `ano` | `int` | No | Year: 1985-2025 in collection 11; 1985-2024 in collection 10. If None, all years |
| `classe_id` | `int` | No | MapBiomas class code (e.g. 15 for Pasture). A code outside the classes published in the collection raises `InvalidParameterError` with the list, after download |
| `nivel` | `str` | No | `"estado"` (default) or `"municipio"`. The full municipal file is downloaded before filtering |
| `municipio` | `str` or `int` | No | Full municipality name (case- and accent-insensitive; with `uf` to disambiguate) or seven-digit territorial code (`int` or text). A partial name, a name shared by several municipalities without `uf` and a code absent from the resource raise `InvalidParameterError`. Requires `nivel="municipio"` |
| `colecao` | `int` | No | `10` or `11`; `None` uses the current collection (11). Other collections raise `ValueError` before download |
| `as_polars` | `bool` | No | Return as polars.DataFrame |
| `return_meta` | `bool` | No | If True, returns `(DataFrame, MetaInfo)` |

### Selecting a collection

Each collection has its own files and historical revisions. Set `colecao` and retain the resource and its hash to reproduce an analysis; selecting a collection does not freeze its bytes. Collection 11 also revises years before 2025.

```python
df, meta = await agrobr.mapbiomas.cobertura(
    uf="MT", ano=2025, colecao=11, return_meta=True
)
previous = await agrobr.mapbiomas.cobertura(uf="MT", ano=2024, colecao=10)
print(meta.data_sources, meta.source_url)
```

`meta.data_sources` identifies `mapbiomas_colecao_11` or `mapbiomas_colecao_10`; `meta.source_url` records the requested file. `colecao=10, ano=2025` is invalid. The `datasets.uso_do_solo()` dataset also accepts and forwards `colecao`.

### Returned Columns

| Column | Type | Description |
|--------|------|-------------|
| `bioma` | str | Biome name |
| `uf` | str | State code (e.g. "MT") |
| `municipio` | str | Municipality name (only when `nivel="municipio"`) |
| `classe_id` | int | MapBiomas class code |
| `classe` | str | SDK-normalized label for the collection; not a literal transcription of the legend worksheet |
| `nivel_0` | str | Published text, such as `Natural`, `Antropic`, `Natural/Antropic` and `Undefined` |
| `ano` | int | Reference year |
| `area_ha` | float | Area in hectares |
| `geocodigo` | str | Municipal coverage only: territorial identifier published by MapBiomas |
| `id_registro` | Int64 | Municipal coverage only: the original numeric row `ID`, scoped to its collection and resource |
| `cod_municipio` | Int64 | Municipal coverage only: IBGE code taken from `geocodigo`; null without a state prefix |

### Municipal coverage in Collection 11

Municipal output contains eleven columns in the order above: the previous eight, `geocodigo`, `id_registro` and `cod_municipio` (`Int64`, IBGE code taken from `geocodigo`, null without a state prefix). `classe_id`, `ano` and `id_registro` use pandas `Int64`, `area_ha` uses `float64` and text uses the default text dtype of the installed pandas (`str` in pandas 3, `object` in 2), including an empty selection. The `mapbiomas.cobertura_municipal` 1.1 contract validates `(bioma, uf, geocodigo, classe_id, id_registro, ano)` within one collection and resource. The municipal parser has version 2; state contracts and parsing retain their separate versions.

`geocodigo` preserves the published `geocode` column; membership in the current IBGE municipal catalog is not guaranteed. The resource includes Lagoa Mirim and Lagoa dos Patos, and a code may appear in more than one state. Published territorial intersections remain separate, without correcting the state or automatically summing areas.

The `municipio` filter always selects by `geocodigo`. A name goes through `normalize.resolver_municipio`, which compares the full name with the IBGE register and returns the code: `"Santa Rita"` does not match `"Santa Rita do Sapucaí"`, and a name shared by several municipalities requires `uf`. A seven-digit code is used as given, because the resource also publishes codes outside the register (the lagoons); a code that does not appear in the collection's resource raises `InvalidParameterError` after download. To find a municipality by part of its name, use `normalize.buscar_municipios`.

The entire file is downloaded without integrated caching. The parser reads every row and all 41 years from 1985 through 2025 before returning, validating identity, uniqueness and areas outside the selected filters as well. Missing, negative, non-finite or incompatible areas cause `ParseError`; zero is retained. Only entirely empty rows are skipped and counted. Output follows worksheet year and row order, without first materializing a national wide DataFrame.

With `return_meta=True`, `source_details` records the layout fingerprint, population and output counts, annual statistics and `geocodes_with_multiple_states`. `source_details["acquisition"]` separates the HTTP resource, any download confirmation and the extracted XLSX member. `raw_content_hash` and `raw_content_size` describe the downloaded HTTP body; when that body is a ZIP, the XLSX hash is in `acquisition["member"]`. These records do not certify scientific accuracy or equivalence across revisions.

Collection 10 uses 40 years from 1985 to 2024 and may publish multiple rows sharing a biome/state/geocode/class combination with distinct areas. `id_registro` retains the original `ID` to preserve these rows without summing or deduplicating them. Zero is valid; the parser does not generate this ID, it is not a public filter, and stability across files, revisions or collections is not promised. Uniqueness of the published ID is checked across the worksheet before filtering. Both collections return the same municipal schema.

The legend also depends on the collection: in municipal resource 10, class 13 means **Other non-Forest Formations**, returned as `Outras Formações não Florestais`; in 11, it means **Herbaceous-Shrub Mosaic**, returned as `Mosaico Herbáceo-Arbustivo`. Both publish class 0 as not observed. Identical codes across collections do not establish equivalent meanings; within one collection, the state and municipal cuts share the same legend.

### MapBiomas Classes (main)

| Code | Class | Level 0 |
|--------|--------|---------|
| 3 | Formacao Florestal | Natural |
| 4 | Formacao Savanica | Natural |
| 12 | Formacao Campestre | Natural |
| 15 | Pastagem | Antropic |
| 18 | Agricultura | Antropic |
| 39 | Soja | Antropic |
| 20 | Cana | Antropic |
| 40 | Arroz | Antropic |
| 9 | Silvicultura | Antropic |
| 21 | Mosaico de Usos | Antropic |
| 24 | Area Urbanizada | Antropic |
| 33 | Rio, Lago e Oceano | Natural |

---

## `mapbiomas.transicao()`

Area of transitions between land use classes by biome x state x period.

```python
import agrobr

df = await agrobr.mapbiomas.transicao(bioma="Cerrado", periodo="2019-2020")
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `bioma` | `str` | No | Filter by biome. If None, all |
| `uf` | `str` | No | State code or full state name. Case and accents are optional; invalid values raise `InvalidParameterError` with the valid codes, before download |
| `periodo` | `str` | No | Period in the selected collection (e.g. `"2019-2020"`, `"1985-2025"` in collection 11) |
| `classe_de_id` | `int` | No | Source class code. A code outside the published classes raises `InvalidParameterError` with the list, after download |
| `classe_para_id` | `int` | No | Target class code, with the same check |
| `colecao` | `int` | No | `10` or `11`; `None` uses the current collection (11). The transition file belongs to the selected collection |
| `as_polars` | `bool` | No | Return as polars.DataFrame |
| `return_meta` | `bool` | No | If True, returns `(DataFrame, MetaInfo)` |

### Returned Columns

| Column | Type | Description |
|--------|------|-------------|
| `bioma` | str | Biome name |
| `uf` | str | State code |
| `classe_de_id` | int | Source class code |
| `classe_de` | str | Source class name |
| `classe_para_id` | int | Target class code |
| `classe_para` | str | Target class name |
| `periodo` | str | Period in "YYYY-YYYY" format |
| `area_ha` | float | Area in hectares |

### Available Periods

In collection 11, annual periods run from `1985-1986` to `2024-2025`, five-year periods from `1985-1990` to `2020-2025`, and the full period is `1985-2025`. The spreadsheet also includes ten-year and special intervals. Collection 10 ends in 2024.

Use `df.periodo.unique()` without a `periodo` filter to inspect the intervals actually published in the selected collection.

---

## Synchronous Usage

```python
from agrobr import sync

df = sync.mapbiomas.cobertura(bioma="Cerrado", ano=2020)
df_trans = sync.mapbiomas.transicao(bioma="Amazonia", periodo="2019-2020")
```

## Examples

### Deforestation in the Cerrado (loss of native vegetation)

```python
import agrobr

# Transition from Formacao Florestal (3) to Pastagem (15) in the Cerrado
df = await agrobr.mapbiomas.transicao(
    bioma="Cerrado",
    classe_de_id=3,
    classe_para_id=15,
    periodo="2019-2020",
)
print(f"Area convertida: {df['area_ha'].sum():,.0f} ha")
```

### Municipal coverage (Belem, PA)

Filters are applied after downloading the municipal file for the selected collection.

```python
import agrobr

df = await agrobr.mapbiomas.cobertura(
    nivel="municipio", uf="PA", municipio="Belém", ano=2020
)
print(df[["municipio", "classe", "area_ha"]].head())
```

By the seven-digit territorial code:

```python
df, meta = await agrobr.mapbiomas.cobertura(
    nivel="municipio", municipio=5107925, classe_id=39,
    ano=2025, colecao=11, return_meta=True,
)
print(df[["bioma", "uf", "municipio", "geocodigo", "area_ha"]])
```

### Soybean evolution in Brazil

```python
import agrobr

df = await agrobr.mapbiomas.cobertura(classe_id=39)  # Soja
pivot = df.groupby("ano")["area_ha"].sum()
print(pivot)
```

## Data Source

- **Project:** MapBiomas — Annual Mapping of Land Cover and Use in Brazil
- **Default collection:** 11 (August 2026); collection 10 remains available explicitly
- **Historical series:** 1985-2025 in collection 11; 1985-2024 in collection 10
- **Resolution:** 30m (Landsat)
- **Provider:** Multi-institutional collaborative network
- **Data:** [Land cover and use — MapBiomas 30m](https://brasil.mapbiomas.org/iniciativas-e-produtos/cobertura-e-uso-da-terra/cobertura-30m/cobertura/)
- **License:** Public data — free to use with attribution to the MapBiomas Project
