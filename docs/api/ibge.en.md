# IBGE API

The IBGE module provides access to data from the IBGE Automatic Retrieval System (SIDRA). The municipal mesh and the urbanized areas come from the IBGE geoservices WFS ([`malha_municipal`](#malha_municipal-malha_municipal_geo) and [`areas_urbanizadas`](#areas_urbanizadas-areas_urbanizadas_geo)).

SIDRA queries use asynchronous HTTP directly, with a 120-second request deadline per attempt, cancellation, and exponential retries for transient failures. No transport thread remains pending after a timeout. Queries follow the [official SIDRA parameters](https://apisidra.ibge.gov.br/home/ajuda).

In API 2.0, territorial filters and flags are passed by keyword. PAM, LSPA, PPM, forestry and plant extraction accept only product/species and year positionally; slaughter accepts species and quarter; census functions accept only the topic. For GDP, only `setor` is positional: use `ibge.pib_agro(trimestre="202401")`. Milk accepts only `trimestre` positionally.

Closed domains normalize case and accents; invalid parameters raise `InvalidParameterError` before any query. `variaveis=[]`, unknown variables and empty year lists are rejected. Forestry and plant extraction validate years from 1974 through the current year. The historical census accepts an integer year or a nonempty list of integer years published for the topic.

Each empty SIDRA query emits a warning and records the same text in `MetaInfo.validation_warnings`, including after another empty query for the same table and period. Empty results preserve columns and dtypes: years/codes use `Int64`, measures use `float64`, quarter labels remain text, and text follows the installed pandas default. `animais_abatidos` uses `Int64` (contract 2.0); fractional head counts raise `ParseError`. PAM preserves its 14 output columns, with unrequested measures set to null.

## Functions

### `pam`

Retrieves Municipal Agricultural Production (PAM) data.

```python
async def pam(
    produto: str,
    ano: int | float | str | Sequence[int | float | str] | None = None,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variaveis: list[str] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) when return_meta=True
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str` | Product code (e.g. 'soja', 'milho') |
| `ano` | `int \| str \| list[int] \| None` | Year(s). Default: latest available |
| `uf` | `str \| None` | Filter by state (e.g. 'MT') |
| `nivel` | `Literal['brasil', 'uf', 'municipio']` | Level: 'brasil', 'uf', 'municipio' |
| `variaveis` | `list[str] \| None` | Specific variables |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | Returns a `(df, MetaInfo)` tuple with provenance |

**Available variables:**

| Code | Variable |
|------|----------|
| `area_plantada` | Planted area (hectares) |
| `area_colhida` | Harvested area (hectares) |
| `producao` | Quantity produced (see `unidade_producao`) |
| `rendimento` | Average yield (see `unidade_rendimento`) |
| `valor_producao` | Production value (see `unidade_valor_producao`) |

**Example:**

```python
from agrobr import ibge

# PAM by state
df = await ibge.pam('soja', ano=2023, nivel='uf')

# Multiple years
df = await ibge.pam('soja', ano=[2020, 2021, 2022, 2023])

# By municipality (filter state to reduce volume)
df = await ibge.pam('soja', ano=2023, nivel='municipio', uf='MT')

# Specific variables
df = await ibge.pam('soja', ano=2023, variaveis=['producao', 'area_plantada'])
```

---

### `lspa`

Retrieves Systematic Survey of Agricultural Production (LSPA) data.

```python
async def lspa(
    produto: str,
    ano: int | str | None = None,
    *,
    mes: int | str | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) when return_meta=True
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str` | Product code |
| `ano` | `int \| str \| None` | Year. Default: current |
| `mes` | `int \| str \| None` | Month (1-12). Without a filter: available months in the requested year |
| `uf` | `str \| None` | Filter by state |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | Returns a `(df, MetaInfo)` tuple with provenance |

The [LSPA 2.0 contract](../contracts/lspa.md) returns one row per year, month, locality, product, and variable. `mes` is always present; `variavel`, `variavel_cod`, and `unidade` identify the measure. `datasets.estimativa_safra` continues to consolidate the latest month and convert to its own contract units.

**LSPA products (21 codes, plus aliases):**

| Code | Product |
|------|---------|
| `soja` | Soybean |
| `milho_1` | Corn 1st crop |
| `milho_2` | Corn 2nd crop |
| `arroz` | Rice |
| `feijao_1` | Beans 1st crop |
| `feijao_2` | Beans 2nd crop |
| `feijao_3` | Beans 3rd crop |
| `trigo` | Wheat |
| `algodao` | Seed cotton |
| `cafe_arabica` | Arabica coffee (since 2012) |
| `cafe_canephora` | Canephora coffee (since 2012) |
| `amendoim_1` | Peanut 1st crop |
| `amendoim_2` | Peanut 2nd crop |
| `aveia` | Oats |
| `batata_1` | Potato 1st crop |
| `batata_2` | Potato 2nd crop |
| `batata_3` | Potato 3rd crop |
| `cevada` | Barley |
| `mamona` | Castor bean |
| `sorgo` | Sorghum |
| `triticale` | Triticale |

**Generic aliases:**

Generic names automatically expand into sub-crops and return a concatenated DataFrame:

| Alias | Expands to |
|-------|-----------|
| `milho` | `milho_1` + `milho_2` |
| `feijao` | `feijao_1` + `feijao_2` + `feijao_3` |
| `amendoim` | `amendoim_1` + `amendoim_2` |
| `batata` | `batata_1` + `batata_2` + `batata_3` |
| `cafe` | Official total through 2011; `cafe_arabica` + `cafe_canephora` since 2012 |

Species and crop seasons remain separate in `produto`. Historical coffee totals
use `produto="cafe"`. Yields are not additive; compute aggregate yield as
production × 1,000 / harvested area. `produtos_lspa()` includes codes and aliases.

**Example:**

```python
from agrobr import ibge

# Monthly LSPA
df = await ibge.lspa('soja', ano=2024, mes=6)

# Corn 2nd crop
df = await ibge.lspa('milho_2', ano=2024)

# Generic alias — returns milho_1 + milho_2 concatenated
df = await ibge.lspa('milho', ano=2024)

# By state
df = await ibge.lspa('soja', ano=2024, uf='MT')
```

---

### `produtos_pam`

Lists products available in PAM.

```python
async def produtos_pam() -> list[str]
```

---

### `produtos_lspa`

Lists products available in LSPA.

```python
async def produtos_lspa() -> list[str]
```

---

### `ppm`

Retrieves Municipal Livestock Survey (PPM) data.

```python
async def ppm(
    especie: str,
    ano: int | str | list[int] | None = None,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) when return_meta=True
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `especie` | `str` | Species or product (e.g. 'bovino', 'leite') |
| `ano` | `int \| str \| list[int] \| None` | Year(s). Default: latest available |
| `uf` | `str \| None` | Filter by state (e.g. 'MT') |
| `nivel` | `Literal['brasil', 'uf', 'municipio']` | Level: 'brasil', 'uf', 'municipio' |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | Returns a `(df, MetaInfo)` tuple with provenance |

**Available species (herds):**

| Code | Species |
|------|---------|
| `bovino` | Cattle |
| `bubalino` | Buffalo |
| `equino` | Horse |
| `suino_total` | Swine (total) |
| `suino_matrizes` | Sows |
| `caprino` | Goat |
| `ovino` | Sheep |
| `galinaceos_total` | Chickens (total) |
| `galinhas` | Hens (IBGE category "Galináceos - galinhas": includes laying and breeder hens) |
| `codornas` | Quail |

`galinhas_poedeiras` is still accepted as a deprecated alias of `galinhas`, with a `FutureWarning`; the output carries `especie="galinhas"`.

**Animal-origin products:**

| Code | Product | Unit |
|------|---------|------|
| `leite` | Milk | thousand liters |
| `ovos_galinha` | Chicken eggs | thousand dozen |
| `ovos_codorna` | Quail eggs | thousand dozen |
| `mel` | Honey | kg |
| `casulos` | Silkworm cocoons | kg |
| `la` | Wool | kg |

**Example:**

```python
from agrobr import ibge

# Cattle herd by state
df = await ibge.ppm('bovino', ano=2023, nivel='uf')

# Milk production by municipality in MG
df = await ibge.ppm('leite', ano=2023, nivel='municipio', uf='MG')

# Historical series
df = await ibge.ppm('bovino', ano=[2019, 2020, 2021, 2022, 2023])

# With metadata
df, meta = await ibge.ppm('bovino', ano=2023, return_meta=True)
```

---

### `especies_ppm`

Lists species and products available in PPM.

```python
async def especies_ppm() -> list[str]
```

---

### `abate`

Retrieves Quarterly Animal Slaughter Survey data.

```python
async def abate(
    especie: str,
    trimestre: str | list[str] | None = None,
    *,
    uf: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) when return_meta=True
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `especie` | `str` | Species: 'bovino', 'suino', 'frango' |
| `trimestre` | `str \| list[str] \| None` | Quarter YYYYQQ (e.g. '202303'). Default: latest available |
| `uf` | `str \| None` | Filter by state (e.g. 'PR') |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | Returns a `(df, MetaInfo)` tuple with provenance |

**Available species:**

| Code | Species | SIDRA table |
|------|---------|-------------|
| `bovino` | Cattle | 1092 |
| `suino` | Swine | 1093 |
| `frango` | Chicken | 1094 |

**Returned variables:**

| Variable | Description | Unit |
|----------|-------------|------|
| `animais_abatidos` | Number of slaughtered animals | head |
| `peso_carcacas` | Total carcass weight | kg |

**Example:**

```python
from agrobr import ibge

# Cattle slaughter by state
df = await ibge.abate('bovino', trimestre='202303')

# Chicken slaughter in Paraná
df = await ibge.abate('frango', trimestre='202303', uf='PR')

# Swine slaughter — all states
df = await ibge.abate('suino', trimestre='202304')

# With metadata
df, meta = await ibge.abate('bovino', trimestre='202303', return_meta=True)
```

---

### `especies_abate`

Lists species available in the Quarterly Slaughter survey.

```python
async def especies_abate() -> list[str]
```

---

### `censo_agro`

Retrieves Agricultural Census data (1995, 2006 and 2017).

```python
async def censo_agro(
    tema: str,
    *,
    ano: int | str | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) when return_meta=True
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `tema` | `str` | Census theme (see table below) |
| `ano` | `int \| str \| None` | Census year (1995, 2006 or 2017). Default: all available years |
| `uf` | `str \| None` | Filter by state (e.g. 'MT') |
| `nivel` | `Literal['brasil', 'uf', 'municipio']` | Level: 'brasil', 'uf', 'municipio' |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | Returns a `(df, MetaInfo)` tuple with provenance |

**Available themes:**

| Code | Theme | Table 1995 | Table 2006 | Table 2017 |
|------|-------|:-----------:|:-----------:|:-----------:|
| `efetivo_rebanho` | Herd inventory | 323 | — | 6907 |
| `uso_terra` | Land use | 316/311 | — | 6881 |
| `lavoura_temporaria` | Temporary crops | 497/492/503 | — | 6957 |
| `lavoura_permanente` | Permanent crops | 509/504/510 | — | 6956 |
| `preparo_solo` | Soil preparation | — | 791 | 6855 |
| `adubacao` | Fertilization | — | 1249 | 6848 |
| `calagem` | Liming | — | 1245 | 6849 |
| `agrotoxicos` | Pesticide use | — | 1459 | 6851 |
| `praticas_agricolas` | Agricultural practices | — | 837 | 8561 |
| `irrigacao` | Irrigation | — | 855 | 6857 |
| `despesa_adubos` | Fertilizer and soil amendment expenses | — | — | 6899 |

**Variables returned per theme (original themes):**

| Theme | Variable | Unit |
|-------|----------|------|
| `efetivo_rebanho` | `estabelecimentos` | units |
| `efetivo_rebanho` | `cabecas` | head |
| `uso_terra` | `estabelecimentos` | units |
| `uso_terra` | `area` | hectares |
| `lavoura_temporaria` | `estabelecimentos` (2017) or `informantes` (1995) | units |
| `lavoura_temporaria` | `producao` | varies |
| `lavoura_temporaria` | `area_colhida` | hectares |
| `lavoura_permanente` | `estabelecimentos` (2017) or `informantes` (1995) | units |
| `lavoura_permanente` | `producao` | varies |
| `lavoura_permanente` | `area_colhida` | hectares |

In 1995, the crop themes publish `informantes`: SIDRA variable 151 (tables 492 and 504), which SIDRA labels
"Número de informantes" (number of informants). In 2017, `estabelecimentos` is the "Número de estabelecimentos
agropecuários com lavoura temporária" (10084) and, for permanent crops, "com 50 pés e mais existentes" (9504).

**Categories of the newer themes (examples):**

| Theme | Categories (examples) |
|-------|----------------------|
| `preparo_solo` | Conventional tillage, Minimum tillage, No-till on straw |
| `adubacao` | Chemical, Organic, Green manure |
| `calagem` | Applied, Not applied |
| `agrotoxicos` | Used, Did not use |
| `praticas_agricolas` | Contour planting, Crop rotation, Fallow |
| `irrigacao` | Drip, Center pivot, Flooding, Sprinkler |
| `despesa_adubos` | Establishments with expenses, expense value |

**Example:**

```python
from agrobr import ibge

# Herd inventory by state (1995 and 2017)
df = await ibge.censo_agro('efetivo_rebanho')

# Land use in Mato Grosso
df = await ibge.censo_agro('uso_terra', uf='MT')

# Temporary crops by municipality
df = await ibge.censo_agro('lavoura_temporaria', nivel='municipio', uf='PR')

# Soil preparation — both years (2006 + 2017)
df = await ibge.censo_agro('preparo_solo')

# Irrigation 2017 only
df = await ibge.censo_agro('irrigacao', ano=2017)

# Fertilization in 2006, filtered by state
df = await ibge.censo_agro('adubacao', ano=2006, uf='SP')

# With metadata
df, meta = await ibge.censo_agro('efetivo_rebanho', return_meta=True)
```

---

### `temas_censo_agro`

Lists themes available in the Agricultural Census.

```python
async def temas_censo_agro() -> list[str]
```

---

### `censo_agro_legado`

Retrieves Agricultural Census 1995/96 data — six themes via FTP, in ZIP archives containing XLS or HTML tables.

[Contract 2.1](../contracts/censo_agropecuario_legado.md) distinguishes Brazil, actual state totals, and municipalities using the official tables. The default `nivel='uf'` queries all 27 states when `uf` is omitted. `nivel='brasil'` includes national activity categories and does not accept a `uf` filter. The `uf` column distinguishes municipalities with identical names; municipal codes absent from the source remain null. Variables and units come from the actual headers. IBGE does not publish Pará's municipal machinery table (`Para/Tab_7Mn.zip` carries Table 6, personnel): `tema='maquinas'` without `uf` returns the other 26 states, with the warning in `MetaInfo.validation_warnings` and a `UserWarning`, and `uf='PA'` raises `SourceUnavailableError`.

```python
async def censo_agro_legado(
    tema: str,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) when return_meta=True
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `tema` | `str` | Legacy theme (see table below) |
| `uf` | `str \| None` | Filter by state (e.g. 'SP') |
| `nivel` | `Literal['brasil', 'uf', 'municipio']` | Level: 'brasil', 'uf', 'municipio' |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | Returns a `(df, MetaInfo)` tuple with provenance |

**Available themes:**

| Code | Theme |
|------|-------|
| `tecnologia` | Technology (technical assistance, irrigation, fertilizers, etc.) |
| `pessoal_ocupado` | Persons employed (total, family, permanent, temporary) |
| `maquinas` | Machinery and equipment (tractors by HP range) |
| `producao_animal` | Animal production (milk, wool, eggs) |
| `valor_producao` | Value of production (crop, animal, subtypes) |
| `financeiro` | Financial data (investments, financing, expenses, revenue) |

**Example:**

```python
from agrobr import ibge

# Technology by state
df = await ibge.censo_agro_legado('tecnologia')

# Persons employed in São Paulo
df = await ibge.censo_agro_legado('pessoal_ocupado', uf='SP')

# Machinery in Goiás municipalities
df = await ibge.censo_agro_legado('maquinas', uf='GO', nivel='municipio')

# With metadata
df, meta = await ibge.censo_agro_legado('tecnologia', return_meta=True)
```

---

### `temas_censo_agro_legado`

Lists themes available in the Legacy Agricultural Census (FTP).

```python
async def temas_censo_agro_legado() -> list[str]
```

---

### `censo_agro_historico`

Retrieves the Agricultural Census historical series (1920-2006, state level maximum).

```python
async def censo_agro_historico(
    tema: str,
    *,
    ano: int | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal["brasil", "regiao", "uf"] = "uf",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) when return_meta=True
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `tema` | `str` | Historical series theme (see table below) |
| `ano` | `int \| list[int] \| None` | Census year(s). Default: all available |
| `uf` | `str \| None` | Filter by state (e.g. 'SP'). Only applied at nivel='uf' |
| `nivel` | `Literal['brasil', 'regiao', 'uf']` | Level: 'brasil', 'regiao', 'uf' (municipal NOT available) |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | Returns a `(df, MetaInfo)` tuple with provenance |

**Available themes:**

| Code | Theme | SIDRA table | Periods |
|------|-------|:------------:|---------|
| `estabelecimentos_area` | Establishments and area by area group | 263 | 1920-2006 (10 censuses) |
| `uso_terra` | Area by land use | 264 | 1970-2006 (6 censuses) |
| `pessoal_tratores` | Persons employed and tractors | 265 | 1970-2006 (6 censuses) |
| `condicao_produtor` | Establishments by producer status | 280 | 1920-2006 (10 censuses) |
| `efetivo_animais` | Animal inventory by species | 281 | 1970-2006 (6 censuses) |
| `producao_animal` | Animal production by type | 282 | 1920-2006 (10 censuses) |
| `producao_vegetal` | Crop production and harvested area | 283 | 1920-2006 (10 censuses) |
| `lavoura_permanente` | Quantity produced — permanent crops | 1730 | 1940-2006 (9 censuses) |
| `lavoura_temporaria` | Quantity produced — temporary crops | 1731 | 1940-2006 (9 censuses) |

**Example:**

```python
from agrobr import ibge

# Establishments and area, Brazil, 1985
df = await ibge.censo_agro_historico('estabelecimentos_area', ano=1985, nivel='brasil')

# Animal inventory, all states, all censuses
df = await ibge.censo_agro_historico('efetivo_animais')

# Persons and tractors in São Paulo, 1980 and 1985
df = await ibge.censo_agro_historico('pessoal_tratores', ano=[1980, 1985], uf='SP')

# Crop production, region level
df = await ibge.censo_agro_historico('producao_vegetal', nivel='regiao')

# With metadata
df, meta = await ibge.censo_agro_historico('uso_terra', ano=1985, return_meta=True)
```

---

### `temas_censo_agro_historico`

Lists themes available in the Agricultural Census historical series.

```python
async def temas_censo_agro_historico() -> list[str]
```

#### CLI

```bash
# All establishments/area data by state
agrobr ibge censo-historico estabelecimentos_area

# Specific year, CSV format
agrobr ibge censo-historico uso_terra --ano 1985 --formato csv

# Multiple years, Brazil level
agrobr ibge censo-historico efetivo_animais --ano 1970,1985,2006 --nivel brasil

# Filter by state
agrobr ibge censo-historico pessoal_tratores --ano 1985 --uf SP

# List available themes
agrobr ibge temas-historico
```

---

### `censo_agro_municipal_1985`

Agricultural Census 1985, municipal tables 67 to 119 of the IBGE's 28 state volumes (27 states; Minas Gerais in 2 volumes).
agrobr extracted the numbers from the IBGE's PDFs and ships them in a local package (`agrobr/data/censo_1985/`): queries do not use
the network.

```python
async def censo_agro_municipal_1985(
    tema: str,
    *,
    uf: str | None = None,
    nivel: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

- **1 row per PDF cell**, each with its `status`: the cell with the number and also the cells with no reading, with no identified
  column and outside the grid.
- **`valor` is filled only in cells confirmed by the printed sums** (municipality → microregion → mesoregion → state). `valor_lido`
  always carries the reading. Filter by `status` to pick the confidence level; the measured precision of each `status` is in the
  [contract](../contracts/censo_agropecuario_municipal_1985.md).
- `tema`: one of the 53 themes (`temas_censo_agro_municipal_1985()`), one per table, from the printed title.
- `uf`: state code. A state whose volume lacks the table raises `InvalidParameterError` with the reason (the IBGE omits the table
  that does not apply to the state).
  A table that is in the volume but of which the extraction read no cell (AM 80, AP 80, RR 80 and RR 119) raises
  `ParseError` with the pages and the reason.
- `nivel`: `uf`, `mesorregiao`, `microrregiao` or `municipio`.
- An invalid theme, state or level raises `InvalidParameterError` before reading the package.
- `MetaInfo`:
  - with 1 volume, `source_url` and `raw_content_hash` are the IBGE PDF and its SHA-256;
  - with more than 1, they are the catalog and the SHA-256 of the PDF list.
  - `source_method="pacote"`, `from_cache=False`; `source_details` carries the volumes and the query's page coverage.

```python
df = await ibge.censo_agro_municipal_1985("efetivo_bovinos", uf="ES")
confirmed = df[df["valor"].notna()]
```

### `temas_censo_agro_municipal_1985` / `cobertura_censo_agro_municipal_1985`

`temas_censo_agro_municipal_1985()` lists the 53 themes. `cobertura_censo_agro_municipal_1985()` gives, per theme, the states with
cells in the package.

```python
temas = await ibge.temas_censo_agro_municipal_1985()
cobertura = await ibge.cobertura_censo_agro_municipal_1985()
```

The CLI has `agrobr ibge temas-municipal-1985` and `agrobr ibge censo-municipal-1985 <tema> [--uf] [--nivel] [--formato]`.

---

### `ufs`

Lists available states.

```python
async def ufs() -> list[str]
```

---

### `malha_municipal` / `malha_municipal_geo`

Municipal boundaries from the IBGE territorial mesh, 2025 edition, at original resolution, through the IBGE geoservices WFS
(`https://geoservicos.ibge.gov.br/geoserverIBGE/wfs`, layer `CGMAT:qg_2025_030_munic`).

```python
async def malha_municipal(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame | tuple[pd.DataFrame | pl.DataFrame, MetaInfo]

async def malha_municipal_geo(
    *,
    uf: str | None = None,
    municipio: str | int | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = None,
    return_meta: bool = False,
) -> gpd.GeoDataFrame | tuple[gpd.GeoDataFrame, MetaInfo]
```

- `uf`: state code. `municipio`: 7-digit IBGE code or full name (`normalize.resolver_municipio`); pass `uf` as well to
  disambiguate the name. Both filters run on the server.
- `bbox` (`_geo` only): `(minlon, minlat, maxlon, maxlat)` in EPSG:4326. Returns the municipalities that touch the rectangle
  and combines with `uf`.
- **Columns:** `uf`, `cod_uf` (text, 2 digits), `cod_municipio` (`Int64`), `municipio`, `area_km2` (`float64`, the area
  published by IBGE). `_geo` adds `geometry` (MultiPolygon, EPSG:4326). Empty and full results share the same dtypes.
- **Lagoons:** the layer has 5,573 features: the 5,571 municipalities and 2 operational lagoon areas in RS (`4300001`, Lagoa
  Mirim, and `4300002`, Lagoa dos Patos), which IBGE publishes in the mesh. They come back with `uf="RS"`, with `bbox` and in
  the whole mesh; not by `municipio`, because they are not municipalities.
- **Limits:** the tabular call returns the whole layer (1.4 MB). `_geo` accepts up to 900 municipalities per query: MG, the
  largest state, has 853 (74 MB of GeoJSON, ~17 s on 2026-10-01). Above the cap, `ResourceLimitError` before the download;
  narrow with `uf`, `municipio`, `bbox` or `max_registros`. For the whole country as a file, use the mesh ZIP on the
  [IBGE geoftp](https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2025/)
  (`Brasil/BR_Municipios_2025.zip`, 237 MB, or `UFs/<UF>/<UF>_Municipios_2025.zip`).
- **Original WFS pages:** to store them as IBGE publishes them, in the native CRS (`EPSG:4674`) and with the
  manifest, use [raw collection](bruto.md): `bruto.coletar("ibge", "malha_municipal", ...)` or `"areas_urbanizadas"`.
- `max_registros` cuts on the server, in `cd_mun` order; with it, `coverage["truncated"]` is `True` when the selection is
  larger.
- **`MetaInfo`:** `source="ibge"`, `selected_source` `ibge_malha_municipal_wfs` (`_wfs_geo` for geo); `source_details` with
  `layer`, `edition` (2025), `query`, `coverage` (the `hits` count runs before the download and is reconciled with what
  arrives) and `count`. Current layer: no `ano`, `inicio` or `fim`.

```python
municipios = await ibge.malha_municipal(uf="MT")
sinop = await ibge.malha_municipal_geo(municipio="Sinop", uf="MT")
entorno = await ibge.malha_municipal_geo(bbox=(-48.3, -16.1, -47.3, -15.5))
```

---

### `areas_urbanizadas` / `areas_urbanizadas_geo`

IBGE Urbanized Areas of Brazil 2022: polygons mapped on satellite imagery, through the WFS
(`https://geoservicos.ibge.gov.br/geoserverCGEO/wfs`, layer `CGEO:AU_2026_AreasUrbanizadas2022_Brasil`, 190,172 polygons).

```python
async def areas_urbanizadas(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame | tuple[pd.DataFrame | pl.DataFrame, MetaInfo]

async def areas_urbanizadas_geo(
    *,
    bbox: tuple[float, float, float, float],
    max_registros: int | None = None,
    return_meta: bool = False,
) -> gpd.GeoDataFrame | tuple[gpd.GeoDataFrame, MetaInfo]
```

- **`bbox` is required**, `(minlon, minlat, maxlon, maxlat)` in EPSG:4326: the layer has no state or municipality. Without
  it, `TypeError`; with `bbox=None`, `InvalidParameterError`.
- **One municipality:** use the rectangle of its boundary, `tuple((await ibge.malha_municipal_geo(municipio=...)).total_bounds)`.
  It also picks neighbouring polygons; to cut by the boundary, intersect with the municipality geometry (`geopandas.clip` or
  `geopandas.overlay`).
- **Columns:** `id` (text, the layer `fid`), `densidade`, `tipo` and `comparacao` (text, as IBGE publishes them; in the DF
  rectangle, `densidade` is "Densa", "Pouco densa" or "Loteamento vazio", and `comparacao`, the change since 2019, has values
  such as "Sem alteração", "Adição" and "Densificação"), `data_imagem` (`datetime64[ns]`, the first day of the image month,
  published as `"2022/09"`), `area_ha` and `area_km2` (`float64`). `_geo` adds `geometry` (MultiPolygon, EPSG:4326).
- **Limits:** 50,000 polygons in the tabular call and 10,000 in `_geo` per query (the DF rectangle has 1,083, 1.1 MB of
  GeoJSON). Above that, `ResourceLimitError` before the download. `max_registros` cuts on the server, in `id` (text) order.
- **`MetaInfo`:** as in the mesh, with `selected_source` `ibge_areas_urbanizadas_wfs` (`_wfs_geo`) and `edition` 2022.

```python
bbox = tuple((await ibge.malha_municipal_geo(municipio="Sinop", uf="MT")).total_bounds)
areas = await ibge.areas_urbanizadas_geo(bbox=bbox)
```

---

### `silvicultura`

Retrieves Plant Extraction and Silviculture Production (PEVS) data — silviculture.

```python
async def silvicultura(
    produto: str,
    ano: int | str | list[int] | None = None,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variavel: str = "quantidade_produzida",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str` | Product code (e.g. 'madeira_tora', 'carvao') or area species (e.g. 'eucalipto') |
| `ano` | `int \| str \| list[int] \| None` | Year(s). Default: latest available |
| `uf` | `str \| None` | Filter by state (e.g. 'MG') |
| `nivel` | `Literal['brasil', 'uf', 'municipio']` | Level: 'brasil', 'uf', 'municipio' |
| `variavel` | `str` | 'quantidade_produzida', 'valor_producao' (table 291) or 'area' (table 5930) |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | Return MetaInfo |

**Products (table 291, classification c194):**

`carvao`, `carvao_eucalipto`, `carvao_pinus`, `carvao_outras`, `lenha`, `lenha_eucalipto`, `lenha_pinus`, `lenha_outras`, `madeira_tora`, `madeira_celulose`, `madeira_outras_finalidades`, `acacia_negra`, `eucalipto_folha`, `resina`

**Area species (table 5930, classification c734) — variavel='area':**

`eucalipto`, `pinus`, `outras`

**Example:**

```python
from agrobr import ibge

# Log wood production by state
df = await ibge.silvicultura('madeira_tora', ano=2023)

# Eucalyptus planted area
df = await ibge.silvicultura('eucalipto', variavel='area')

# Charcoal in MG
df = await ibge.silvicultura('carvao', ano=2023, uf='MG')

# With metadata
df, meta = await ibge.silvicultura('madeira_tora', ano=2023, return_meta=True)
```

---

### `produtos_silvicultura`

Lists products available in silviculture (table 291).

```python
async def produtos_silvicultura() -> list[str]
```

---

### `especies_silvicultura_area`

Lists species available for planted area (table 5930).

```python
async def especies_silvicultura_area() -> list[str]
```

---

### `extracao_vegetal`

Retrieves Plant Extraction and Silviculture Production (PEVS) data — plant extraction.

```python
async def extracao_vegetal(
    produto: str,
    ano: int | str | list[int] | None = None,
    *,
    uf: str | None = None,
    nivel: Literal["brasil", "uf", "municipio"] = "uf",
    variavel: str = "quantidade_produzida",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str` | Product code (e.g. 'acai', 'castanha_para') |
| `ano` | `int \| str \| list[int] \| None` | Year(s). Default: latest available |
| `uf` | `str \| None` | Filter by state |
| `nivel` | `Literal['brasil', 'uf', 'municipio']` | Level: 'brasil', 'uf', 'municipio' |
| `variavel` | `str` | 'quantidade_produzida' or 'valor_producao' |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | Return MetaInfo |

**Products (table 289, classification c193):**

`acai`, `castanha_caju`, `castanha_para`, `erva_mate`, `mangaba`, `palmito`, `pequi_fruto`, `pinhao`, `umbu`, `hevea_coagulado`, `hevea_liquido`, `carnauba_cera`, `carnauba_po`, `piacava`, `carvao`, `lenha`, `madeira_tora`, `babacu`, `copaiba`, `cumaru`, `pequi_amendoa`

**Example:**

```python
from agrobr import ibge

# Açaí production by state
df = await ibge.extracao_vegetal('acai', ano=2023)

# Brazil nut in Amazonas
df = await ibge.extracao_vegetal('castanha_para', ano=2023, uf='AM')

# Value of production
df = await ibge.extracao_vegetal('acai', ano=2023, variavel='valor_producao')

# With metadata
df, meta = await ibge.extracao_vegetal('acai', ano=2023, return_meta=True)
```

---

### `produtos_extracao_vegetal`

Lists products available in plant extraction (table 289).

```python
async def produtos_extracao_vegetal() -> list[str]
```

---

### `leite_trimestral`

Retrieves Quarterly Milk Survey data — acquisition, processing and average price.

```python
async def leite_trimestral(
    trimestre: str | list[str] | None = None,
    *,
    uf: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `trimestre` | `str \| list[str] \| None` | Quarter YYYYQQ (e.g. '202303'). Default: latest |
| `uf` | `str \| None` | Filter by state |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | Return MetaInfo |

**Returned columns (wide pivot):**

| Column | Type | Description |
|--------|------|-------------|
| `trimestre` | str | Quarter YYYYQQ |
| `localidade` | str | State |
| `localidade_cod` | int | IBGE code |
| `leite_adquirido` | float | Raw milk acquired (thousand liters) |
| `leite_industrializado` | float | Raw milk processed (thousand liters) |
| `preco_medio` | float | Average price paid to producer (BRL/liter) |
| `fonte` | str | "ibge_leite_trimestral" |

**Example:**

```python
from agrobr import ibge

# Quarterly milk by state
df = await ibge.leite_trimestral(trimestre='202303')

# Filter by state
df = await ibge.leite_trimestral(trimestre='202303', uf='MG')

# Multiple quarters
df = await ibge.leite_trimestral(trimestre=['202301', '202302', '202303'])

# With metadata
df, meta = await ibge.leite_trimestral(trimestre='202303', return_meta=True)
```

---

### `pib_agro`

Retrieves the quarterly agricultural GDP (Quarterly National Accounts).

```python
async def pib_agro(
    setor: str = "agropecuaria",
    *,
    trimestre: str | list[str] | None = None,
    precos: str = "corrente",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `trimestre` | `str \| list[str] \| None` | Quarter YYYYQQ. Default: latest |
| `precos` | `str` | 'corrente' (table 1846) or 'real_1995' (table 6612) |
| `setor` | `str` | 'agropecuaria', 'industria', 'servicos' or 'pib_total' |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | Return MetaInfo |

**Example:**

```python
from agrobr import ibge

# Agricultural GDP at current prices
df = await ibge.pib_agro(trimestre='202501')

# GDP at real prices (1995 base)
df = await ibge.pib_agro(trimestre='202501', precos='real_1995')

# Total GDP (all sectors)
df = await ibge.pib_agro(trimestre='202501', setor='pib_total')

# With metadata
df, meta = await ibge.pib_agro(return_meta=True)
```

---

## PAM vs LSPA vs PPM vs Slaughter vs PEVS vs Milk vs GDP vs Agri Census vs Historical Series vs Municipal 1985

| Aspect | PAM | LSPA | PPM | Slaughter | PEVS | Milk | GDP | Agri Census | Legacy Census | Historical Series | Municipal 1985 |
|--------|-----|------|-----|-----------|------|------|-----|-------------|---------------|-------------------|----------------|
| Frequency | Annual | Monthly | Annual | Quarterly | Annual | Quarterly | Quarterly | Decennial | One-off (1995/96) | Decennial | One-off (1985) |
| Granularity | To municipality | To state | To municipality | State | To municipality | State | Brazil | To municipality | To municipality | Brazil/Region/State | To municipality |
| Type | Consolidated | Estimates | Consolidated | Consolidated | Consolidated | Consolidated | Estimates | Census | Census (FTP) | Census | Census (OCR) |
| Availability | Y+1 year | Y+1 month | Y+1 year | Q+2 months | Y+1 year | Q+2 months | Q+2 months | Post-census | Static | Static | Static (local package) |
| Scope | Crops | Crops | Livestock | Slaughter | Silviculture + Plant extraction | Milk (acquisition, processing) | Sector GDP | Agri structure | 6 legacy themes | 9 themes (1920-2006) | 53 themes (1985) |

## SIDRA Tables Used

| Table | Description |
|-------|-------------|
| 5457 | PAM - Series since 1974 |
| 6588 | LSPA - Monthly estimates |
| 3939 | PPM - Herd inventory |
| 74 | PPM - Animal-origin production |
| 1092 | Slaughter - Cattle |
| 1093 | Slaughter - Swine |
| 1094 | Slaughter - Chickens |
| 323 | Agri Census 1995 - Herd inventory |
| 316 / 311 | Agri Census 1995 - Land use |
| 497 / 492 / 503 | Agri Census 1995 - Temporary crops |
| 509 / 504 / 510 | Agri Census 1995 - Permanent crops |
| 6907 | Agri Census 2017 - Herd inventory |
| 6881 | Agri Census 2017 - Land use |
| 6957 | Agri Census 2017 - Temporary crops |
| 6956 | Agri Census 2017 - Permanent crops |
| 791 / 6855 | Agri Census 2006/2017 - Soil preparation |
| 1249 / 6848 | Agri Census 2006/2017 - Fertilization |
| 1245 / 6849 | Agri Census 2006/2017 - Liming |
| 1459 / 6851 | Agri Census 2006/2017 - Pesticides |
| 837 / 8561 | Agri Census 2006/2017 - Agricultural practices |
| 855 / 6857 | Agri Census 2006/2017 - Irrigation |
| 263 | Historical Series - Establishments and area |
| 264 | Historical Series - Land use |
| 265 | Historical Series - Persons and tractors |
| 280 | Historical Series - Producer status |
| 281 | Historical Series - Animal inventory |
| 282 | Historical Series - Animal production |
| 283 | Historical Series - Crop production |
| 1730 | Historical Series - Permanent crops |
| 1731 | Historical Series - Temporary crops |
| 289 | PEVS - Plant extraction (c193) |
| 291 | PEVS - Silviculture production (c194) |
| 5930 | PEVS - Silviculture area (c734) |
| 1086 | Milk - Quarterly Milk Survey |
| 1846 | GDP - National Accounts at current prices |
| 6612 | GDP - National Accounts at real prices (1995) |

## Synchronous Version

```python
from agrobr.sync import ibge

df = ibge.pam('soja', ano=2023)
df = ibge.lspa('milho_1', ano=2024, mes=6)
df = ibge.ppm('bovino', ano=2023)
df = ibge.abate('bovino', trimestre='202303')
df = ibge.censo_agro('efetivo_rebanho')
df = ibge.censo_agro('preparo_solo', ano=2017)
df = ibge.censo_agro_legado('tecnologia')
df = ibge.censo_agro_legado('pessoal_ocupado', uf='SP')
df = ibge.censo_agro_historico('estabelecimentos_area', ano=1985)
temas_1985 = ibge.temas_censo_agro_municipal_1985()
df = ibge.silvicultura('madeira_tora', ano=2023)
df = ibge.extracao_vegetal('acai', ano=2023)
df = ibge.leite_trimestral(trimestre='202303')
df = ibge.pib_agro(trimestre='202501')
```

## Notes

- Municipality-level queries generate large data volumes
- Filtering by state is recommended when using municipality level
- LSPA is updated monthly by IBGE
- PAM is consolidated annually after harvest
- PPM is consolidated annually (September), series since 1974
- Quarterly Slaughter available since 1997, updated each quarter (Q+2 months)
- Agricultural Census: 11 themes, data from 1995, 2006 and/or 2017 depending on availability. 2017 reference: Oct/2016 to Sep/2017
- Legacy Agricultural Census: 6 FTP themes (tecnologia, pessoal_ocupado, maquinas, producao_animal, valor_producao, financeiro). Fixed year 1995
- Historical Series: 9 themes, 1920-2006, up to state (municipal NOT available). Mixed units per category (Poultry=Thousand head, etc)
- Municipal Census 1985: 53 themes (tables 67 to 119), 27 states, cell by cell, in a local package; `valor` only in cells confirmed by the printed sums and `valor_lido` always, with each cell's `status` (contract 2.0)

## PAM units and historical breaks

Published values are not implicitly converted. `unidade_producao`, `unidade_rendimento`, and `unidade_valor_producao` identify each row's scale. Before 2001, oranges use `mil_frutos` and `frutos/ha`; from 2001 onward, `ton` and `kg/ha`. `condicao_produto` distinguishes coffee `em_coco` through 2001 from `beneficiado` since 2002. Historical currencies remain identified without conversion to BRL or inflation adjustment. See the [IBGE methodology notes](https://sidra.ibge.gov.br/pesquisa/pam/tabelas/).

`localidade_cod` (`producao_anual` contract 2.2) carries the locality's IBGE code as SIDRA publishes it (D1C): 7 digits for a municipality, 2 for a state and 1 for Brazil. Use it to join municipalities across years, because the published name changes (and the Federal District comes out as "Brasília (DF)", without the " - UF" suffix of the others). In `producao_anual`, only IBGE rows carry the code; the CONAB fallback does not.

SIDRA's `-` symbol means numeric zero and remains zero; `..`, `...`, and `X` remain missing. Municipalities with zero production are retained. The `producao_anual` contract is 2.2; the four descriptive columns and `localidade_cod` are optional in the contract and supplied by the PAM API.

PAM parser 2 also preserves localities and measures whose values are entirely missing or suppressed. Two observations for the same locality, year and measure, including colliding variable aliases, raise `ParseError`; unmapped variables are also rejected. The reader does not silently select the first value. The PAM API schema is 2.1 (2.0 plus `localidade_cod`); the dataset contract is 2.2, with `cod_municipio`.

In the PEVS APIs, `variavel="valor_producao"` preserves the published monetary unit in `unidade` and returns `valor` as `float64`, even when all values are whole numbers. The parser is version 2; forestry and plant extraction contracts are at 1.1.

The quarterly slaughter, milk and GDP APIs use parser 2. In slaughter data, `-`
means zero while `X` and `...` remain null; reported carcass weight is preserved
when the head-count observation is absent. Slaughter and milk reject duplicate
observations for the same variable, quarter and locality before joining them.
Milk `preco_medio` and GDP `valor` use `float64`, including all-integer inputs.

The current SIDRA census API uses parser 3, which publishes the source's `Total`
row, and the historical one uses parser 2. In both, `-` means zero, while
`X`/`...` remain null. Year/locality/topic/category/variable keys must be unique,
including across complementary tables. For soil-preparation tables whose
variables encode categories, a reported year is preserved; the table year is
used only when the response omits the period dimension.
