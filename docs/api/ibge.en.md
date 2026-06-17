# IBGE API

The IBGE module provides access to data from the IBGE Automatic Retrieval System (SIDRA).

## Functions

### `pam`

Retrieves Municipal Agricultural Production (PAM) data.

```python
async def pam(
    produto: str,
    ano: int | str | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal['brasil', 'uf', 'municipio'] = 'uf',
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
| `producao` | Quantity produced (tonnes) |
| `rendimento` | Average yield (kg/ha) |

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
| `mes` | `int \| str \| None` | Month (1-12). Default: latest |
| `uf` | `str \| None` | Filter by state |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | Returns a `(df, MetaInfo)` tuple with provenance |

**LSPA products:**

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
| `algodao` | Herbaceous cotton |
| `amendoim_1` | Peanut 1st crop |
| `amendoim_2` | Peanut 2nd crop |
| `batata_1` | Potato 1st crop |
| `batata_2` | Potato 2nd crop |

**Generic aliases:**

Generic names automatically expand into sub-crops and return a concatenated DataFrame:

| Alias | Expands to |
|-------|-----------|
| `milho` | `milho_1` + `milho_2` |
| `feijao` | `feijao_1` + `feijao_2` + `feijao_3` |
| `amendoim` | `amendoim_1` + `amendoim_2` |
| `batata` | `batata_1` + `batata_2` |

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
    uf: str | None = None,
    nivel: Literal['brasil', 'uf', 'municipio'] = 'uf',
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
| `galinhas_poedeiras` | Laying hens |
| `codornas` | Quail |

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

# Swine slaughter — Brazil
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
    ano: int | str | None = None,
    uf: str | None = None,
    nivel: Literal['brasil', 'uf', 'municipio'] = 'uf',
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

**Variables returned per theme (original themes):**

| Theme | Variable | Unit |
|-------|----------|------|
| `efetivo_rebanho` | `estabelecimentos` | units |
| `efetivo_rebanho` | `cabecas` | head |
| `uso_terra` | `estabelecimentos` | units |
| `uso_terra` | `area` | hectares |
| `lavoura_temporaria` | `estabelecimentos` | units |
| `lavoura_temporaria` | `producao` | varies |
| `lavoura_temporaria` | `area_colhida` | hectares |
| `lavoura_permanente` | `estabelecimentos` | units |
| `lavoura_permanente` | `producao` | varies |
| `lavoura_permanente` | `area_colhida` | hectares |

**Categories of the newer themes (examples):**

| Theme | Categories (examples) |
|-------|----------------------|
| `preparo_solo` | Conventional tillage, Minimum tillage, No-till on straw |
| `adubacao` | Chemical, Organic, Green manure |
| `calagem` | Applied, Not applied |
| `agrotoxicos` | Used, Did not use |
| `praticas_agricolas` | Contour planting, Crop rotation, Fallow |
| `irrigacao` | Drip, Center pivot, Flooding, Sprinkler |

**Example:**

```python
from agrobr import ibge

# Herd inventory by state (2017)
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

Retrieves Agricultural Census 1995/96 data — 6 legacy themes via FTP (XLS).

> **Note:** `nivel='uf'` returns data by **mesoregion** (not by individual state) — a quirk of the legacy format; `nivel='municipio'` and `nivel='brasil'` work as expected.

```python
async def censo_agro_legado(
    tema: str,
    uf: str | None = None,
    nivel: Literal['brasil', 'uf', 'municipio'] = 'uf',
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

# Technology by mesoregion
df = await ibge.censo_agro_legado('tecnologia')

# Persons employed in São Paulo
df = await ibge.censo_agro_legado('pessoal_ocupado', uf='SP')

# Machinery — municipality level
df = await ibge.censo_agro_legado('maquinas', nivel='municipio')

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
    ano: int | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal['brasil', 'regiao', 'uf'] = 'uf',
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

Retrieves Agricultural Census 1985 data at municipal level (extracted via OCR from IBGE PDFs).

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

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `tema` | `str` | Theme (53 available — use `temas_censo_agro_municipal_1985()`) |
| `uf` | `str \| None` | Filter by state (22 states available) |
| `nivel` | `str \| None` | Filter: total, mesorregiao, microrregiao, municipio |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | Return MetaInfo |

**Example:**

```python
from agrobr import ibge

# Land ownership in São Paulo
df = await ibge.censo_agro_municipal_1985('propriedade_terras', uf='SP')

# Municipalities only
df = await ibge.censo_agro_municipal_1985('bovinos', nivel='municipio')

# With metadata
df, meta = await ibge.censo_agro_municipal_1985('propriedade_terras', return_meta=True)
```

---

### `temas_censo_agro_municipal_1985`

Lists themes available in the Municipal Agricultural Census 1985.

```python
async def temas_censo_agro_municipal_1985() -> list[str]
```

#### CLI

```bash
# Land ownership data in SP
agrobr ibge censo-municipal-1985 propriedade_terras --uf SP

# CSV format
agrobr ibge censo-municipal-1985 bovinos --formato csv

# Filter by level
agrobr ibge censo-municipal-1985 uso_terra_lavoura --nivel municipio --uf MG

# List available themes
agrobr ibge temas-municipal-1985
```

---

### `ufs`

Lists available states.

```python
async def ufs() -> list[str]
```

---

### `silvicultura`

Retrieves Plant Extraction and Silviculture Production (PEVS) data — silviculture.

```python
async def silvicultura(
    produto: str,
    ano: int | str | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal['brasil', 'uf', 'municipio'] = 'uf',
    variavel: str = 'quantidade_produzida',
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
    uf: str | None = None,
    nivel: Literal['brasil', 'uf', 'municipio'] = 'uf',
    variavel: str = 'quantidade_produzida',
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
    trimestre: str | list[str] | None = None,
    precos: str = 'corrente',
    setor: str = 'agropecuaria',
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
| Granularity | To municipality | To state | To municipality | Brazil + state | To municipality | State | Brazil | To municipality | To municipality | Brazil/Region/State | To municipality |
| Type | Consolidated | Estimates | Consolidated | Consolidated | Consolidated | Consolidated | Estimates | Census | Census (FTP) | Census | Census (OCR) |
| Availability | Y+1 year | Y+1 month | Y+1 year | Q+2 months | Y+1 year | Q+2 months | Q+2 months | Post-census | Static | Static | Static |
| Scope | Crops | Crops | Livestock | Slaughter | Silviculture + Plant extraction | Milk (acquisition, processing) | Sector GDP | Agri structure | 6 legacy themes | 9 themes (1920-2006) | 53 themes (1985) |

## SIDRA Tables Used

| Table | Description |
|-------|-------------|
| 5457 | PAM - New series (2018+) |
| 6588 | LSPA - Monthly estimates |
| 1612 | PAM - Temporary crops (historical) |
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
df = ibge.censo_agro_municipal_1985('propriedade_terras', uf='SP')
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
- Agricultural Census: 10 themes, data from 1995, 2006 and/or 2017 depending on availability. 2017 reference: Oct/2016 to Sep/2017. 30-day cache
- Legacy Agricultural Census: 6 FTP themes (tecnologia, pessoal_ocupado, maquinas, producao_animal, valor_producao, financeiro). Fixed year 1995. 90-day cache
- Historical Series: 9 themes, 1920-2006, up to state (municipal NOT available). Mixed units per category (Poultry=Thousand head, etc). 30-day cache
- Municipal Census 1985: 53 themes, municipal data for 22 states, extracted via OCR from IBGE state PDFs. Static (bundled) data. The `confianca` field indicates OCR quality
```
