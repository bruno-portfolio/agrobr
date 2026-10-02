# IBGE - Brazilian Institute of Geography and Statistics

## Overview

| Field | Value |
|-------|-------|
| **Institution** | Federal Government |
| **Website** | [ibge.gov.br](https://www.ibge.gov.br) |
| **API** | [SIDRA](https://sidra.ibge.gov.br) |
| **agrobr access** | Via SIDRA API (JSON); municipal mesh and urbanized areas via WFS (GeoJSON) |

## Data Origin

### Source

- **API**: `https://sidra.ibge.gov.br/`
- **Format**: JSON
- **Access**: Public, no authentication

### Access channel and fallback

Every tabular query (PAM, LSPA, PPM, slaughter, PEVS, milk, agricultural GDP and censuses) goes through
`agrobr.ibge.client.fetch_sidra`. The SIDRA API (`apisidra.ibge.gov.br`) is the primary channel; since
September 2026 it answers 403 with a Cloudflare challenge to programmatic clients. When SIDRA fails (403,
HTML, 5xx; a network failure or timeout does not trigger the switch), the same table is queried on the IBGE aggregates API
(`servicodados.ibge.gov.br/api/v3/agregados`) with the same selectors translated
(`t/p/v/n/c` → `agregados/{table}/periodos/{p}/variaveis/{v}?localidades=N{n}[...]&classificacao=c[...]`)
and the response is converted to the same SIDRA column layout (`NC`, `NN`, `MC`, `MN`, `V`, `D1C`…), so
parsers and contracts do not change. Known differences of the fallback channel: `MC` (unit code) is empty
because the aggregates API publishes only the unit name; `allxp` becomes `all`; the period name (`D2N`) comes
from the table's own `/periodos` endpoint. The channel used is recorded in
`MetaInfo.source_details["canal"]` (`sidra`, `servicodados` or `misto`), `source_details["consultas"]` lists channel
and URL per query, `attempted_sources` gains `ibge_servicodados` and `selected_source` becomes `ibge_servicodados`
when the fallback was used (datasets inherit that provenance); `source_url` points to the URL actually queried; the channel switch emits `SourceFallbackWarning` (details below). Both channels
return the same values for the July 2026 LSPA (soybeans). The IBGE health probe queries the aggregates API.

When a SIDRA response requires switching to the aggregates API, each acquisition emits `SourceFallbackWarning` with the reason. With `return_meta=True`, the message also appears in `meta.validation_warnings`, and `selected_source="ibge_servicodados"` identifies the selected channel. Successive calls keep emitting the warning; an empty response preserves both the channel-switch warning and the missing-observations warning. Datasets retain these metadata. Network failures and timeouts retain their existing handling and do not trigger this switch.

Each query also requests `/agregados/{tabela}/periodos` and records the modification date of the returned periods, which
tells which edition the number came from: `source_details["periodos_modificacao"]` comes as `{table: {period: ISO date}}`
(PAM 2024 was revised on 2026-09-17), and each `consultas` item has `tabela` and `periodos_modificacao`. The `01/01/0001`
IBGE publishes for a period without a date comes out null. If the metadata request fails, the query goes on, and the reason
is in `periodos_modificacao_erro`.

A response without observations (`[]`, for example a period not yet published), on either channel, returns an
empty DataFrame with the same columns as a response with data and emits a warning (`warnings.warn`, on every query, also recorded in `MetaInfo.validation_warnings`).

In API 2.0, territorial filters and flags are passed by keyword. PAM, LSPA, PPM, forestry and plant extraction accept only product/species and year positionally; slaughter accepts species and quarter; census functions accept only the topic. For GDP, only `setor` is positional: use `ibge.pib_agro(trimestre="202401")`. Milk accepts only `trimestre` positionally.

Closed domains normalize case and accents; invalid parameters raise `InvalidParameterError` before any query. `variaveis=[]`, unknown variables and empty year lists are rejected. Forestry and plant extraction validate years from 1974 through the current year. The historical census accepts an integer year or a nonempty list of integer years published for the topic.

Each empty SIDRA query emits a warning and records the same text in `MetaInfo.validation_warnings`, including after another empty query for the same table and period. Empty results preserve columns and dtypes: years/codes use `Int64`, measures use `float64`, quarter labels remain text, and text follows the installed pandas default. `animais_abatidos` uses `Int64` (contract 2.0); fractional head counts raise `ParseError`. PAM preserves its 14 output columns, with unrequested measures set to null.

## Available Surveys

### PAM - Municipal Agricultural Production

- **SIDRA table**: 5457 (series since 1974)
- **Coverage**: All municipalities
- **Frequency**: Annual

### LSPA - Systematic Survey of Agricultural Production

- **SIDRA table**: 6588
- **Coverage**: National/state
- **Frequency**: Monthly
- **Contract**: [LSPA 2.0](../contracts/lspa.md), one row per year/month/locality/product/variable, with an explicit unit; omitting `mes` preserves available months in the year

`ibge.lspa("soja", ano=2025, mes="01", uf="MT")` accepts an integer month or integer string from 1 to 12. `uf=None` queries the Brazil aggregate. An unpublished period may return an empty DataFrame; HTTP 200 does not establish observation availability.

The [`estimativa_safra` 3.1 dataset](../contracts/estimativa_safra.md) can select this source with `fonte="ibge_lspa"` or `mes`, preserving `ano_lspa` and `mes_lspa`. Its `safra="2024/25"` parameter selects the final calendar year, 2025; this crop-year label is not a native LSPA dimension. Without a month, the dataset selects the latest observed period, while the source API preserves the available monthly series.

The dataset combines expected maize and bean crop components, converts hectares/tonnes to thousand ha/thousand tonnes and recalculates yield from totals. Missing components, duplicates or incompatible units/localities are rejected; NA values remain missing. Do not use a CONAB `levantamento` as an LSPA month or sum monthly estimates as production flows.

### PPM - Municipal Livestock Survey

- **SIDRA tables**: 3939 (herds), 74 (animal-origin production)
- **Coverage**: All municipalities
- **Frequency**: Annual
- **Series**: 1974-present (51 years)

### Slaughter - Quarterly Animal Slaughter Survey

- **SIDRA tables**: 1092 (cattle), 1093 (hogs), 1094 (chickens)
- **Coverage**: 27 states (no Brazil row; for the national total, use the species table in SIDRA at the Brazil level)
- **Frequency**: Quarterly
- **Series**: 1997-present
- **Species**: cattle, hog, chicken
- **Variables**: animals slaughtered (head), carcass weight (kg)

### Agricultural Census 1995/2006/2017

- **SIDRA tables 2017**: 6907 (livestock numbers), 6881 (land use), 6957 (temporary crops), 6956 (permanent crops), 6855 (soil preparation), 6848 (fertilization), 6849 (liming), 6851 (pesticides), 8561 (agricultural practices), 6857 (irrigation), 6899 (fertilizer expenses)
- **SIDRA tables 2006**: 791 (soil preparation), 1249 (fertilization), 1245 (liming), 1459 (pesticides), 837 (agricultural practices), 855 (irrigation)
- **SIDRA tables 1995**: 323 (livestock numbers), 316/311 (land use), 497/492/503 (temporary crops), 509/504/510 (permanent crops)
- **Coverage**: Brazil + state + municipality
- **Frequency**: Decennial
- **Periods**: 1995, 2006 and 2017 (by theme)
- **Themes**: efetivo_rebanho, uso_terra, lavoura_temporaria, lavoura_permanente, preparo_solo, adubacao, calagem, agrotoxicos, praticas_agricolas, irrigacao, despesa_adubos
- **Format**: Long format (variable/value per row)
- **Total row**: published as the source does (`categoria = "Total"`), not added to the other categories;
  `estabelecimentos` does not add up across categories

### Agricultural Census — Historical Series (1920-2006)

- **SIDRA tables**: 263 (establishments/area), 264 (land use), 265 (personnel/tractors), 280 (producer status), 281 (animal numbers), 282 (animal production), 283 (crop production), 1730 (permanent crops), 1731 (temporary crops)
- **Coverage**: Brazil + region + state (municipal NOT available in SIDRA)
- **Frequency**: Decennial censuses (1920-2006, by table)
- **Periods**: up to 10 censuses per theme (1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006)
- **Themes**: 9 themes with a long historical series
- **Quirks**: Poultry in thousand head (tab 281), mixed units by category (tabs 282/283/1730/1731), classifications without Total (tabs 281/282/283/1730/1731)

### Agricultural Census 1985 — Municipal Data (IBGE PDFs)

- **Source**: the IBGE Library's 28 state PDFs (27 states; Minas Gerais in 2 volumes). MA, PI, CE and RN use the version the IBGE
  republished on 2018-09-03, with a text layer.
- **Format**: agrobr package in Parquet (`agrobr/data/censo_1985/`), 1 row per PDF cell, extracted from the text layer,
  with RapidOCR as the 2nd reading; the manifest keeps each PDF's SHA-256.
- **Coverage**: 27 states, down to municipality (mesoregion, microregion, municipality); 85.8% of the cells read have
  an identified column (the per-volume list is in the contract).
- **Frequency**: One-off (1985 Census)
- **Themes**: 53 themes, 1 per table (67 to 119), from the printed title
- **Confidence**: `valor` only in cells confirmed by the printed sums (0 errors in the measured precision); `valor_lido` and the
  `status` for the rest, with the precision measured in the [contract](../contracts/censo_agropecuario_municipal_1985.md)
- **Access**: local, no network
- **Catalog URL**: https://biblioteca.ibge.gov.br/index.php/biblioteca-catalogo?view=detalhes&id=768

### Agricultural Census 1995/96 — Legacy Themes (FTP)

- **Source**: IBGE FTP (`ftp.ibge.gov.br`)
- **Format**: ZIP archives containing legacy XLS (BIFF5/BIFF8) or HTML
- **Coverage**: Brazil, actual state totals, and municipalities; `uf` distinguishes municipalities with identical names
- **Contract**: [Legacy Census 2.1](../contracts/censo_agropecuario_legado.md), with categories, variables, and units from official headers
- **Frequency**: One-off (1995/96 Census)
- **Themes**: tecnologia, pessoal_ocupado, maquinas, producao_animal, valor_producao, financeiro
- **Access**: Public, no authentication

Of the six themes across the 27 states, 161 combinations have data and one, machinery in PA, is rejected: the
machinery file
[Pará/Tab_7Mn.zip](https://ftp.ibge.gov.br/Censo_Agropecuario/Censo_Agropecuario_1995_96/Para/Tab_7Mn.zip)
contains the same bytes as the personnel table, and none of Pará's 11
`Tab_*Mn` files carries Table 7. The machinery query
with `uf='PA'` raises `SourceUnavailableError`, and the query without `uf` returns
the other 26 states, with the warning in `MetaInfo` and a `UserWarning`; the theme
is not replaced with personnel data or a table with a different level of aggregation.

Sergipe's BIFF8 machinery headers distinguish planting, harvesting, trucks and
utility vehicles even when text boxes overlap. State values are not rebuilt
by summing municipalities.

### PEVS — Silviculture

- **SIDRA tables**: 291 (production, classification c194) + 5930 (planted area, classification c734)
- **Coverage**: All municipalities
- **Frequency**: Annual
- **Series**: 1986-present
- **Products**: carvao, lenha, madeira_tora, madeira_celulose, acacia_negra, eucalipto_folha, resina (14 total)
- **Area species**: eucalipto, pinus, outras
- **Variables**: quantidade_produzida (var 142), valor_producao (var 143), area (var 6549)
- **Units**: the SIDRA response's `MN` field, according to variable and period; physical production in tonnes or cubic meters, area in hectares, and production value in currency (for example, `Mil Reais` in 2023)

### PEVS — Plant Extraction

- **SIDRA table**: 289 (classification c193)
- **Coverage**: All municipalities
- **Frequency**: Annual
- **Series**: 1986-present
- **Products**: acai, castanha_caju, castanha_para, erva_mate, mangaba, palmito, pequi_fruto, pinhao, umbu, hevea_coagulado, hevea_liquido, carnauba_cera, carnauba_po, piacava, carvao, lenha, madeira_tora, babacu, copaiba, cumaru, pequi_amendoa (21 total)
- **Variables**: quantidade_produzida (var 144), valor_producao (var 145)
- **Units**: the SIDRA response's `MN` field; quantity in tonnes or cubic meters and production value in currency (for example, `Mil Reais` in 2023)

### Quarterly Milk — Quarterly Milk Survey

- **SIDRA table**: 1086
- **Coverage**: 27 states (no Brazil row; for the national total, use SIDRA table 1086 at the Brazil level)
- **Frequency**: Quarterly
- **Series**: 1997-present
- **Variables**: milk acquired (var 282, thousand liters), milk industrialized (var 283, thousand liters), average price (var 2522, R$/liter)
- **Output**: Wide format (3 variables as columns)

### Agricultural GDP — Quarterly National Accounts

- **SIDRA tables**: 1846 (current prices, var 585) + 6612 (real prices, 1995 base, var 9318)
- **Coverage**: Brazil (national level)
- **Frequency**: Quarterly
- **Series**: 1996-present
- **Sectors**: agropecuaria (90687), industria (90691), servicos (90696), pib_total (90707)
- **Classification**: c11255
- **Unit**: Millions of Reais

## Variables

| Code | Name | Unit |
|--------|------|---------|
| 214 | Quantity produced | tonnes |
| 215 | Production value | thousand BRL |
| 216 | Harvested area | hectares |
| 8331 | Planted area | hectares |
| 112 | Average yield | kg/ha |

## Usage - PAM

### Basic

```python
import asyncio
from agrobr import ibge

async def main():
    # Soybean data by state
    df = await ibge.pam('soja', ano=2023)

    # Multiple years
    df = await ibge.pam('milho', ano=[2020, 2021, 2022, 2023])

    # Filter by state
    df = await ibge.pam('soja', ano=2023, uf='MT')

    # Municipality level
    df = await ibge.pam('arroz', ano=2023, nivel='municipio', uf='RS')

    # With metadata
    df, meta = await ibge.pam('soja', ano=2023, return_meta=True)

asyncio.run(main())
```

### Territorial Levels

| Level | Description |
|-------|-----------|
| `brasil` | National total |
| `uf` | By Federative Unit |
| `municipio` | By municipality |

## Usage - LSPA

### Basic

```python
# Estimates for the year
df = await ibge.lspa('soja', ano=2024)

# Specific month
df = await ibge.lspa('milho_1', ano=2024, mes=6)

# Filter by state
df = await ibge.lspa('soja', ano=2024, uf='PR')

# With metadata
df, meta = await ibge.lspa('soja', ano=2024, return_meta=True)
```

## Schema - PAM

| Column | Type | Description |
|--------|------|-----------|
| `ano` | int | Reference year |
| `localidade` | str | Locality name |
| `localidade_cod` | int | IBGE locality code (SIDRA D1C) |
| `produto` | str | Product name |
| `area_plantada` | float | Hectares |
| `area_colhida` | float | Hectares |
| `producao` | float | See `unidade_producao` |
| `rendimento` | float | See `unidade_rendimento` |
| `valor_producao` | float | See `unidade_valor_producao` |
| `fonte` | str | "ibge_pam" |
| `unidade_producao` | str | `ton`; oranges before 2001, `mil_frutos` |
| `unidade_rendimento` | str | `kg/ha`; oranges before 2001, `frutos/ha` |
| `unidade_valor_producao` | str | `mil_reais`; before 1994, the currency of the time (`mil_cruzeiros`, `mil_cruzados`, etc.) |
| `condicao_produto` | str | Coffee: `em_coco` through 2001, `beneficiado` since 2002; null for other products |

## Schema - LSPA

| Column | Type | Description |
|--------|------|-----------|
| `ano` | int | Reference year |
| `mes` | int | Reference month |
| `localidade` | str | Locality name |
| `localidade_cod` | int | IBGE locality code (SIDRA D1C) |
| `variavel` | str | Variable name |
| `variavel_cod` | int | SIDRA variable code |
| `valor` | float | Variable value |
| `unidade` | str | Unit published by SIDRA |
| `produto` | str | Product name |
| `fonte` | str | "ibge_lspa" |

## PAM Products

```python
produtos = await ibge.produtos_pam()
# ['soja', 'milho', 'arroz', 'feijao', 'trigo', 'cafe', ...]
```

## LSPA Products

```python
produtos = await ibge.produtos_lspa()
# ['soja', 'milho_1', 'milho_2', 'arroz', 'feijao_1', 'feijao_2', ...]
```

Note: In LSPA, `milho_1` and `milho_2` refer to the first and second maize crops within the same calendar year. The source API's `milho` alias returns the components separately; aggregation takes place in the dataset.

## Available States

```python
ufs = await ibge.ufs()
# ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', ...]
```

## Usage - PPM

### Basic

```python
import asyncio
from agrobr import ibge

async def main():
    # Cattle herd by state
    df = await ibge.ppm('bovino', ano=2023)

    # Milk production
    df = await ibge.ppm('leite', ano=2023)

    # Multiple years
    df = await ibge.ppm('bovino', ano=[2020, 2021, 2022, 2023])

    # Filter by state
    df = await ibge.ppm('bovino', ano=2023, uf='MT')

    # Municipality level
    df = await ibge.ppm('bovino', ano=2023, nivel='municipio', uf='MS')

    # With metadata
    df, meta = await ibge.ppm('bovino', ano=2023, return_meta=True)

asyncio.run(main())
```

## Schema - PPM

| Column | Type | Description |
|--------|------|-----------|
| `ano` | int | Reference year |
| `localidade` | str | Locality name |
| `localidade_cod` | int | IBGE code of the locality |
| `especie` | str | Species/product name |
| `valor` | float | Value (head, thousand liters, etc.) |
| `unidade` | str | Unit of measure |
| `fonte` | str | "ibge_ppm" |

## PPM Species/Products

### Herds (table 3939)

| Code | Species | Unit |
|--------|---------|---------|
| `bovino` | Cattle | head |
| `bubalino` | Buffalo | head |
| `equino` | Equine | head |
| `suino_total` | Hog (total) | head |
| `suino_matrizes` | Breeding sows | head |
| `caprino` | Goat | head |
| `ovino` | Sheep | head |
| `galinaceos_total` | Poultry (total) | head |
| `galinhas` | Hens (laying and breeder hens) | head |
| `codornas` | Quails | head |

`galinhas_poedeiras`: deprecated alias of `galinhas` (`FutureWarning`).

### Animal-origin production (table 74)

| Code | Product | Unit |
|--------|---------|---------|
| `leite` | Milk | thousand liters |
| `ovos_galinha` | Hen eggs | thousand dozens |
| `ovos_codorna` | Quail eggs | thousand dozens |
| `mel` | Bee honey | kg |
| `casulos` | Silkworm cocoons | kg |
| `la` | Wool | kg |

```python
especies = await ibge.especies_ppm()
# ['bovino', 'bubalino', 'caprino', 'casulos', 'codornas', ...]
```

## Usage - Quarterly Slaughter

### Basic

```python
import asyncio
from agrobr import ibge

async def main():
    # Cattle slaughter by state
    df = await ibge.abate('bovino', trimestre='202303')

    # Chicken slaughter in Paraná
    df = await ibge.abate('frango', trimestre='202303', uf='PR')

    # Hog slaughter — all states
    df = await ibge.abate('suino', trimestre='202304')

    # With metadata
    df, meta = await ibge.abate('bovino', trimestre='202303', return_meta=True)

asyncio.run(main())
```

## Schema - Quarterly Slaughter

| Column | Type | Description |
|--------|------|-----------|
| `trimestre` | str | Quarter in YYYYQQ format |
| `localidade` | str | State |
| `localidade_cod` | int | IBGE code of the locality |
| `especie` | str | bovino, suino or frango |
| `animais_abatidos` | Int64 | Quantity slaughtered (head) |
| `peso_carcacas` | float | Total carcass weight (kg) |
| `fonte` | str | "ibge_abate" |

## Slaughter Species

| Code | Species | SIDRA Table |
|--------|---------|--------------|
| `bovino` | Cattle | 1092 |
| `suino` | Hog | 1093 |
| `frango` | Chicken | 1094 |

```python
especies = await ibge.especies_abate()
# ['bovino', 'suino', 'frango']
```

## Usage - Agricultural Census

### Basic

```python
import asyncio
from agrobr import ibge

async def main():
    # Livestock numbers by state
    df = await ibge.censo_agro('efetivo_rebanho')

    # Land use in Mato Grosso
    df = await ibge.censo_agro('uso_terra', uf='MT')

    # Temporary crops by municipality
    df = await ibge.censo_agro('lavoura_temporaria', nivel='municipio', uf='PR')

    # Permanent crops — Brazil
    df = await ibge.censo_agro('lavoura_permanente', nivel='brasil')

    # With metadata
    df, meta = await ibge.censo_agro('efetivo_rebanho', return_meta=True)

asyncio.run(main())
```

## Schema - Agricultural Census

| Column | Type | Description |
|--------|------|-----------|
| `ano` | int | Reference year (1995, 2006 or 2017) |
| `localidade` | str | Locality name |
| `localidade_cod` | int | IBGE code of the locality |
| `tema` | str | Census theme |
| `categoria` | str | Category within the theme |
| `variavel` | str | Variable name |
| `valor` | float | Variable value |
| `unidade` | str | Unit of measure |
| `fonte` | str | "ibge_censo_agro" |

## Agricultural Census Themes

| Code | Theme | SIDRA Table 1995 | SIDRA Table 2006 | SIDRA Table 2017 |
|--------|------|-------------------|-------------------|-------------------|
| `efetivo_rebanho` | Livestock numbers | 323 | — | 6907 |
| `uso_terra` | Land use | 316/311 | — | 6881 |
| `lavoura_temporaria` | Temporary crops | 497/492/503 | — | 6957 |
| `lavoura_permanente` | Permanent crops | 509/504/510 | — | 6956 |
| `preparo_solo` | Soil preparation | — | 791 | 6855 |
| `adubacao` | Fertilization | — | 1249 | 6848 |
| `calagem` | Liming | — | 1245 | 6849 |
| `agrotoxicos` | Pesticide use | — | 1459 | 6851 |
| `praticas_agricolas` | Agricultural practices | — | 837 | 8561 |
| `irrigacao` | Irrigation | — | 855 | 6857 |

```python
temas = await ibge.temas_censo_agro()
# ['efetivo_rebanho', 'uso_terra', 'lavoura_temporaria', 'lavoura_permanente',
#  'preparo_solo', 'adubacao', 'calagem', 'agrotoxicos', 'praticas_agricolas', 'irrigacao',
#  'despesa_adubos']
```

## Cache

There is no local cache: every call queries IBGE, and `MetaInfo` comes with `from_cache=False` and a null `cache_expires_at`.

## Update

| Survey | Frequency |
|----------|------------|
| PAM | Annual (August-September) |
| LSPA | Monthly |
| PPM | Annual (September) |
| Slaughter | Quarterly (T+2 months) |
| Agricultural Census | Decennial (latest: 2017) |
| Silviculture (PEVS) | Annual (August-September) |
| Plant Extraction (PEVS) | Annual (August-September) |
| Quarterly Milk | Quarterly (T+2 months) |
| Agricultural GDP | Quarterly (T+2 months) |

## Usage - Silviculture (PEVS)

### Basic

```python
import asyncio
from agrobr import ibge

async def main():
    # Roundwood production by state
    df = await ibge.silvicultura('madeira_tora', ano=2023)

    # Planted area of eucalyptus
    df = await ibge.silvicultura('eucalipto', variavel='area')

    # Charcoal in MG
    df = await ibge.silvicultura('carvao', ano=2023, uf='MG')

    # With metadata
    df, meta = await ibge.silvicultura('madeira_tora', return_meta=True)

asyncio.run(main())
```

## Schema - Silviculture

| Column | Type | Description |
|--------|------|-----------|
| `ano` | int | Reference year |
| `localidade` | str | Locality name |
| `localidade_cod` | int | IBGE code of the locality |
| `produto` | str | Product name |
| `valor` | float | Value (Tonnes, cubic meters or Hectares) |
| `unidade` | str | Unit of measure |
| `fonte` | str | "ibge_silvicultura" |

## Usage - Plant Extraction (PEVS)

### Basic

```python
import asyncio
from agrobr import ibge

async def main():
    # Açaí production by state
    df = await ibge.extracao_vegetal('acai', ano=2023)

    # Brazil nut in Amazonas
    df = await ibge.extracao_vegetal('castanha_para', ano=2023, uf='AM')

    # Production value
    df = await ibge.extracao_vegetal('acai', variavel='valor_producao')

    # With metadata
    df, meta = await ibge.extracao_vegetal('acai', return_meta=True)

asyncio.run(main())
```

## Schema - Plant Extraction

| Column | Type | Description |
|--------|------|-----------|
| `ano` | int | Reference year |
| `localidade` | str | Locality name |
| `localidade_cod` | int | IBGE code of the locality |
| `produto` | str | Product name |
| `valor` | float | Value (Tonnes or cubic meters) |
| `unidade` | str | Unit of measure |
| `fonte` | str | "ibge_extracao_vegetal" |

## Usage - Quarterly Milk

### Basic

```python
import asyncio
from agrobr import ibge

async def main():
    # Quarterly milk by state
    df = await ibge.leite_trimestral(trimestre='202303')

    # Filter by state
    df = await ibge.leite_trimestral(trimestre='202303', uf='MG')

    # Multiple quarters
    df = await ibge.leite_trimestral(trimestre=['202301', '202302', '202303'])

    # With metadata
    df, meta = await ibge.leite_trimestral(return_meta=True)

asyncio.run(main())
```

## Schema - Quarterly Milk

| Column | Type | Description |
|--------|------|-----------|
| `trimestre` | str | Quarter YYYYQQ |
| `localidade` | str | State |
| `localidade_cod` | int | IBGE code of the locality |
| `leite_adquirido` | float | Raw milk acquired (thousand liters) |
| `leite_industrializado` | float | Raw milk industrialized (thousand liters) |
| `preco_medio` | float | Average price paid to the producer (R$/liter) |
| `fonte` | str | "ibge_leite_trimestral" |

## Usage - Agricultural GDP

### Basic

```python
import asyncio
from agrobr import ibge

async def main():
    # Agricultural GDP at current prices
    df = await ibge.pib_agro(trimestre='202501')

    # GDP at real prices (1995 base)
    df = await ibge.pib_agro(trimestre='202501', precos='real_1995')

    # Total GDP
    df = await ibge.pib_agro(trimestre='202501', setor='pib_total')

    # With metadata
    df, meta = await ibge.pib_agro(return_meta=True)

asyncio.run(main())
```

## Schema - Agricultural GDP

| Column | Type | Description |
|--------|------|-----------|
| `trimestre` | str | Quarter YYYYQQ |
| `valor` | float | Value (Millions of Reais) |
| `unidade` | str | Unit of measure |
| `setor` | str | Economic sector |
| `fonte` | str | "ibge_pib" |

## Municipal mesh and urbanized areas (geoservices)

Outside SIDRA, the module reads 2 layers from the IBGE geoservices WFS (GeoServer, WFS 2.0.0, GeoJSON output):

| Function | Layer | Edition | Features |
|----------|-------|---------|----------|
| `malha_municipal` / `malha_municipal_geo` | `CGMAT:qg_2025_030_munic`, at `https://geoservicos.ibge.gov.br/geoserverIBGE/wfs` | 2025 municipal mesh | 5,573: the 5,571 municipalities and 2 operational lagoon areas in RS |
| `areas_urbanizadas` / `areas_urbanizadas_geo` | `CGEO:AU_2026_AreasUrbanizadas2022_Brasil`, at `https://geoservicos.ibge.gov.br/geoserverCGEO/wfs` | Urbanized Areas of Brazil 2022 | 190,172 polygons, with no state or municipality |

- The count (`resultType=hits`) runs before the download; above the query cap, `ResourceLimitError` without downloading
  anything, and what arrives is reconciled with the count, as in CNUC and ICMBio.
- Filters run on the server only as equality (`sigla_uf`, `cd_mun`) and `BBOX`. The IBGE firewall rejects CQL with `OR` or
  `NOT LIKE` through an HTML "Request Rejected" page with status 200, which agrobr raises as `SourceUnavailableError`.
- Geometry comes in EPSG:4326, requested from the server (`srsName`), at the layer's original resolution.
- The IBGE mesh API (`servicodados.ibge.gov.br/api/v3/malhas`) is not used: its geometry is generalized for the web
  (Brasília with 840 vertices, against 9,896 in the WFS) and it only carries the area code.
- As files, the mesh ZIPs per state and for Brazil are on the
  [IBGE geoftp](https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2025/),
  and the urbanized areas shapefile is at `organizacao_do_territorio/tipologias_do_territorio/areas_urbanizadas_do_brasil/2022/Shapefile/`
  on the same server.
- License: IBGE, `livre` (see [licenses](../licenses.md)).
- Columns, limits and examples: [IBGE API](../api/ibge.md#malha_municipal-malha_municipal_geo).

## Limits and errors

Bursts of SIDRA queries may trigger a Cloudflare anti-bot check (`challenge`). When a 403 response contains `cf-mitigated: challenge` or the “Just a moment” page, the query raises `SourceUnavailableError` with a `Cloudflare challenge` message and guidance to reduce the request rate. Wait before trying again; this 403 response is not retried automatically.

## Notes

- PEVS Silviculture: 14 products, annual data since 1986. Planted area (tab 5930) with 3 species
- PEVS Plant Extraction: 21 products, annual data since 1986. Mixed units (Tonnes vs cubic meters)
- Quarterly Milk: table 1086, 3 variables pivoted into wide columns. Series since 1997
- Agricultural GDP: tabs 1846/6612, 4 sectors, Brazil level. Series since 1996. `pib_agro` contract 1.0 (`datasets.pib_agro` dataset)

## Periods and historical coverage

PAM before 1988 may omit planted area. `datasets.producao_anual` represents that absence with nullable `Float64`; production value also remains null when not requested. Other measures are not automatically supplied, so source changes remain visible.

PPM rejects future years with `InvalidParameterError`, including through `datasets.pecuaria_municipal`. Slaughter, quarterly milk, and agricultural GDP accept `2025-4`, `2025-T4`, `2025T4`, `2025/4`, and `2025Q4`, normalized to `202504`. Invalid formats are rejected before network access.

## PAM units and historical breaks

Published values are not implicitly converted. `unidade_producao`, `unidade_rendimento`, and `unidade_valor_producao` identify each row's scale. Before 2001, oranges use `mil_frutos` and `frutos/ha`; from 2001 onward, `ton` and `kg/ha`. `condicao_produto` distinguishes coffee `em_coco` through 2001 from `beneficiado` since 2002. Historical currencies remain identified without conversion to BRL or inflation adjustment. See the [IBGE methodology notes](https://sidra.ibge.gov.br/pesquisa/pam/tabelas/).

`localidade_cod` (`producao_anual` contract 2.2) carries the locality's IBGE code as SIDRA publishes it (D1C): 7 digits for a municipality, 2 for a state and 1 for Brazil. Use it to join municipalities across years, because the published name changes (and the Federal District comes out as "Brasília (DF)", without the " - UF" suffix of the others). In `producao_anual`, only IBGE rows carry the code; the CONAB fallback does not.

SIDRA's `-` symbol means numeric zero and remains zero; `..`, `...`, and `X` remain missing. Municipalities with zero production are retained. The `producao_anual` contract is 2.2; the four descriptive columns and `localidade_cod` are optional in the contract and supplied by the PAM API.
