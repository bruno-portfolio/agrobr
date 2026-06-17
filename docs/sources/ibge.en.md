# IBGE - Brazilian Institute of Geography and Statistics

## Overview

| Field | Value |
|-------|-------|
| **Institution** | Federal Government |
| **Website** | [ibge.gov.br](https://www.ibge.gov.br) |
| **API** | [SIDRA](https://sidra.ibge.gov.br) |
| **agrobr access** | Via SIDRA API (JSON) |

## Data Origin

### Source

- **API**: `https://sidra.ibge.gov.br/`
- **Format**: JSON
- **Access**: Public, no authentication

## Available Surveys

### PAM - Municipal Agricultural Production

- **SIDRA table**: 5457 (new series 2018+)
- **Coverage**: All municipalities
- **Frequency**: Annual

### LSPA - Systematic Survey of Agricultural Production

- **SIDRA table**: 6588
- **Coverage**: National/state
- **Frequency**: Monthly

### PPM - Municipal Livestock Survey

- **SIDRA tables**: 3939 (herds), 74 (animal-origin production)
- **Coverage**: All municipalities
- **Frequency**: Annual
- **Series**: 1974-present (51 years)

### Slaughter - Quarterly Animal Slaughter Survey

- **SIDRA tables**: 1092 (cattle), 1093 (hogs), 1094 (chickens)
- **Coverage**: Brazil + states (27 states)
- **Frequency**: Quarterly
- **Series**: 1997-present
- **Species**: cattle, hog, chicken
- **Variables**: animals slaughtered (head), carcass weight (kg)

### Agricultural Census 1995/2006/2017

- **SIDRA tables 2017**: 6907 (livestock numbers), 6881 (land use), 6957 (temporary crops), 6956 (permanent crops), 6855 (soil preparation), 6848 (fertilization), 6849 (liming), 6851 (pesticides), 8561 (agricultural practices), 6857 (irrigation)
- **SIDRA tables 2006**: 791 (soil preparation), 1249 (fertilization), 1245 (liming), 1459 (pesticides), 837 (agricultural practices), 855 (irrigation)
- **SIDRA tables 1995**: 323 (livestock numbers), 316/311 (land use), 497/492/503 (temporary crops), 509/504/510 (permanent crops)
- **Coverage**: Brazil + state + municipality
- **Frequency**: Decennial
- **Periods**: 1995, 2006 and 2017 (by theme)
- **Themes**: efetivo_rebanho, uso_terra, lavoura_temporaria, lavoura_permanente, preparo_solo, adubacao, calagem, agrotoxicos, praticas_agricolas, irrigacao
- **Format**: Long format (variable/value per row)

### Agricultural Census — Historical Series (1920-2006)

- **SIDRA tables**: 263 (establishments/area), 264 (land use), 265 (personnel/tractors), 280 (producer status), 281 (animal numbers), 282 (animal production), 283 (crop production), 1730 (permanent crops), 1731 (temporary crops)
- **Coverage**: Brazil + region + state (municipal NOT available in SIDRA)
- **Frequency**: Decennial censuses (1920-2006, by table)
- **Periods**: up to 10 censuses per theme (1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006)
- **Themes**: 9 themes with a long historical series
- **Quirks**: Poultry in thousand head (tab 281), mixed units by category (tabs 282/283/1730/1731), classifications without Total (tabs 281/282/283/1730/1731)

### Agricultural Census 1985 — Municipal Data (OCR PDFs)

- **Source**: State PDFs from the IBGE Library
- **Format**: CSVs extracted via hybrid OCR (PyMuPDF coords + OCR correction)
- **Coverage**: 22 states, down to municipality (mesoregion, microregion, municipality)
- **Frequency**: One-off (1985 Census)
- **Themes**: 53 themes (property, land use, personnel, mechanization, livestock, crops, production)
- **Excluded states**: MA, PI, CE, RN (PDFs without an OCR layer)
- **Access**: Data bundled in the package (agrobr/data/censo_1985/)
- **Quality**: `confianca` field (alta/media/baixa), 77.9% state↔national cross-validation
- **Catalog URL**: https://biblioteca.ibge.gov.br/index.php/biblioteca-catalogo?view=detalhes&id=768

### Agricultural Census 1995/96 — Legacy Themes (FTP)

- **Source**: IBGE FTP (`ftp.ibge.gov.br`)
- **Format**: Legacy XLS (xlrd)
- **Coverage**: Brazil (mesoregions, microregions, municipalities)
- **Frequency**: One-off (1995/96 Census)
- **Themes**: tecnologia, pessoal_ocupado, maquinas, producao_animal, valor_producao, financeiro
- **Access**: Public, no authentication

### PEVS — Silviculture

- **SIDRA tables**: 291 (production, classification c194) + 5930 (planted area, classification c734)
- **Coverage**: All municipalities
- **Frequency**: Annual
- **Series**: 1986-present
- **Products**: carvao, lenha, madeira_tora, madeira_celulose, acacia_negra, eucalipto_folha, resina (14 total)
- **Area species**: eucalipto, pinus, outras
- **Variables**: quantidade_produzida (var 142), valor_producao (var 143), area (var 6549)
- **Units**: Tonnes or cubic meters (by product)

### PEVS — Plant Extraction

- **SIDRA table**: 289 (classification c193)
- **Coverage**: All municipalities
- **Frequency**: Annual
- **Series**: 1986-present
- **Products**: acai, castanha_caju, castanha_para, erva_mate, mangaba, palmito, pequi_fruto, pinhao, umbu, hevea_coagulado, hevea_liquido, carnauba_cera, carnauba_po, piacava, carvao, lenha, madeira_tora, babacu, copaiba, cumaru, pequi_amendoa (21 total)
- **Variables**: quantidade_produzida (var 144), valor_producao (var 145)
- **Units**: Tonnes (most) or cubic meters (lenha, madeira_tora)

### Quarterly Milk — Quarterly Milk Survey

- **SIDRA table**: 1086
- **Coverage**: Brazil + states (27 states)
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
| 214 | Planted area | hectares |
| 215 | Harvested area | hectares |
| 216 | Quantity produced | tonnes |
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
| `produto` | str | Product name |
| `area_plantada` | float | Hectares |
| `area_colhida` | float | Hectares |
| `producao` | float | Tonnes |
| `rendimento` | float | kg/ha |
| `valor_producao` | float | Thousand reais |
| `fonte` | str | "ibge_pam" |

## Schema - LSPA

| Column | Type | Description |
|--------|------|-----------|
| `ano` | int | Reference year |
| `mes` | int | Reference month |
| `variavel` | str | Variable name |
| `valor` | float | Variable value |
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

Note: In LSPA, `milho_1` and `milho_2` refer to the first and second crop years.

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
| `galinhas_poedeiras` | Laying hens | head |
| `codornas` | Quails | head |

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

    # Hog slaughter — Brazil
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
| `animais_abatidos` | float | Quantity slaughtered (head) |
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
    df = await ibge.censo_agro('lavoura_permanente')

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
#  'preparo_solo', 'adubacao', 'calagem', 'agrotoxicos', 'praticas_agricolas', 'irrigacao']
```

## Cache

| Survey | TTL | Maximum stale |
|----------|-----|--------------|
| PAM | 7 days | 90 days |
| LSPA | 24 hours | 30 days |
| PPM | 7 days | 90 days |
| Slaughter | 7 days | 90 days |
| Agricultural Census | 30 days | 90 days |
| Legacy Agricultural Census | 90 days | 90 days |
| Silviculture (PEVS) | 7 days | 90 days |
| Plant Extraction (PEVS) | 7 days | 90 days |
| Quarterly Milk | 7 days | 90 days |
| Agricultural GDP | 7 days | 90 days |

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

## Notes

- PEVS Silviculture: 14 products, annual data since 1986. Planted area (tab 5930) with 3 species. Cache 7 days
- PEVS Plant Extraction: 21 products, annual data since 1986. Mixed units (Tonnes vs cubic meters). Cache 7 days
- Quarterly Milk: table 1086, 3 variables pivoted into wide columns. Series since 1997. Cache 7 days
- Agricultural GDP: tabs 1846/6612, 4 sectors, Brazil level. Series since 1996. No contract (macro view). Cache 7 days
