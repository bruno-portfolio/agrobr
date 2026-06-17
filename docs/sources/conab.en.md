# CONAB - Companhia Nacional de Abastecimento

## Overview

| Field | Value |
|-------|-------|
| **Institution** | Ministério da Agricultura |
| **Website** | [conab.gov.br](https://www.conab.gov.br) |
| **agrobr access** | Direct (public XLSX) |

## Data Origin

### Source

- **URL**: `https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/safra-de-graos/boletim-da-safra-de-graos`
- **Format**: XLSX (Excel spreadsheets)
- **Access**: Public, no restrictions

## Surveys

CONAB publishes monthly crop surveys:

| Month | Survey |
|-----|--------------|
| October | 1st Survey |
| November | 2nd Survey |
| December | 3rd Survey |
| January | 4th Survey |
| February | 5th Survey |
| March | 6th Survey |
| April | 7th Survey |
| May | 8th Survey |
| June | 9th Survey |
| July | 10th Survey |
| August | 11th Survey |
| September | 12th Survey |

## Available Data

### Crops

- Planted area (thousand hectares)
- Harvested area (thousand hectares)
- Yield (kg/ha)
- Production (thousand tonnes)

### Supply and Demand Balance

- Opening stock
- Production
- Imports
- Consumption
- Exports
- Closing stock

## Usage

### Crops by Product

```python
import asyncio
from agrobr import conab

async def main():
    # Soybean crop data
    df = await conab.safras('soja')

    # Specific crop year
    df = await conab.safras('milho', safra='2025/26')

    # Filter by state
    df = await conab.safras('soja', uf='MT')

    # With metadata
    df, meta = await conab.safras('soja', return_meta=True)

asyncio.run(main())
```

### Supply/Demand Balance

```python
# Balance for all products
df = await conab.balanco()

# Balance for a specific product
df = await conab.balanco(produto='soja')
```

### Brazil Totals

```python
# National totals by product
df = await conab.brasil_total()
```

## Schema - Crops

| Column | Type | Description |
|--------|------|-----------|
| `fonte` | str | "conab" |
| `produto` | str | Product name |
| `safra` | str | Crop year (e.g. "2024/25") |
| `uf` | str | State code |
| `area_plantada` | Decimal | Thousand hectares |
| `area_colhida` | Decimal | Thousand hectares |
| `produtividade` | Decimal | kg/ha |
| `producao` | Decimal | Thousand tonnes |
| `levantamento` | int | Survey number (1-12) |
| `data_publicacao` | date | Publication date |

## Available Products

```python
produtos = await conab.produtos()
# ['soja', 'milho', 'arroz', 'feijao', 'algodao', 'trigo', ...]
```

## Available States

```python
ufs = await conab.ufs()
# ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', ...]
```

## Available Surveys

```python
levs = await conab.levantamentos()
for lev in levs[:5]:
    print(f"{lev['safra']} - survey #{lev['levantamento']}")
```

## Production Cost

Production cost per hectare spreadsheets, published on `gov.br/conab`.

```python
# Detailed soybean cost in MT
df = await conab.custo_producao("soja", uf="MT")

# Totals (COE, COT, CT)
totais = await conab.custo_producao_total("soja", uf="MT", safra="2024/25")
```

### Schema - custo_producao

| Column | Type | Description |
|--------|------|-----------|
| `cultura` | str | Crop name |
| `uf` | str | State code |
| `safra` | str | Crop year (e.g. "2024/25") |
| `item` | str | Cost item |
| `categoria` | str | Category (insumos, operacoes, mao_de_obra, etc) |
| `valor_ha` | float | Value per hectare (R$/ha) |
| `unidade` | str | Item unit |

### Status (Mar/2026)

The grain spreadsheets (soja, milho, cafe, algodao) on gov.br are loaded
via dynamic JavaScript and do not have .xlsx links accessible via scraping.
Special crops (abacaxi, banana, cebola, cacau, tomate, etc.) work normally.

The parser (v2) supports 4 spreadsheet formats:
- **Standard format** — header in 1 row (Item/Especificacao, Unidade, Valor Total/ha)
- **Format A** — header split across 2 rows (DISCRIMINACAO + R$/ha in separate rows)
- **Format C** — compact 1-row header (DISCRIMINACAO, CUSTO POR HA, CUSTO/kg)
- **Format D** — header split across 3 rows (A PRECOS DE + DISCRIMINACAO + R$/ha). Detection via best-quality selection among 2-row candidates

## Historical Series (v0.8.0)

Historical crop data since ~1976, published in Excel spreadsheets (.xls legacy).
The parser automatically detects the format (OLE2/BIFF → xlrd, OOXML → openpyxl with calamine fallback).

```python
# Soybean historical series
df = await conab.serie_historica("soja", inicio=2020, fim=2025)

# Filter by state
df = await conab.serie_historica("soja", inicio=2020, uf="MT")
```

### Schema - serie_historica

| Column | Type | Description |
|--------|------|-----------|
| `safra` | str | Crop year (e.g. "2024/25") |
| `produto` | str | Product name |
| `uf` | str | State code |
| `area_plantada` | float | Thousand hectares |
| `producao` | float | Thousand tonnes |
| `produtividade` | float | kg/ha |

## Cache

| Aspect | Value |
|---------|-------|
| **TTL** | 24 hours |
| **Max stale** | 30 days |
| **Policy** | Fixed TTL |

## Update

| Aspect | Value |
|---------|-------|
| **Frequency** | Monthly |
| **Publication** | Generally between days 10-15 |

## Datasets

- [`serie_historica_safra`](../contracts/serie_historica_safra.md) — wraps `conab.serie_historica()` (32 crops)
