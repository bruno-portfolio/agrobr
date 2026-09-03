# CEPEA — Center for Advanced Studies in Applied Economics

> **License:** CEPEA/ESALQ data licensed under
> [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/deed.pt-br).
> Commercial use requires authorization from CEPEA (cepea@usp.br).
> Ref: [Data use license](https://www.cepea.org.br/br/licenca-de-uso-de-dados.aspx)

## Overview

| Field | Value |
|-------|-------|
| **Institution** | ESALQ/USP |
| **Website** | [cepea.org.br](https://www.cepea.org.br) |
| **License** | CC BY-NC 4.0 |
| **agrobr access** | Via Notícias Agrícolas (authorized mirror) |

## Data Origin

### Primary Source

- **Official URL**: `https://www.cepea.org.br/br/indicador/soja.aspx`
- **Status**: Blocked by Cloudflare (programmatic access denied)

### Alternative Source (current)

- **URL**: `https://www.noticiasagricolas.com.br/cotacoes/{produto}/`
- **Type**: Authorized mirror of CEPEA indicators
- **Status**: Working

## Available Products (22 products)

| Product | Main Market | Unit | Frequency |
|---------|-----------------|---------|------------|
| Soybean | Paranagua/PR | BRL/sc 60kg | Daily |
| Soybean Parana | Parana | BRL/sc 60kg | Daily |
| Corn | Campinas/SP | BRL/sc 60kg | Daily |
| Live Cattle | Sao Paulo/SP | BRL/@ | Daily |
| Calf | Mato Grosso do Sul | BRL/@ | Daily |
| Arabica Coffee | Sao Paulo/SP | BRL/sc 60kg | Daily |
| Robusta Coffee | Espirito Santo | BRL/sc 60kg | Daily |
| Wheat | Parana + RS | BRL/ton | Daily |
| Cotton | Sao Paulo/SP | cBRL/lb | Daily |
| Rough rice | ESALQ/BBM | BRL/sc 50kg | Daily |
| Crystal sugar | Sao Paulo/SP | BRL/sc 50kg | Daily |
| Refined sugar | Sao Paulo/SP | BRL/sc 50kg | Daily |
| Hydrous ethanol | Sao Paulo/SP | BRL/L | Weekly |
| Anhydrous ethanol | Sao Paulo/SP | BRL/L | Weekly |
| Frozen chicken | Sao Paulo/SP | BRL/kg | Daily |
| Chilled chicken | Sao Paulo/SP | BRL/kg | Daily |
| Live hog | Sao Paulo/SP | BRL/kg | Daily |
| Milk | At the producer | BRL/L | Monthly |
| Orange (industry) | Sao Paulo/SP | BRL/cx 40,8kg | Daily |
| Orange (fresh) | Sao Paulo/SP | BRL/cx 40,8kg | Daily |

## CEPEA Methodology

CEPEA calculates indicators based on:

- Daily survey with market agents
- Weighted average by traded volume
- Adjustment to standard quality

Source: [CEPEA Methodology](https://www.cepea.esalq.usp.br/br/metodologia.aspx)

## Update and Lag

| Aspect | Value |
|---------|-------|
| **Update time** | ~17:00 - 18:00 (business days) |
| **Typical lag** | D+0 (same day) |
| **Days without publication** | Weekends, national holidays |
| **agrobr cache** | Expires at 18:00 (Smart TTL) |

## Usage

### Basic

```python
import asyncio
from agrobr import cepea

async def main():
    # Last year of data
    df = await cepea.indicador('soja')

    # Specific period
    df = await cepea.indicador('milho', inicio='2024-01-01', fim='2024-12-31')

    # Latest available value
    ultimo = await cepea.ultimo('boi')
    print(f"Live cattle: R$ {ultimo.valor}")

asyncio.run(main())
```

### With Metadata

```python
df, meta = await cepea.indicador('soja', return_meta=True)

print(meta.source)      # "noticias_agricolas"
print(meta.source_url)  # "https://www.noticiasagricolas.com.br/..."
print(meta.fetched_at)  # datetime(2026, 2, 4, 8, 24, 36)
print(meta.from_cache)  # True/False
```

## Data Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `data` | date | No | Indicator date |
| `produto` | str | No | Product name |
| `praca` | str | Yes | Reference market |
| `valor` | float | No | Price in BRL |
| `unidade` | str | No | Unit (BRL/sc60kg, etc) |
| `fonte` | str | No | Data source |
| `metodologia` | str | Yes | Methodology description |

## Cache

CEPEA uses Smart TTL - the cache expires automatically at 18:00:

```
08:00 - Fetch soybean -> Cache valid until 18:00
10:00 - Fetch soybean -> Uses cache
17:59 - Fetch soybean -> Uses cache
18:01 - Fetch soybean -> Cache expired -> Fetch source -> Valid until 18:00 tomorrow
```

## Helper Functions

```python
# List available products
produtos = await cepea.produtos()
# ['soja', 'soja_parana', 'milho', 'boi', 'bezerro', 'cafe', 'cafe_robusta', 'algodao',
#  'trigo', 'arroz', 'acucar', 'acucar_refinado', 'etanol_hidratado',
#  'etanol_anidro', 'frango_congelado', 'frango_resfriado', 'suino', 'leite',
#  'laranja_industria', 'laranja_in_natura']

# List markets for a product
pracas = await cepea.pracas('soja')
# ['paranagua', 'parana', 'rio_grande_do_sul']
```
