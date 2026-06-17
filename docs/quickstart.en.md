# Quick Start

This guide shows how to get started with agrobr in a few minutes.

## Installation

```bash
# Basic install
pip install agrobr

# With Polars support (recommended for large volumes)
pip install agrobr[polars]

# With Playwright (sources that require JavaScript)
pip install agrobr[browser]
playwright install chromium
```

### Via Docker (no local Python)

```bash
docker build -t agrobr .
docker run -it --rm agrobr
```

See the [Docker guide](guides/docker.md) for extras and advanced options.

## CEPEA - Price Indicators

CEPEA (Center for Advanced Studies in Applied Economics) publishes daily agricultural price indicators.

### Async (recommended for pipelines)

```python
import asyncio
from agrobr import cepea

async def main():
    # Soybean indicator
    df = await cepea.indicador('soja')
    print(df)

    # With a specific period
    df = await cepea.indicador(
        'soja',
        inicio='2024-01-01',
        fim='2024-12-31'
    )

    # Latest available value
    ultimo = await cepea.ultimo('soja')
    print(f"Soybean: R$ {ultimo.valor}/bag on {ultimo.data}")

    # List of available products
    produtos = await cepea.produtos()
    print(produtos)

asyncio.run(main())
```

### Sync (simple use)

```python
from agrobr.sync import cepea

# Same API, no async/await
df = cepea.indicador('soja')
print(df.head())

# Latest value
ultimo = cepea.ultimo('milho')
print(f"Corn: R$ {ultimo.valor}")
```

### Available Products

| Product | Description | Unit |
|---------|-------------|------|
| `soja` | Soybean grain (Paranaguá) | BRL/60kg bag |
| `soja_parana` | Soybean (Paraná) | BRL/60kg bag |
| `milho` | Corn (Campinas) | BRL/60kg bag |
| `boi` / `boi_gordo` | Live cattle (São Paulo) | BRL/@ |
| `cafe` / `cafe_arabica` | Arabica coffee (São Paulo) | BRL/60kg bag |
| `cafe_robusta` | Robusta/Conilon coffee (Espírito Santo) | BRL/60kg bag |
| `algodao` | Cotton lint | cBRL/lb |
| `trigo` | Wheat (Paraná + RS) | BRL/ton |
| `arroz` | Paddy rice (ESALQ/BBM) | BRL/50kg bag |
| `acucar` | Crystal sugar | BRL/50kg bag |
| `acucar_refinado` | Refined amorphous sugar | BRL/50kg bag |
| `etanol_hidratado` | Hydrous ethanol (weekly) | BRL/L |
| `etanol_anidro` | Anhydrous ethanol (weekly) | BRL/L |
| `frango_congelado` | Frozen chicken | BRL/kg |
| `frango_resfriado` | Chilled chicken | BRL/kg |
| `suino` | Live hog | BRL/kg |
| `leite` | Farm-gate milk | BRL/L |
| `laranja_industria` | Industry orange | BRL/40.8kg box |
| `laranja_in_natura` | Pera orange (fresh) | BRL/40.8kg box |

## CONAB - Crop Surveys

CONAB (National Supply Company) publishes monthly crop survey estimates.

```python
from agrobr import conab

async def main():
    # Crop survey data
    df = await conab.safras('soja', safra='2024/25')
    print(df)

    # By state
    df = await conab.safras('soja', safra='2024/25', uf='MT')

    # Supply/demand balance
    df = await conab.balanco('soja')

    # Brazil totals
    df = await conab.brasil_total()

    # List of available surveys
    levs = await conab.levantamentos()
    print(levs)

asyncio.run(main())
```

### CONAB Products

Soybean, corn, rice, beans, cotton, wheat, sorghum, oats, rye, barley, sunflower, castor bean, peanut, sesame, canola, triticale.

## CONAB - Crop Progress

Weekly planting and harvest progress by crop and state.

```python
from agrobr import conab

async def main():
    # Progress for all crops (latest week)
    df = await conab.progresso_safra()

    # Filter by crop and state
    df = await conab.progresso_safra(cultura="Soja", estado="MT")

    # Harvest only
    df = await conab.progresso_safra(operacao="Colheita")

    # List available weeks
    semanas = await conab.semanas_disponiveis()
    print(semanas[0])  # {'descricao': '...', 'url': '...'}

asyncio.run(main())
```

### Progress Crops

Soybean, Corn 1st, Corn 2nd, Rice, Cotton, Beans 1st, Beans 3rd.

## IBGE - PAM and LSPA

IBGE provides data through the SIDRA API.

### PAM - Municipal Agricultural Production

Annual agricultural production data by municipality.

```python
from agrobr import ibge

async def main():
    # PAM by state
    df = await ibge.pam('soja', ano=2023, nivel='uf')
    print(df)

    # PAM by municipality (large volume!)
    df = await ibge.pam('soja', ano=2023, nivel='municipio', uf='MT')

    # Multiple years
    df = await ibge.pam('soja', ano=[2020, 2021, 2022, 2023])

asyncio.run(main())
```

### LSPA - Systematic Survey

Monthly crop estimates.

```python
from agrobr import ibge

async def main():
    # Monthly LSPA
    df = await ibge.lspa('soja', ano=2024, mes=6)
    print(df)

    # Corn 1st and 2nd crop
    df1 = await ibge.lspa('milho_1', ano=2024)
    df2 = await ibge.lspa('milho_2', ano=2024)

    # Generic aliases — automatically expand into sub-crops
    df = await ibge.lspa('milho', ano=2024)   # → milho_1 + milho_2
    df = await ibge.lspa('feijao', ano=2024)  # → feijao_1 + feijao_2 + feijao_3
    df = await ibge.lspa('batata', ano=2024)  # → batata_1 + batata_2

asyncio.run(main())
```

### PEVS — Silviculture and Plant Extraction

Annual silvicultural and plant-extraction production data.

```python
from agrobr import ibge

async def main():
    # Silviculture — log wood production
    df = await ibge.silvicultura('madeira_tora', ano=2023)

    # Plant extraction — açaí production
    df = await ibge.extracao_vegetal('acai', ano=2023)

    # Eucalyptus planted area
    df = await ibge.silvicultura('eucalipto', variavel='area')

asyncio.run(main())
```

### Quarterly Milk and Agricultural GDP

```python
from agrobr import ibge

async def main():
    # Milk — acquisition + processing + price
    df = await ibge.leite_trimestral(trimestre='202303')

    # Quarterly agricultural GDP
    df = await ibge.pib_agro(trimestre='202501')

asyncio.run(main())
```

## ComexStat - Exports

Foreign-trade data from MDIC/SECEX by NCM, state and country.

```python
from agrobr import comexstat

async def main():
    # Monthly soybean exports
    df = await comexstat.exportacao("soja", ano=2024)

    # By state
    df = await comexstat.exportacao("soja", ano=2024, uf="MT")

    # Cotton (prefix match captures all NCM subheadings)
    df = await comexstat.exportacao("algodao", ano=2024)

asyncio.run(main())
```

### ComexStat Products

Soybean, corn, coffee, cotton, wheat, rice, sugar, ethanol, beef/poultry/pork, and more.
See the [ComexStat source docs](sources/comexstat.md) for the full NCM table.

## NASA POWER - Climate Data

Global climate data from NASA (a substitute for INMET, whose API is down).
Global coverage, 0.5-degree grid, since 1981, no authentication.

```python
from agrobr import nasa_power

async def main():
    # Monthly climate for MT in 2024
    df = await nasa_power.clima_uf("MT", ano=2024)

    # Daily data for a single point
    df = await nasa_power.clima_ponto(
        lat=-12.6, lon=-56.1,
        inicio="2024-01-01", fim="2024-01-31"
    )

    # Monthly aggregation for a point
    df = await nasa_power.clima_ponto(
        lat=-12.6, lon=-56.1,
        inicio="2024-01-01", fim="2024-12-31",
        agregacao="mensal"
    )

asyncio.run(main())
```

## INMET - Meteorology (API down)

> **Note:** The INMET API is returning 404. Use `nasa_power` as an alternative.

Climate data from 600+ automatic INMET stations.

```python
from agrobr import inmet

async def main():
    # Automatic stations in MT
    df = await inmet.estacoes(tipo="T", uf="MT")

    # Monthly climate aggregated by state
    df = await inmet.clima_uf("MT", ano=2024)

    # Hourly data for a station
    df = await inmet.estacao("A001", inicio="2024-01-01", fim="2024-01-31")

asyncio.run(main())
```

## BCB - Rural Credit

Rural credit data from SICOR (Rural Credit Operations System).

```python
from agrobr import bcb

async def main():
    # Working-capital credit for soybean
    df = await bcb.credito_rural("soja", safra="2024/25")

    # Filter by state
    df = await bcb.credito_rural("soja", safra="2024/25", uf="MT")

asyncio.run(main())
```

## ANDA - Fertilizers

Fertilizer deliveries by state and month. Requires `pip install agrobr[pdf]`.

```python
from agrobr import anda

async def main():
    # National deliveries
    df = await anda.entregas(ano=2024)

    # Filter by state
    df = await anda.entregas(ano=2024, uf="MT")

asyncio.run(main())
```

## CONAB - Production Cost

Detailed costs per hectare, crop and state.

```python
from agrobr import conab

async def main():
    # Soybean production cost in MT
    df = await conab.custo_producao("soja", uf="MT")

    # Totals (COE, COT, CT)
    totais = await conab.custo_producao_total("soja", uf="MT")

asyncio.run(main())
```

## Using Polars

Every API supports returning Polars for better performance:

```python
from agrobr import cepea

async def main():
    # Returns polars.DataFrame instead of pandas
    df = await cepea.indicador('soja', as_polars=True)

    # Polars operations are much faster
    resultado = (
        df
        .filter(pl.col('valor') > 100)
        .group_by('produto')
        .agg(pl.col('valor').mean())
    )

asyncio.run(main())
```

## CLI - Command Line

agrobr ships with a full CLI:

```bash
# CEPEA
agrobr cepea indicador soja
agrobr cepea indicador soja --inicio 2024-01-01 --formato csv > soja.csv
agrobr cepea indicador soja --ultimo

# CONAB
agrobr conab safras soja --safra 2024/25
agrobr conab balanco milho
agrobr conab levantamentos

# IBGE
agrobr ibge pam soja --ano 2023 --nivel uf
agrobr ibge lspa milho --ano 2024 --mes 6

# Health check
agrobr health          # all sources
agrobr health --deep   # deep check (fingerprint + parse)

# Cache (status via doctor; clear = remove the file)
agrobr doctor
rm ~/.agrobr/cache/agrobr.duckdb
```

## Configuration

### Environment Variables

```bash
# Cache
export AGROBR_CACHE_CACHE_DIR=~/.agrobr/cache
export AGROBR_CACHE_DB_NAME=agrobr.duckdb

# HTTP
export AGROBR_HTTP_TIMEOUT_READ=30
export AGROBR_HTTP_MAX_RETRIES=3

# Alerts (optional)
export AGROBR_ALERT_SLACK_WEBHOOK=https://hooks.slack.com/...
export AGROBR_ALERT_DISCORD_WEBHOOK=https://discord.com/api/webhooks/...
```

### Via Code

```python
from agrobr.constants import CacheSettings, HTTPSettings

# Configure cache
cache = CacheSettings(
    cache_dir='./my_cache',
    offline_mode=True  # Use local cache only
)

# Configure HTTP
http = HTTPSettings(
    timeout_read=60,
    max_retries=5
)
```

## Error Handling

```python
from agrobr import cepea
from agrobr.exceptions import (
    SourceUnavailableError,
    ParseError,
    ValidationError
)

async def main():
    try:
        df = await cepea.indicador('soja')
    except SourceUnavailableError as e:
        print(f"Source unavailable: {e.source}")
        # Use offline cache
        df = await cepea.indicador('soja', offline=True)
    except ParseError as e:
        print(f"Parsing error: {e.reason}")
    except ValidationError as e:
        print(f"Invalid data: {e.field} = {e.value}")
```

## Interactive Notebook

Try every source right in your browser:

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/bruno-portfolio/agrobr/blob/main/examples/agrobr_demo.ipynb)

## Next Steps

- See the [full examples](https://github.com/bruno-portfolio/agrobr/tree/main/examples)
- Check the [API Reference](api/cepea.md)
- Learn about [resilience and fallbacks](advanced/resilience.md)
