# Guia Rápido

Este guia mostra como começar a usar o agrobr em poucos minutos.

## Instalação

```bash
# Instalação básica
pip install agrobr

# Com suporte a Polars (recomendado para grandes volumes)
pip install agrobr[polars]

# Instalar navegador para scraping avançado
playwright install chromium
```

## CEPEA - Indicadores de Preços

O CEPEA (Centro de Estudos Avançados em Economia Aplicada) publica indicadores diários de preços agrícolas.

### Async (recomendado para pipelines)

```python
import asyncio
from agrobr import cepea

async def main():
    # Indicador de soja
    df = await cepea.indicador('soja')
    print(df)

    # Com período específico
    df = await cepea.indicador(
        'soja',
        inicio='2024-01-01',
        fim='2024-12-31'
    )

    # Último valor disponível
    ultimo = await cepea.ultimo('soja')
    print(f"Soja: R$ {ultimo.valor}/sc em {ultimo.data}")

    # Lista de produtos disponíveis
    produtos = await cepea.produtos()
    print(produtos)

asyncio.run(main())
```

### Sync (uso simples)

```python
from agrobr.sync import cepea

# Mesma API, sem async/await
df = cepea.indicador('soja')
print(df.head())

# Último valor
ultimo = cepea.ultimo('milho')
print(f"Milho: R$ {ultimo.valor}")
```

### Produtos Disponíveis

| Produto | Descrição |
|---------|-----------|
| `soja` | Soja em grão (Paranaguá) |
| `milho` | Milho (Campinas) |
| `boi_gordo` | Boi gordo (São Paulo) |
| `cafe` | Café Arábica |
| `algodao` | Algodão em pluma |
| `trigo` | Trigo (Paraná) |

## CONAB - Safras

A CONAB (Companhia Nacional de Abastecimento) publica mensalmente estimativas de safras.

```python
from agrobr import conab

async def main():
    # Dados de safra
    df = await conab.safras('soja', safra='2024/25')
    print(df)

    # Por UF
    df = await conab.safras('soja', safra='2024/25', uf='MT')

    # Balanço oferta/demanda
    df = await conab.balanco('soja')

    # Totais Brasil
    df = await conab.brasil_total()

    # Lista de levantamentos disponíveis
    levs = await conab.levantamentos()
    print(levs)

asyncio.run(main())
```

### Produtos CONAB

Soja, milho, arroz, feijão, algodão, trigo, sorgo, aveia, centeio, cevada, girassol, mamona, amendoim, gergelim, canola, triticale.

## IBGE - PAM e LSPA

O IBGE fornece dados através da API SIDRA.

### PAM - Produção Agrícola Municipal

Dados anuais de produção agrícola por município.

```python
from agrobr import ibge

async def main():
    # PAM por UF
    df = await ibge.pam('soja', ano=2023, nivel='uf')
    print(df)

    # PAM por município (grande volume!)
    df = await ibge.pam('soja', ano=2023, nivel='municipio', uf='MT')

    # Múltiplos anos
    df = await ibge.pam('soja', ano=[2020, 2021, 2022, 2023])

asyncio.run(main())
```

### LSPA - Levantamento Sistemático

Estimativas mensais de safra.

```python
from agrobr import ibge

async def main():
    # LSPA mensal
    df = await ibge.lspa('soja', ano=2024, mes=6)
    print(df)

    # Milho 1ª e 2ª safra
    df1 = await ibge.lspa('milho_1', ano=2024)
    df2 = await ibge.lspa('milho_2', ano=2024)

asyncio.run(main())
```

## Usando Polars

Todas as APIs suportam retorno em Polars para melhor performance:

```python
from agrobr import cepea

async def main():
    # Retorna polars.DataFrame em vez de pandas
    df = await cepea.indicador('soja', as_polars=True)

    # Operações Polars são muito mais rápidas
    resultado = (
        df
        .filter(pl.col('valor') > 100)
        .group_by('produto')
        .agg(pl.col('valor').mean())
    )

asyncio.run(main())
```

## CLI - Linha de Comando

O agrobr inclui uma CLI completa:

```bash
# CEPEA
agrobr cepea soja
agrobr cepea soja --inicio 2024-01-01 --formato csv > soja.csv
agrobr cepea soja --ultimo --json

# CONAB
agrobr conab safras soja --safra 2024/25
agrobr conab balanco milho
agrobr conab levantamentos

# IBGE
agrobr ibge pam soja --ano 2023 --nivel uf
agrobr ibge lspa milho --ano 2024 --mes 6

# Health check
agrobr health --all

# Cache
agrobr cache status
agrobr cache clear --older-than 30d
```

## Configuração

### Variáveis de Ambiente

```bash
# Cache
export AGROBR_CACHE_CACHE_DIR=~/.agrobr/cache
export AGROBR_CACHE_OFFLINE_MODE=false

# HTTP
export AGROBR_HTTP_TIMEOUT_READ=30
export AGROBR_HTTP_MAX_RETRIES=3

# Alertas (opcional)
export AGROBR_ALERT_SLACK_WEBHOOK=https://hooks.slack.com/...
export AGROBR_ALERT_DISCORD_WEBHOOK=https://discord.com/api/webhooks/...

# Telemetria (opt-in)
export AGROBR_TELEMETRY_ENABLED=true
```

### Via Código

```python
from agrobr.constants import CacheSettings, HTTPSettings

# Configurar cache
cache = CacheSettings(
    cache_dir='./meu_cache',
    offline_mode=True  # Usa apenas cache local
)

# Configurar HTTP
http = HTTPSettings(
    timeout_read=60,
    max_retries=5
)
```

## Tratamento de Erros

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
        print(f"Fonte indisponível: {e.source}")
        # Usar cache offline
        df = await cepea.indicador('soja', offline=True)
    except ParseError as e:
        print(f"Erro de parsing: {e.reason}")
    except ValidationError as e:
        print(f"Dados inválidos: {e.field} = {e.value}")
```

## Próximos Passos

- Veja os [exemplos completos](https://github.com/bruno-portfolio/agrobr/tree/main/examples)
- Consulte a [API Reference](api/cepea.md)
- Aprenda sobre [resiliência e fallbacks](advanced/resilience.md)
