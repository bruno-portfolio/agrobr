# Guia Rápido

Este guia mostra como começar a usar o agrobr em poucos minutos.

## Instalação

```bash
# Instalação básica
pip install agrobr

# Com suporte a Polars (recomendado para grandes volumes)
pip install agrobr[polars]

# Com Playwright (fontes que requerem JavaScript)
pip install agrobr[browser]
playwright install chromium
```

### Via Docker (sem Python local)

```bash
docker build -t agrobr .
docker run -it --rm agrobr
```

Veja o [guia Docker](guides/docker.md) para extras e opções avançadas.

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

| Produto | Descrição | Unidade |
|---------|-----------|---------|
| `soja` | Soja em grão (Paranaguá) | BRL/sc 60kg |
| `soja_parana` | Soja (Paraná) | BRL/sc 60kg |
| `milho` | Milho (Campinas) | BRL/sc 60kg |
| `boi` / `boi_gordo` | Boi gordo (São Paulo) | BRL/@ |
| `cafe` / `cafe_arabica` | Café Arábica (São Paulo) | BRL/sc 60kg |
| `cafe_robusta` | Café Robusta/Conilon (Espírito Santo) | BRL/sc 60kg |
| `algodao` | Algodão em pluma | cBRL/lb |
| `trigo` | Trigo (Paraná + RS) | BRL/ton |
| `arroz` | Arroz em casca (ESALQ/BBM) | BRL/sc 50kg |
| `acucar` | Açúcar cristal | BRL/sc 50kg |
| `acucar_refinado` | Açúcar refinado amorfo | BRL/sc 50kg |
| `etanol_hidratado` | Etanol hidratado (semanal) | BRL/L |
| `etanol_anidro` | Etanol anidro (semanal) | BRL/L |
| `frango_congelado` | Frango congelado | BRL/kg |
| `frango_resfriado` | Frango resfriado | BRL/kg |
| `suino` | Suíno vivo | BRL/kg |
| `leite` | Leite ao produtor | BRL/L |
| `laranja_industria` | Laranja indústria | BRL/cx 40,8kg |
| `laranja_in_natura` | Laranja pera in natura | BRL/cx 40,8kg |

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

## CONAB - Progresso de Safra

Progresso semanal de plantio e colheita por cultura e UF.

```python
from agrobr import conab

async def main():
    # Progresso de todas as culturas (semana mais recente)
    df = await conab.progresso_safra()

    # Filtrar por produto e UF
    df = await conab.progresso_safra(produto="soja", uf="MT")

    # Apenas colheita
    df = await conab.progresso_safra(operacao="Colheita")

    # Listar semanas disponíveis
    semanas = await conab.semanas_disponiveis()
    print(semanas[0])  # {'descricao': '...', 'url': '...'}

asyncio.run(main())
```

### Culturas Progresso

Soja, Milho 1ª, Milho 2ª, Arroz, Algodão, Feijão 1ª e Trigo.

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

    # Aliases genéricos — expandem para sub-safras automaticamente
    df = await ibge.lspa('milho', ano=2024)   # → milho_1 + milho_2
    df = await ibge.lspa('feijao', ano=2024)  # → feijao_1 + feijao_2 + feijao_3
    df = await ibge.lspa('batata', ano=2024)  # → batata_1 + batata_2

asyncio.run(main())
```

### PEVS — Silvicultura e Extracao Vegetal

Dados anuais de producao silvicultural e extrativista vegetal.

```python
from agrobr import ibge

async def main():
    # Silvicultura — producao de madeira
    df = await ibge.silvicultura('madeira_tora', ano=2023)

    # Extracao vegetal — producao de acai
    df = await ibge.extracao_vegetal('acai', ano=2023)

    # Area plantada de eucalipto
    df = await ibge.silvicultura('eucalipto', variavel='area')

asyncio.run(main())
```

### Leite Trimestral e PIB Agro

```python
from agrobr import ibge

async def main():
    # Leite — aquisicao + industrializacao + preco
    df = await ibge.leite_trimestral(trimestre='202303')

    # PIB agropecuario trimestral
    df = await ibge.pib_agro(trimestre='202501')

asyncio.run(main())
```

## ComexStat - Exportacoes

Dados de comercio exterior do MDIC/SECEX por NCM, UF e pais.

```python
from agrobr import comexstat

async def main():
    # Exportacoes mensais de soja
    df = await comexstat.exportacao("soja", ano=2024)

    # Por UF
    df = await comexstat.exportacao("soja", ano=2024, uf="MT")

    # Algodao (prefix match captura todas subposicoes NCM)
    df = await comexstat.exportacao("algodao", ano=2024)

asyncio.run(main())
```

### Produtos ComexStat

Soja, milho, cafe, algodao, trigo, arroz, acucar, etanol, carne bovina/frango/suina, e mais.
Veja [docs/sources/comexstat.md](sources/comexstat.md) para tabela completa de NCMs.

## NASA POWER - Dados Climaticos

Dados climaticos globais da NASA (alternativa ao INMET, sem token).
Cobertura global, grid 0.5 grau, desde 1981, sem autenticacao.

```python
from agrobr import nasa_power

async def main():
    # Clima mensal de MT em 2024
    df = await nasa_power.clima_uf("MT", ano=2024)

    # Dados diarios de um ponto
    df = await nasa_power.clima_ponto(
        lat=-12.6, lon=-56.1,
        inicio="2024-01-01", fim="2024-01-31"
    )

    # Agregacao mensal de um ponto
    df = await nasa_power.clima_ponto(
        lat=-12.6, lon=-56.1,
        inicio="2024-01-01", fim="2024-12-31",
        agregacao="mensal"
    )

asyncio.run(main())
```

## INMET - Meteorologia

> **Nota:** o catálogo de estações e os ZIPs históricos anuais (`historico`, `historico_periodo`, `historico_uf`) são
> públicos, sem token; a rota observacional (`estacao`, `clima_uf`) exige `AGROBR_INMET_TOKEN`. Veja
> [a fonte](sources/inmet.md).

Dados climaticos de 600+ estacoes automaticas do INMET.

```python
from agrobr import inmet

async def main():
    # Estacoes automaticas de MT
    df = await inmet.estacoes(tipo="T", uf="MT")

    # Clima mensal agregado por UF
    df = await inmet.clima_uf("MT", ano=2024)

    # Dados horarios de uma estacao
    df = await inmet.estacao("A001", inicio="2024-01-01", fim="2024-01-31")

asyncio.run(main())
```

## BCB - Credito Rural

Dados de credito rural do SICOR (Sistema de Operacoes do Credito Rural).

```python
from agrobr import bcb

async def main():
    # Credito de custeio para soja
    df = await bcb.credito_rural("soja", safra="2024/25")

    # Filtrar por UF
    df = await bcb.credito_rural("soja", safra="2024/25", uf="MT")

asyncio.run(main())
```

## ANDA - Fertilizantes

Entregas mensais de fertilizantes (total nacional). Requer `pip install agrobr[pdf]`.

```python
from agrobr import anda

async def main():
    # Entregas nacionais
    df = await anda.entregas(ano=2024)

asyncio.run(main())
```

## CONAB - Custo de Producao

Custos detalhados por hectare, cultura e UF. O exemplo seleciona automaticamente
a última planilha em ordem alfabética no catálogo e uma aba identificada de MT com o ano
de referência mais recente. Planilhas publicadas em anos diferentes podem cobrir
períodos sobrepostos; o ano de referência de preços não é a safra.

```python
import asyncio

from agrobr import conab

async def main():
    catalogo = await conab.catalogo_custos("soja")
    planilha = catalogo["planilha"].max()
    contextos = await conab.catalogo_custos("soja", planilha=planilha)
    mt = contextos[contextos["uf"].eq("MT") & contextos["status"].eq("identified")]
    aba = mt.sort_values("ano_referencia")["aba"].iloc[-1]
    df = await conab.custo_producao("soja", uf="MT", planilha=planilha, aba=aba)
    totais = await conab.custo_producao_total("soja", uf="MT", planilha=planilha, aba=aba)
    print(df)
    print(totais)

asyncio.run(main())
```

## Usando Polars

Todas as APIs suportam retorno em Polars para melhor performance:

```python
import asyncio

import polars as pl

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
agrobr health          # todas as fontes
agrobr health --deep   # CEPEA: fingerprint contra a baseline do pacote + parse

# Cache (status via doctor; limpar = remover o arquivo)
agrobr doctor
rm ~/.agrobr/cache/agrobr.duckdb
```

`--formato json` sai com as datas em ISO 8601 (`"2026-09-22T00:00:00.000"`). Com `--ultimo`, a linha tem as mesmas
colunas e os mesmos tipos da tabela sem ele.

`--formato` aceita `table`, `csv` e `json` (outro valor sai com código 2), e a saída sai em UTF-8, também redirecionada
no Windows. Todos os comandos e opções estão na [referência da CLI](advanced/cli.md).

## Configuração

### Variáveis de Ambiente

A lista completa, com padrões e quando cada uma vale, está em [Variáveis de ambiente](advanced/ambiente.md).

```bash
# Cache
export AGROBR_CACHE_DIR=~/.agrobr/cache
export AGROBR_CACHE_DB_NAME=agrobr.duckdb

# HTTP
export AGROBR_HTTP_TIMEOUT_READ=30
export AGROBR_HTTP_MAX_RETRIES=3

# Alertas (opcional)
export AGROBR_ALERT_SLACK_WEBHOOK=https://hooks.slack.com/...
export AGROBR_ALERT_DISCORD_WEBHOOK=https://discord.com/api/webhooks/...
```

### Via Código

Defina as variáveis antes de importar o pacote. `offline=True` consulta os dados já existentes no cache configurado.

```python
import os

os.environ["AGROBR_CACHE_DIR"] = "./meu_cache"
os.environ["AGROBR_HTTP_TIMEOUT_READ"] = "60"
os.environ["AGROBR_HTTP_MAX_RETRIES"] = "5"

from agrobr import cepea

df = await cepea.indicador("soja", offline=True)
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

## Notebook Interativo

Experimente todas as fontes direto no navegador:

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/bruno-portfolio/agrobr/blob/main/examples/agrobr_demo.ipynb)

## Próximos Passos

- Veja os [exemplos completos](https://github.com/bruno-portfolio/agrobr/tree/main/examples)
- Consulte a [API Reference](api/cepea.md)
- Aprenda sobre [resiliência e fallbacks](advanced/resilience.md)
