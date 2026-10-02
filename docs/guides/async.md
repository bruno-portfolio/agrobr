# Ergonomia Async

O agrobr usa `async/await` por padrão. Este guia mostra como integrar
em diferentes ambientes.

## Resumo Rápido

| Ambiente | Abordagem | Import |
|---|---|---|
| Script standalone | `asyncio.run()` | `from agrobr import ...` |
| Jupyter Notebook | `await` direto ou sync | `from agrobr.sync import ...` |
| FastAPI | `await` direto | `from agrobr import ...` |
| Airflow/Prefect | sync wrapper | `from agrobr.sync import ...` |

## Script Standalone

```python
import asyncio
from agrobr import cepea

async def main():
    df = await cepea.indicador("soja")
    print(df.head())

asyncio.run(main())
```

Coleta paralela de múltiplas fontes:

```python
import asyncio
from agrobr import cepea, comexstat, bcb

async def main():
    precos, exportacao, credito = await asyncio.gather(
        cepea.indicador("soja"),
        comexstat.exportacao("soja", ano=2024),
        bcb.credito_rural(produto="soja", safra="2024/25"),
    )

    print(f"Preços: {len(precos)} registros")
    print(f"Exportação: {len(exportacao)} registros")
    print(f"Crédito: {len(credito)} registros")

asyncio.run(main())
```

## Jupyter Notebook

### Opção 1: Top-level await (recomendado)

Jupyter suporta `await` diretamente nas células:

```python
from agrobr import cepea

df = await cepea.indicador("soja")
df.head()
```

### Opção 2: API sync

Sem `await`, usa wrapper síncrono:

```python
from agrobr.sync import cepea

df = cepea.indicador("soja")
df.head()
```

> **Nota:** Com um event loop rodando (Jupyter), prefira o `await` da opção 1. O
> `agrobr.sync` também funciona, sem `nest_asyncio`: a consulta roda numa thread à
> parte, com o seu próprio `asyncio.run` e o contexto (modo determinístico) da
> célula, e o loop do notebook fica parado até ela terminar. Na 1ª vez, o agrobr
> avisa (`UserWarning`) e recomenda o `await`.

> **Nota:** O `agrobr.sync` embrulha as funções que devolvem um resultado. O
> `sicar.imoveis_geo_stream` continua um gerador assíncrono, porque entrega os lotes aos
> poucos: consuma com `async for` dentro de uma função `async`, como na API async.

### MetaInfo no notebook

```python
from agrobr.sync import comexstat

df, meta = comexstat.exportacao("soja", ano=2024, return_meta=True)

print(f"Fonte: {meta.source}")
print(f"Registros: {meta.records_count}")
print(f"Cache: {meta.from_cache}")
```

## FastAPI

O agrobr é async nativo, perfeito para FastAPI:

```python
from fastapi import FastAPI
from agrobr import cepea, comexstat

app = FastAPI()

@app.get("/precos/{produto}")
async def get_precos(produto: str):
    df = await cepea.indicador(produto)
    return df.to_dict(orient="records")

@app.get("/exportacao/{produto}/{ano}")
async def get_exportacao(produto: str, ano: int):
    df, meta = await comexstat.exportacao(
        produto, ano=ano, return_meta=True
    )
    return {
        "data": df.to_dict(orient="records"),
        "meta": meta.to_dict(),
    }
```

Com coleta paralela em um endpoint:

```python
import asyncio

@app.get("/dashboard/{produto}")
async def dashboard(produto: str):
    precos, safra = await asyncio.gather(
        cepea.indicador(produto),
        comexstat.exportacao(produto, ano=2024),
    )
    return {
        "precos": precos.tail(5).to_dict(orient="records"),
        "exportacao": safra.to_dict(orient="records"),
    }
```

## Airflow

Airflow gerencia seu próprio event loop. Use a API sync:

```python
from airflow.decorators import task, dag
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2024, 1, 1))
def agrobr_pipeline():

    @task
    def extract_precos():
        from agrobr.sync import cepea
        df = cepea.indicador("soja")
        df.to_parquet("/data/soja_precos.parquet")

    @task
    def extract_exportacao():
        from agrobr.sync import comexstat
        df = comexstat.exportacao("soja", ano=2024)
        df.to_parquet("/data/soja_export.parquet")

    @task
    def extract_credito():
        from agrobr.sync import bcb
        df = bcb.credito_rural(produto="soja", safra="2024/25")
        df.to_parquet("/data/soja_credito.parquet")

    extract_precos() >> extract_exportacao() >> extract_credito()

agrobr_pipeline()
```

## Prefect

```python
from prefect import task, flow

@task
def fetch_precos(produto: str):
    from agrobr.sync import cepea
    return cepea.indicador(produto)

@task
def fetch_clima(uf: str, ano: int):
    from agrobr.sync import inmet
    return inmet.clima_uf(uf, ano=ano)

@flow
def pipeline_agro():
    df_precos = fetch_precos("soja")
    df_clima = fetch_clima("MT", 2024)

    df_precos.to_parquet("/data/precos.parquet")
    df_clima.to_parquet("/data/clima_mt.parquet")

pipeline_agro()
```

## Módulos disponíveis via `agrobr.sync`

Todas as fontes e os datasets do agrobr estão disponíveis na API sync, com as mesmas assinaturas em tempo de execução. O checker de tipos e o editor não as veem, porque o espelho é dinâmico e devolve `Any`; para código tipado, use a API assíncrona, cujo `return_meta` tem `@overload`:

```python
from agrobr.sync import (
    abiove,
    acervo_fundiario,
    alt,
    ana,
    anda,
    anec,
    antaq,
    b3,
    bcb,
    cepea,
    cftc,
    cnuc,
    comexstat,
    comtrade,
    conab,
    datasets,
    defensivos,
    deral,
    desmatamento,
    embrapa_solos,
    funai,
    ibama,
    ibge,
    icmbio,
    imea,
    incra,
    inmet,
    lista_suja,
    mapbiomas,
    mapbiomas_alerta,
    nasa_power,
    noticias_agricolas,
    queimadas,
    rio_verde,
    rnc,
    sfb,
    sicar,
    unica,
    usda,
    zarc,
)
```

## Tratamento de Erros

```python
from agrobr.sync import datasets
from agrobr.exceptions import SourceUnavailableError, ParseError

try:
    df = datasets.preco_diario("soja")
except SourceUnavailableError as e:
    print(f"Fontes tentadas: {e.errors}")
except ParseError as e:
    print(f"Parser v{e.parser_version} falhou: {e.reason}")
```
