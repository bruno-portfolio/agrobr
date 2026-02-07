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

> **Nota:** Se usar `agrobr.sync` dentro de um Jupyter com event loop rodando,
> o agrobr tentará usar `nest_asyncio` automaticamente. Instale com
> `pip install nest_asyncio` se necessário.

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

Todos os módulos do agrobr estão disponíveis na API sync:

```python
from agrobr.sync import (
    anda,        # Fertilizantes (ANDA)
    bcb,         # Crédito rural (BCB/SICOR)
    cepea,       # Indicadores de preço (CEPEA)
    comexstat,   # Exportação/importação (MDIC)
    conab,       # Safras + custos (CONAB)
    datasets,    # Camada semântica
    ibge,        # PAM/LSPA (IBGE)
    inmet,       # Meteorologia (INMET)
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
