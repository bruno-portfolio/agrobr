# Async Ergonomics

agrobr uses `async/await` by default. This guide shows how to integrate it
in different environments.

## Quick Summary

| Environment | Approach | Import |
|---|---|---|
| Standalone script | `asyncio.run()` | `from agrobr import ...` |
| Jupyter Notebook | direct `await` or sync | `from agrobr.sync import ...` |
| FastAPI | direct `await` | `from agrobr import ...` |
| Airflow/Prefect | sync wrapper | `from agrobr.sync import ...` |

## Standalone Script

```python
import asyncio
from agrobr import cepea

async def main():
    df = await cepea.indicador("soja")
    print(df.head())

asyncio.run(main())
```

Parallel collection from multiple sources:

```python
import asyncio
from agrobr import cepea, comexstat, bcb

async def main():
    precos, exportacao, credito = await asyncio.gather(
        cepea.indicador("soja"),
        comexstat.exportacao("soja", ano=2024),
        bcb.credito_rural(produto="soja", safra="2024/25"),
    )

    print(f"Prices: {len(precos)} records")
    print(f"Exports: {len(exportacao)} records")
    print(f"Credit: {len(credito)} records")

asyncio.run(main())
```

## Jupyter Notebook

### Option 1: Top-level await (recommended)

Jupyter supports `await` directly in cells:

```python
from agrobr import cepea

df = await cepea.indicador("soja")
df.head()
```

### Option 2: sync API

Without `await`, use the sync wrapper:

```python
from agrobr.sync import cepea

df = cepea.indicador("soja")
df.head()
```

> **Note:** If you use `agrobr.sync` inside a Jupyter with a running event loop,
> agrobr will automatically try to use `nest_asyncio`. Install it with
> `pip install nest_asyncio` if needed.

### MetaInfo in the notebook

```python
from agrobr.sync import comexstat

df, meta = comexstat.exportacao("soja", ano=2024, return_meta=True)

print(f"Source: {meta.source}")
print(f"Records: {meta.records_count}")
print(f"Cache: {meta.from_cache}")
```

## FastAPI

agrobr is async-native, perfect for FastAPI:

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

With parallel collection in a single endpoint:

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

Airflow manages its own event loop. Use the sync API:

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

## Modules available via `agrobr.sync`

Every agrobr module is available in the sync API:

```python
from agrobr.sync import (
    anda,                 # Fertilizers (ANDA)
    bcb,                  # Rural credit (BCB/SICOR)
    cepea,                # Price indicators (CEPEA)
    comexstat,            # Exports/imports (MDIC)
    conab,                # Crop surveys + costs (CONAB)
    datasets,             # Semantic layer
    ibge,                 # PAM/LSPA (IBGE)
    inmet,                # Meteorology (INMET)
    noticias_agricolas,   # Agricultural quotes (Notícias Agrícolas)
    zarc,                 # Agricultural Climate Risk Zoning
)
```

## Error Handling

```python
from agrobr.sync import datasets
from agrobr.exceptions import SourceUnavailableError, ParseError

try:
    df = datasets.preco_diario("soja")
except SourceUnavailableError as e:
    print(f"Sources tried: {e.errors}")
except ParseError as e:
    print(f"Parser v{e.parser_version} failed: {e.reason}")
```
