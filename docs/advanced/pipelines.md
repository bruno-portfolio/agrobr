# Integração com Pipelines

O agrobr oferece API síncrona via `agrobr.sync` para uso em orquestradores.

## Por que usar a API Sync?

Orquestradores como Airflow, Prefect e Dagster gerenciam seu próprio
event loop. Usar `asyncio.run()` dentro de uma task pode causar conflitos.
A API sync resolve isso automaticamente.

## Airflow

```python
from airflow.decorators import task, dag
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2024, 1, 1))
def agrobr_pipeline():

    @task
    def extract_precos():
        from agrobr.sync import datasets
        df = datasets.preco_diario("soja")
        df.to_parquet("/data/soja_precos.parquet")

    @task
    def extract_safra():
        from agrobr.sync import datasets
        df = datasets.estimativa_safra("soja", safra="2024/25")
        df.to_parquet("/data/soja_safra.parquet")

    @task
    def extract_balanco():
        from agrobr.sync import datasets
        df = datasets.balanco("soja")
        df.to_parquet("/data/soja_balanco.parquet")

    extract_precos() >> extract_safra() >> extract_balanco()

agrobr_pipeline()
```

## Prefect

```python
from prefect import task, flow

@task
def fetch_precos(produto: str):
    from agrobr.sync import datasets
    return datasets.preco_diario(produto)

@task
def fetch_producao(produto: str, ano: int):
    from agrobr.sync import datasets
    return datasets.producao_anual(produto, ano=ano)

@flow
def pipeline_agro():
    produtos = ["soja", "milho", "cafe"]

    for produto in produtos:
        df_precos = fetch_precos(produto)
        df_prod = fetch_producao(produto, ano=2023)

        df_precos.to_parquet(f"/data/{produto}_precos.parquet")
        df_prod.to_parquet(f"/data/{produto}_producao.parquet")

pipeline_agro()
```

## Dagster

```python
from dagster import asset, AssetExecutionContext

@asset
def soja_precos():
    from agrobr.sync import datasets
    return datasets.preco_diario("soja")

@asset
def soja_producao():
    from agrobr.sync import datasets
    return datasets.producao_anual("soja", ano=2023)

@asset
def soja_safra():
    from agrobr.sync import datasets
    return datasets.estimativa_safra("soja")

@asset(deps=[soja_precos, soja_producao, soja_safra])
def soja_report(context: AssetExecutionContext):
    context.log.info("Dados da soja carregados")
```

## Uso Direto (Async)

Se você controla o event loop, use a API async:

```python
import asyncio
from agrobr import datasets

async def main():
    df_precos = await datasets.preco_diario("soja")
    df_safra = await datasets.estimativa_safra("soja")
    df_balanco = await datasets.balanco("soja")

    df_precos.to_parquet("/data/soja_precos.parquet")
    df_safra.to_parquet("/data/soja_safra.parquet")
    df_balanco.to_parquet("/data/soja_balanco.parquet")

asyncio.run(main())
```

## Paralelismo com Async

```python
import asyncio
from agrobr import datasets

async def fetch_all():
    produtos = ["soja", "milho", "cafe", "trigo"]

    tasks = [datasets.preco_diario(p) for p in produtos]
    resultados = await asyncio.gather(*tasks)

    return dict(zip(produtos, resultados))

dfs = asyncio.run(fetch_all())
```

## Jupyter Notebooks

Em notebooks, use a API sync ou `await` diretamente:

```python
# Opção 1: Sync
from agrobr.sync import datasets
df = datasets.preco_diario("soja")

# Opção 2: Await (Jupyter suporta top-level await)
from agrobr import datasets
df = await datasets.preco_diario("soja")
```

## Tratamento de Erros

```python
from agrobr.sync import datasets
from agrobr.exceptions import SourceUnavailableError

try:
    df = datasets.preco_diario("soja")
except SourceUnavailableError as e:
    print(f"Todas as fontes falharam: {e.errors}")
```
