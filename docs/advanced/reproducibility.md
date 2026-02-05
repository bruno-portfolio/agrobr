# Reprodutibilidade

O agrobr permite análises 100% reproduzíveis.

## Modo Determinístico

```python
from agrobr import datasets

async with datasets.deterministic(snapshot="2025-12-31"):
    df = await datasets.preco_diario("soja")
```

Também disponível como decorator:

```python
from agrobr.datasets import deterministic_decorator

@deterministic_decorator("2025-12-31")
async def meu_pipeline():
    df = await datasets.preco_diario("soja")
    return df
```

## Semântica do Snapshot

| Aspecto | Definição |
|---------|-----------|
| **Formato** | `"YYYY-MM-DD"` — data máxima de corte |
| **Filtro** | Todos os DataFrames filtrados por `data <= snapshot` |
| **Rede** | Bloqueada — apenas cache DuckDB local |
| **Escopo** | Isolado por contexto async (contextvars) — não afeta outras tasks |
| **MetaInfo** | Campo `snapshot` preenchido automaticamente |

## Verificando o Modo

```python
from agrobr.datasets import is_deterministic, get_snapshot

async with datasets.deterministic("2025-12-31"):
    print(is_deterministic())  # True
    print(get_snapshot())      # "2025-12-31"

print(is_deterministic())  # False
print(get_snapshot())      # None
```

## Casos de Uso

### Papers Acadêmicos

```python
async with datasets.deterministic("2024-12-31"):
    df_precos = await datasets.preco_diario("soja")
    df_safra = await datasets.estimativa_safra("soja", safra="2024/25")
```

### Backtests

```python
async def backtest(data_corte: str):
    async with datasets.deterministic(data_corte):
        df = await datasets.preco_diario("soja")
        return calcular_estrategia(df)

resultados = [await backtest(f"2024-{m:02d}-01") for m in range(1, 13)]
```

### Auditoria

```python
df, meta = await datasets.preco_diario("soja", return_meta=True)

audit_log = {
    "snapshot": meta.snapshot,
    "source": meta.source,
    "fetched_at": meta.fetched_at.isoformat(),
    "records": meta.records_count,
    "contract": meta.contract_version,
}
```

## Thread/Async Safety

O modo determinístico usa `contextvars`, garantindo isolamento:

- Cada task async tem seu próprio contexto
- Threads diferentes não interferem
- Contextos aninhados funcionam corretamente

```python
async def task_a():
    async with datasets.deterministic("2024-01-01"):
        assert get_snapshot() == "2024-01-01"

async def task_b():
    async with datasets.deterministic("2025-01-01"):
        assert get_snapshot() == "2025-01-01"

await asyncio.gather(task_a(), task_b())
```

## Pré-Requisitos

Para reprodutibilidade total, o cache local deve conter os dados históricos:

1. Execute as consultas normalmente primeiro (popula o cache)
2. Use modo determinístico para reproduzir

```python
df = await datasets.preco_diario("soja")

async with datasets.deterministic("2025-01-15"):
    df_reproduzido = await datasets.preco_diario("soja")
```
