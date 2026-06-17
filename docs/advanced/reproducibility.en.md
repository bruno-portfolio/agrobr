# Reproducibility

agrobr enables 100% reproducible analyses.

## Deterministic Mode

```python
from agrobr import datasets

async with datasets.deterministic(snapshot="2025-12-31"):
    df = await datasets.preco_diario("soja")
```

Also available as a decorator:

```python
from agrobr.datasets.deterministic import deterministic_decorator

@deterministic_decorator("2025-12-31")
async def meu_pipeline():
    df = await datasets.preco_diario("soja")
    return df
```

## Snapshot Semantics

| Aspect | Definition |
|---------|-----------|
| **Format** | `"YYYY-MM-DD"` — maximum cutoff date |
| **Filter** | Datasets with snapshot support (e.g. `preco_diario`) filter by `data <= snapshot` |
| **Network** | Datasets with snapshot support query the local cache only (offline) |
| **Scope** | Isolated per async context (contextvars) — does not affect other tasks |
| **MetaInfo** | `snapshot` field filled automatically in all datasets |

!!! note "Per-dataset support"
    The date filter and offline mode are applied by datasets that implement snapshot support (currently `preco_diario`). The others record the `snapshot` in `MetaInfo` for provenance but query the sources normally.

## Checking the Mode

```python
from agrobr.datasets import is_deterministic, get_snapshot

async with datasets.deterministic("2025-12-31"):
    print(is_deterministic())  # True
    print(get_snapshot())      # "2025-12-31"

print(is_deterministic())  # False
print(get_snapshot())      # None
```

## Use Cases

### Academic Papers

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

### Auditing

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

Deterministic mode uses `contextvars`, ensuring isolation:

- Each async task has its own context
- Different threads do not interfere
- Nested contexts work correctly

```python
async def task_a():
    async with datasets.deterministic("2024-01-01"):
        assert get_snapshot() == "2024-01-01"

async def task_b():
    async with datasets.deterministic("2025-01-01"):
        assert get_snapshot() == "2025-01-01"

await asyncio.gather(task_a(), task_b())
```

## Prerequisites

For full reproducibility, the local cache must contain the historical data:

1. Run the queries normally first (populates the cache)
2. Use deterministic mode to reproduce

```python
df = await datasets.preco_diario("soja")

async with datasets.deterministic("2025-01-15"):
    df_reproduzido = await datasets.preco_diario("soja")
```
