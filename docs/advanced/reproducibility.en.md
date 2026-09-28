# Reproducibility

agrobr provides local-cache queries and metadata to support reproducible analyses. Temporal guarantees depend on the dataset and preservation of the queried data.

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
| **Network** | Datasets with snapshot support query the local cache only (offline); without the product in the cache, `preco_diario` raises `SourceUnavailableError` |
| **Scope** | Isolated per async context (contextvars) — does not affect other tasks |
| **MetaInfo** | `snapshot` records the context in datasets that accept this mode; alone, it does not establish a historical version |
| **Warning** | A dataset that queries the current source inside the context warns in `validation_warnings` and with a `UserWarning` that the data is not that date's |

!!! note "Per-dataset support"
    `preco_diario` applies the date filter and offline mode. Other datasets may record the context while querying current sources; this does not freeze historical revisions, and the warning in `validation_warnings` says so. `cadastro_rural` rejects the context before network access because SICAR WFS provides current registry records and its incremental filters do not reconstruct past state. Check each dataset's contract.

For `clima`, the context supplies only the default year in state mode when `ano` is omitted. A query with snapshot `2001-06-01` may include December 2001. Station mode retains its explicit interval; neither mode freezes source revisions or forces offline execution. `meta.source_details.deterministic` states these limits. For INMET ZIPs, `source_details.resources` records URL, SHA-256, acquisition timestamp, cache use, and selected members; preserve the corresponding files or results to reproduce that edition. The process cache expires and does not replace a research archive.

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
df_safra.to_parquet("estimativa_safra_2024_25.parquet")
```

`estimativa_safra` is not frozen: CONAB revises the estimate at every survey, and the dataset queries the current one
(inside the context, it says so in `validation_warnings`). Keep the file you used, or a [snapshot](../guides/snapshots.md),
alongside the paper.

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

Without the product in the cache, `preco_diario` raises `SourceUnavailableError`; a period with no data, with the
product in the cache, comes back empty.

```python
df = await datasets.preco_diario("soja")

async with datasets.deterministic("2025-01-15"):
    df_reproduzido = await datasets.preco_diario("soja")
```
