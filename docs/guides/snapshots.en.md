# Snapshots and Deterministic Mode

Snapshots capture local data into parquet files, enabling full reproducibility in papers, audits and CI pipelines. No HTTP request is made during queries in deterministic mode.

## Creating a Snapshot

### Programmatic

```python
from agrobr.snapshots import create_snapshot

# Automatic name (current date)
info = await create_snapshot()

# Custom name + specific sources
info = await create_snapshot("2025-Q4", sources=["cepea", "conab", "ibge"])
print(info.name, info.path, info.file_count)
```

### CLI

```bash
# Automatic name
agrobr snapshot create

# Custom name with sources
agrobr snapshot create 2025-Q4 --sources cepea,conab,ibge
```

Snapshots are saved under `~/.agrobr/snapshots/<name>/` with a `manifest.json` and parquet files per source/dataset.

## Listing Snapshots

```python
from agrobr.snapshots import list_snapshots

for s in list_snapshots():
    print(f"{s.name} — {s.file_count} files, {s.size_bytes/1024/1024:.1f} MB")
    print(f"  Sources: {', '.join(s.sources)}")
    print(f"  Created at: {s.created_at}")
```

```bash
agrobr snapshot list
agrobr snapshot list --json    # structured output
```

## Using a Snapshot (Deterministic Mode)

### Context Manager (recommended)

```python
from agrobr import datasets

async with datasets.deterministic("2025-12-31"):
    # All queries filter data <= snapshot
    # Uses local cache only — no network
    df = await datasets.preco_diario("soja")
    df2 = await datasets.producao_anual("milho", ano=2023)
```

The context manager uses `contextvars`, making it thread-safe and async-safe.

### Decorator

```python
from agrobr import datasets
from agrobr.datasets.deterministic import deterministic_decorator

@deterministic_decorator("2025-12-31")
async def meu_pipeline():
    df = await datasets.preco_diario("soja")
    return df
```

!!! note "Context manager vs CLI"
    The `deterministic()` context manager and the decorator take an **ISO date** (e.g. `"2025-12-31"`) and filter queries by date.
    The `snapshot use` CLI takes the **snapshot name**, validates that it exists and shows how to enable deterministic mode in code — the mode is per Python process, so a CLI command cannot enable it for future runs.

### CLI

```bash
agrobr snapshot use 2025-Q4
# Validates the snapshot and shows how to enable it in code
```

### Global configuration

```python
from agrobr.config import set_mode

set_mode("deterministic", snapshot="2025-12-31")

# Back to normal
set_mode("normal")
```

## Loading Data from a Snapshot

```python
from agrobr.snapshots import load_from_snapshot

df = load_from_snapshot("cepea", "indicador", snapshot_name="2025-Q4")
```

## Removing Snapshots

```python
from agrobr.snapshots import delete_snapshot

delete_snapshot("2025-Q4")
```

```bash
agrobr snapshot delete 2025-Q4
agrobr snapshot delete 2025-Q4 --force   # no confirmation
```

## On-Disk Structure

```
~/.agrobr/snapshots/
  2025-Q4/
    manifest.json          # metadata (name, date, sources, agrobr version)
    cepea/
      indicador.parquet
    conab/
      safras.parquet
      balanco.parquet
    ibge/
      pam.parquet
```

## Best Practices

- Create snapshots after fully collecting the needed data
- Use descriptive names (e.g. `2025-Q4`, `paper-submission-v2`)
- In CI, create the snapshot once and reuse it across all jobs
- Combine `deterministic()` with tests to ensure reproducibility
