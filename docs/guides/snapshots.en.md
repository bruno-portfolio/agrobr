# Snapshots and deterministic mode

Snapshots export datasets to Parquet files. Deterministic mode is a separate feature and currently affects only the `preco_diario` dataset. There is no global no-network guarantee for every dataset.

## Creating a snapshot

### Programmatically

```python
from agrobr.snapshots import create_snapshot

info = await create_snapshot()

info = await create_snapshot(
    "2025-Q4",
    sources=["cepea", "conab", "ibge"],
)
print(info.name, info.path, info.file_count)
```

`create_snapshot()` covers only these sources and files:

| Source | Files | Coverage |
|---|---|---|
| CEPEA | `cepea/<product>.parquet` | One file per product available in the DuckDB cache; collection uses `indicador(..., offline=True)` |
| CONAB | `conab/safras.parquet`, `conab/balanco.parquet` | Soybean crop estimates only, plus the supply-and-demand balance |
| IBGE | `ibge/pam.parquet`, `ibge/lspa.parquet` | PAM and LSPA for soybean only |

Creation may access the network while collecting CONAB and IBGE data. For CEPEA, it exports only data already available in the DuckDB cache.

### CLI

```bash
agrobr snapshot create
agrobr snapshot create 2025-Q4 --sources cepea,conab,ibge
```

When no name is supplied, the current date is used. Snapshots are stored under `~/.agrobr/snapshots/<name>/` unless another directory is configured.

## Listing snapshots

```python
from agrobr.snapshots import list_snapshots

for snapshot in list_snapshots():
    size_mb = snapshot.size_bytes / 1024 / 1024
    print(f"{snapshot.name} — {snapshot.file_count} files, {size_mb:.1f} MB")
    print(f"  Sources: {', '.join(snapshot.sources)}")
    print(f"  Created at: {snapshot.created_at}")
```

```bash
agrobr snapshot list
agrobr snapshot list --json
```

## Deterministic mode in `preco_diario`

The context manager makes `preco_diario` query the CEPEA DuckDB cache with `offline=True` and caps `fim` at the snapshot date. If the user supplies an earlier `fim`, that date takes precedence.

```python
from agrobr import datasets

async with datasets.deterministic("2025-12-31"):
    df = await datasets.preco_diario("soja")
    history = await datasets.preco_diario("milho", fim="2025-06-30")
```

Other datasets do not inspect this mode and may access the network normally. The context manager uses `contextvars`, so its state is safe across threads and asynchronous tasks.

### Decorator

```python
from agrobr import datasets
from agrobr.datasets.deterministic import deterministic_decorator

@deterministic_decorator("2025-12-31")
async def my_pipeline():
    return await datasets.preco_diario("soja")
```

### Snapshot configuration

```python
from agrobr.config import set_mode
from agrobr.snapshots import load_from_snapshot

set_mode("deterministic", snapshot="2025-12-31")
df = load_from_snapshot("cepea", "soja")

set_mode("normal")
```

The `snapshot` argument defines the default name used by `load_from_snapshot()`. `snapshot_path` defines the base directory used for both creation and loading. `set_mode()` does not activate the `datasets` context manager, and the configuration's `network_enabled` field does not block HTTP requests.

The `agrobr snapshot use <name>` command only validates that a snapshot exists and shows how to configure the Python process; it does not alter future executions.

## Loading data from a snapshot

```python
from agrobr.snapshots import load_from_snapshot

df = load_from_snapshot("cepea", "soja", snapshot_name="2025-Q4")
```

The second argument is the actual filename without the `.parquet` extension.

## On-disk structure

```text
~/.agrobr/snapshots/
  2025-Q4/
    manifest.json
    cepea/
      soja.parquet
      milho.parquet
      <product>.parquet
    conab/
      safras.parquet
      balanco.parquet
    ibge/
      pam.parquet
      lspa.parquet
```

## Deleting snapshots

```python
from agrobr.snapshots import delete_snapshot

delete_snapshot("2025-Q4")
```

```bash
agrobr snapshot delete 2025-Q4
agrobr snapshot delete 2025-Q4 --force
```

## Best practices

- Confirm which sources and products were recorded in `manifest.json`.
- Do not treat deterministic mode as a global network block.
- Use descriptive names such as `2025-Q4` or `paper-submission-v2`.
- In CI, create the snapshot once and reuse the same files.
