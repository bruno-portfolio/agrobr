# Snapshots and deterministic mode

Snapshots export datasets to Parquet files. Deterministic mode is separate: `preco_diario` applies a date cutoff and offline execution, while other datasets may use the context only to select a year or record provenance. There is no global no-network guarantee for every dataset.

For `clima`, the context year is the state-mode default when `ano` is omitted. Data is not truncated at the snapshot date and INMET/NASA revisions are not frozen; station mode retains `inicio`/`fim`. `meta.source_details.deterministic` states these limits. Export through `create_snapshot()` remains restricted to the sources listed below and does not include climate ZIPs.

## Creating a snapshot

Install a Parquet engine: `pip install "pyarrow>=14.0.1"` (or `fastparquet`). The `agrobr[polars]` extra already includes `pyarrow` with that floor. Without an engine, creation raises `ImportError` before creating directories.

Only `cepea`, `conab`, and `ibge` are accepted in `sources`; other values raise `ValueError`. If no files are produced, the newly created directory is removed and `SnapshotError` reports errors by source. The CLI exits with code 1. Partial snapshots are preserved, with errors stored under `metadata.errors` in `manifest.json`.

### Programmatically

Collection runs in a temporary directory under the snapshot root. The final name only appears after the manifest is written; cancellation and publication failures clean up staging so the name can be retried. Existing snapshots are never overwritten. An abrupt process or system interruption can leave a hidden staging directory: it is not listed as a completed snapshot and can be inspected before manual removal.

New snapshots record SHA-256 for each file. `load_from_snapshot()` checks the hash before reading Parquet and raises `SnapshotError` on a mismatch. Legacy snapshots without hashes remain readable without this integrity guarantee. Hashes detect file changes but do not authenticate data provenance.

**Load only snapshots from a trusted origin.** `pyarrow` before 14.0.1 executes code when reading a malicious Parquet file (CVE-2023-47248), which is why the floor is 14.0.1. SHA-256 checks the files against the snapshot's own `manifest.json`, which travels with them: whoever receives someone else's snapshot checks the `manifest.json` SHA-256 against what the author published through another channel.

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

The context manager makes `preco_diario` query the CEPEA DuckDB cache with `offline=True` and caps `fim` at the snapshot date. If the user supplies an earlier `fim`, that date takes precedence. Without the product in the cache, it raises `SourceUnavailableError`.

```python
from agrobr import datasets

async with datasets.deterministic("2025-12-31"):
    df = await datasets.preco_diario("soja")
    history = await datasets.preco_diario("milho", fim="2025-06-30")
```

The context does not guarantee offline operation for other datasets; each contract defines its behavior. Datasets that query the current source warn in `validation_warnings` and with a `UserWarning` that the data is not that date's. `cadastro_rural` rejects `deterministic` before network access because SICAR filters query current records and do not retrieve historical registry versions. The context manager uses `contextvars`, so its state is safe across threads and asynchronous tasks.

The [four Agrofit datasets](../api/defensivos_datasets.en.md) also reject this context before cache or HTTP access. Their CSVs are current exports, and a cache bundle identifies the original acquisition; neither reconstructs an arbitrary historical register. Without the context, normal cache use remains available without claiming a historical snapshot.

The [`empregadores_lista_suja` dataset](../api/empregadores_lista_suja.en.md) likewise rejects `deterministic` before I/O. It reads the current MTE publication without persistent cache; its local registration ID, update date, and content hash do not retrieve an arbitrary historical edition. Returned metadata has `snapshot=None`.

The [`uso_do_solo` dataset](../contracts/uso_do_solo.en.md) rejects `deterministic` before I/O in every mode. `colecao=10` or `11` selects a MapBiomas collection without reconstructing the file available on an arbitrary date. Each call acquires the corresponding publication and returns `snapshot=None`; hashes identify the received resources.

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
- Use descriptive names such as `2025-Q4` or `paper-submission-v2`. Windows reserved names (`CON`, `NUL`, `COM1`…)
  and names ending in a dot are rejected on every system, so the snapshot opens on Windows.
- In CI, create the snapshot once and reuse the same files.
