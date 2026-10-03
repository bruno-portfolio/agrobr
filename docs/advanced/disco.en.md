# What agrobr writes to disk

Almost everything lives in the cache folder: `~/.agrobr/cache` by default, or the folder in the
`AGROBR_CACHE_DIR` variable ([environment variables](ambiente.md)). The exception is snapshots, in `~/.agrobr/snapshots` (or the
`snapshot_path` of `agrobr.config.set_mode`).

Deleting any file in the table below is safe: on the next query, agrobr downloads again what it needs.
The 2.0 CLI has no cleanup command. Delete with
your file manager or with `rm`.

## In the cache folder

| Source | Path | What it is | Validity and size | How to clean |
|---|---|---|---|---|
| CEPEA | `agrobr.duckdb` (the name comes from `AGROBR_CACHE_DB_NAME`) | DuckDB database with the indicators already collected, the coverage of the historical series, the migration quarantine (`indicadores_quarentena`) and the history of the stateful health check (`agrobr.health.run_checks_with_state`) | valid until CEPEA's 6 p.m. turnover; no cap: it grows with each product and period queried | delete the file. The migration quarantine goes with it: see [cache preservation](../guides/migracao-2.md#18-automatic-preservation-of-existing-caches) first |
| CEPEA, damaged database | `agrobr.duckdb.corrompido-<YYYYMMDDHHMM>` (and `agrobr.duckdb.wal.corrompido-<YYYYMMDDHHMM>`, if there was a WAL) | the database DuckDB could not read, moved aside (time in UTC); the query goes on, and a new database is created | does not expire | delete it when you no longer need it |
| ZARC | `zarc_tabuas.duckdb` | the risk tables already validated, by revision | 24 hours from acquisition; keeps up to 3 revisions, and the oldest goes | delete the file |
| ANEC | `anec/<year>/week_<NN>/shipment.pdf` and `meta.json` | the week's PDF and its metadata (SHA-256 and ANEC's publication date) | downloaded again when ANEC publishes a newer version of the week; no cap: 1 PDF (about 0.9 MB) per week queried | delete the `anec/` folder or the week's folder. `AGROBR_ANEC_CACHE_DISABLED=1` turns the cache off |
| Acervo Fundiário | `acervo_fundiario/<theme>/<UF>.zip` (or `brasil.zip`) and the `.json` next to it; themes `sigef_publico`, `sigef_privado`, `snci`, `snci_publico`, `snci_privado` and `assentamentos` | the theme's ZIP and its metadata (`etag`, `last_modified`, SHA-256) | revalidated by HEAD on every query; no cap: 1 ZIP per theme and UF | delete the theme's folder. The `sigef/` folder from earlier versions (`Sigef Brasil`) is no longer read: delete it. `use_cache=False` or `AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED=1` neither reads nor writes: the ZIP goes to a temporary file deleted when the query ends |
| IBAMA | `ibama/termo_embargo.csv` and `termo_embargo.json` | the embargo CSV and the manifest (SHA-256 and collection time) | 1 hour from collection; 1 file (~208 MB), overwritten | delete the `ibama/` folder. `use_cache=False` neither reads nor writes |
| RNC | `rnc/registradas.acquisition.v1.zip` and `rnc/protegidas.acquisition.v1.zip` | the raw CSV and the manifest | 24 hours from acquisition; 1 file per family, overwritten | delete the `rnc/` folder. `use_cache=False` neither reads nor writes |
| Agrofit (pesticides) | `defensivos/formulados.v3.zip` and `defensivos/tecnicos.v3.zip` | the tables and the manifest | 24 hours; 1 file per kind, overwritten | delete the `defensivos/` folder |

If the process dies in the middle of a write, a `.<name>.<letters>.tmp` may be left in the file's
folder. It is safe to delete.

## Snapshots

`~/.agrobr/snapshots/<name>/` holds the snapshots you create (`agrobr snapshot create`). They do not
expire. To delete: `agrobr snapshot delete <name>`. See the [snapshots guide](../guides/snapshots.md).

## What does not go to disk

- INMET's historical ZIP stays in memory, capped at 256 MB: 1 hour for the current year and 24 hours
  for a closed year.
- ComexStat, PSR and ANTT download to a system temporary file, deleted at the end of the query.
- The catalogs (ZARC, CONAB costs) stay in memory for 1 hour.
- agrobr writes no log file and no credential: `AGROBR_INMET_TOKEN` and the API keys stay only in the
  environment.

## Damaged CEPEA database

A power outage, a full disk or an antivirus in the middle of a write can leave `agrobr.duckdb`
unreadable. When DuckDB flags the file (incomplete read, checksum or invalid file), agrobr moves the
database aside, warns once with both paths (`UserWarning`) and goes on without cache; the next query
creates a new database. A file in use by another process and a full disk move nothing: the operation
goes on without cache and tries again next time. If the file cannot be moved, the warning is the
cache-unavailable one, and the way out is to close the other agrobr processes and delete the file by
hand.
