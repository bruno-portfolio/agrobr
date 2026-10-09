# Command line (CLI)

The `agrobr` command ships with the package. Outside the PATH, use `python -m agrobr`.

```bash
agrobr --version
agrobr cepea indicador soja --inicio 2024-01-01 --formato csv > soja.csv
agrobr health --formato json
```

## Conventions

- **Global options**, before the command: `--version` (`-v`) prints the version and exits; `--verbose` sends `INFO` logs to
  standard error.
- **Format:** `--formato` (`-o`) is the only format option. In the data commands and in `conab levantamentos` it takes
  `table` (default), `csv` and `json`; in `health`, `doctor` and `snapshot list` it takes `text` (default) and `json`.
- **Output:** data goes to standard output, in UTF-8, also when redirected on Windows. The progress notice
  (`Consultando ...`, `Listando levantamentos...`), library warnings (`Aviso: ...`, such as the CEPEA license notice) and
  error messages go to standard error. `json` comes out as a list
  of records, with ISO 8601 dates; `csv` comes out without an index. With no data, only `table` prints
  `Nenhum dado encontrado`: `csv` comes out with just the header, and `json` with the empty list (`[]`).
- **Exit code:**
  - `0`: success;
  - `1`: the query failed, or the function rejected a value the CLI passed on without checking, with the `Erro: ...`
    message on standard error (for example, `ibge pam soja --nivel invalid`, `ibge lspa soja --mes 13` and `--ultimo`
    together with `--inicio`); or `health`/`doctor` found a failing source;
  - `2`: the CLI rejected the usage before calling the function: unknown command or option, value outside the list
    (`--formato`, `--pesquisa`, `--source`), `--levantamento` below 1, `--ano` or `--mes` that is not a number.

## Data

| Command | Same as | Options |
|---|---|---|
| `agrobr cepea indicador <produto>` | `cepea.indicador` | `--inicio`/`-i` and `--fim`/`-f` (`YYYY-MM-DD` or `DD/MM/YYYY`), `--praca`, `--ultimo`/`-u`, `--formato` |
| `agrobr conab safras <produto>` | `conab.safras` | `--safra`/`-s` (`2025/26`), `--uf`/`-u`, `--levantamento` (≥ 1), `--formato` |
| `agrobr conab balanco [produto]` | `conab.balanco` | `--safra`/`-s`, `--levantamento` (≥ 1), `--formato` |
| `agrobr ibge pam <produto>` | `ibge.pam` | `--ano`/`-a` (`2023` or `2020,2021,2022`), `--uf`/`-u`, `--nivel`/`-n` (`brasil`, `uf` or `municipio`; default `uf`), `--formato` |
| `agrobr ibge lspa <produto>` | `ibge.lspa` | `--ano`/`-a`, `--mes`/`-m` (1–12), `--uf`/`-u`, `--formato` |
| `agrobr ibge censo-historico <tema>` | `ibge.censo_agro_historico` | `--ano`/`-a` (one year or a list), `--uf`/`-u`, `--nivel`/`-n` (`brasil`, `regiao` or `uf`; default `uf`), `--formato` |
| `agrobr ibge censo-municipal-1985 <tema>` | `ibge.censo_agro_municipal_1985` | `--uf`/`-u`, `--nivel`/`-n` (`uf`, `mesorregiao`, `microrregiao` or `municipio`), `--formato` |

`--ultimo` returns the latest published indicator, like `cepea.ultimo`, and does not combine with `--inicio`/`--fim`. With
it, the row has the same columns and types as the series.

## Catalogues

| Command | Lists | Options |
|---|---|---|
| `agrobr conab produtos` | the products accepted by the CONAB commands (`conab.produtos`) | — |
| `agrobr conab levantamentos` | every survey of `conab.levantamentos`, newest first, with `url`, `levantamento`, `safra`, `ano_inicio`, `ano_fim` and `data_publicacao` | `--formato` |
| `agrobr ibge produtos` | the PAM (`ibge.produtos_pam`) or LSPA (`ibge.produtos_lspa`) products | `--pesquisa`/`-p` (`pam` or `lspa`; default `pam`) |
| `agrobr ibge temas-historico` | the `censo-historico` themes (`ibge.temas_censo_agro_historico`) | — |
| `agrobr ibge temas-municipal-1985` | the `censo-municipal-1985` themes (`ibge.temas_censo_agro_municipal_1985`) | — |

## Diagnostics

| Command | What it does | Options |
|---|---|---|
| `agrobr health` | tests the connection and the response of each source; exits with `1` if any fails | `--source`/`-s` (one source, by module name: `cepea`, `mapa_psr`...), `--deep`/`-d`, `--formato` (`text` or `json`) |
| `agrobr doctor` | source status, local cache and next update; exits with `1` if any source errors | `--verbose`/`-v`, `--formato` (`text` or `json`) |
| `agrobr config show` | cache folder, database name, read timeout and total attempts in effect ([environment variables](ambiente.md)) | — |

`--deep` only changes CEPEA: it compares the page fingerprint with the package baseline and parses it. In `doctor`, `-v` is
`--verbose`; before the command, `-v` is `--version`. The `doctor` `--verbose` adds to the text the URL probed for each
source, with the result category when there is one (`slow`, `soft_block`, `api_key_missing`...), and the last
collection of each source in the cache. `json` already carries these fields and does not change with the option.

## Snapshots

| Command | What it does | Options |
|---|---|---|
| `agrobr snapshot list` | lists the saved snapshots | `--formato` (`text` or `json`) |
| `agrobr snapshot create [nome]` | creates a snapshot (default name: today's date, `YYYY-MM-DD`); needs `pyarrow` | `--sources`/`-s` (`cepea`, `conab` and `ibge`, comma-separated; default: all 3) |
| `agrobr snapshot delete <nome>` | removes a snapshot; asks for confirmation | `--force`/`-f` (no confirmation) |

The `snapshot list` `json` is a list with `name`, `created_at` (ISO 8601), `size_mb`, `sources` and `files`.

The CLI does not activate a snapshot for future runs. To read a snapshot, use Python:
`load_from_snapshot(..., snapshot_name=<name>)`, in the [snapshots guide](../guides/snapshots.md). Deterministic mode
(`datasets.deterministic("YYYY-MM-DD")`) is a separate feature: it does not read snapshot files.

## Changes in 2.0

| 1.x | 2.0 |
|---|---|
| `agrobr health --output json` | `agrobr health --formato json` |
| `agrobr doctor --json` | `agrobr doctor --formato json` |
| `agrobr snapshot list --json` | `agrobr snapshot list --formato json` |
| `agrobr conab levantamentos`: the first 10, as text, with the notice on standard output | all of them, as `table`, `csv` or `json` (`--formato`), with the notice on standard error |
| `agrobr snapshot use <nome>` | removed: it did not activate anything. Use `load_from_snapshot(..., snapshot_name=<nome>)` |

The old options exit with code `2`. The rest of the migration is in the [2.0 guide](../guides/migracao-2.md).
