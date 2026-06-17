# Docker

Run agrobr without installing Python locally.

## Requirements

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Windows/Mac) or Docker Engine (Linux)

## Build

```bash
docker build -t agrobr .
```

## Interactive use

```bash
docker run -it --rm agrobr
```

Opens a Python REPL with agrobr installed:

```python
>>> from agrobr.sync import cepea
>>> df = cepea.indicador('soja', inicio='2024-01-01')
>>> print(df.head())
```

## CLI

```bash
docker run --rm agrobr agrobr --version
docker run --rm agrobr agrobr cepea indicador boi
docker run --rm agrobr agrobr conab safras soja
```

## Persisting the cache

Without a volume mount, the DuckDB cache is lost when the container exits.

```bash
docker run -it --rm -v agrobr-cache:/home/agrobr/.agrobr agrobr
```

The named volume `agrobr-cache` persists across runs.

## Running local scripts

```bash
docker run --rm -v "$(pwd)":/work agrobr python /work/my_script.py
```

## Extras

The default image already includes the `browser` (Playwright + Chromium) and `pdf` (pdfplumber) extras, required by CONAB, ANDA, Lista Suja and Rio Verde respectively.

`--build-arg EXTRAS` **replaces** the default. To add extras, include the defaults:

```bash
docker build --build-arg EXTRAS="browser,pdf,polars" -t agrobr:extras .
```

### Compatibility

| Extra | Docker | Notes |
|---|---|---|
| `browser` | yes (default) | Playwright + Chromium. Required for CONAB |
| `pdf` | yes (default) | pdfplumber, pure Python. Required for ANDA, Lista Suja, Rio Verde |
| `polars` | yes | Pre-built manylinux wheels |
| `bigquery` | yes | Google Cloud client |
| `geo` | **uncertain** | geopandas/pyogrio may work (GDAL bundled in the wheel). Not verified |
| `app` | yes | Streamlit works, but adds ~200MB and requires `-p 8501:8501` |
| `dev` | don't use | Dev tooling (pytest, ruff, mypy) |
| `docs` | don't use | mkdocs |

## Python version

The default image uses Python 3.11. For other versions:

```bash
docker build --build-arg PYTHON_VERSION=3.12 -t agrobr:py312 .
```

Supported versions: 3.11, 3.12, 3.13.

## Environment variables

All settings are customizable via env vars:

```bash
docker run -it --rm \
  -e AGROBR_CACHE_CACHE_DIR=/data/cache \
  -e AGROBR_HTTP_TIMEOUT_READ=60 \
  -e AGROBR_HTTP_MAX_RETRIES=5 \
  -v agrobr-data:/data \
  agrobr
```

| Prefix | Setting |
|---|---|
| `AGROBR_CACHE_` | Cache directory, DuckDB database name |
| `AGROBR_HTTP_` | Timeouts, retries, per-source rate limits |
| `AGROBR_ALERT_` | Slack/Discord webhooks, SendGrid |

## Limitations

- **Ephemeral cache** without a volume mount. Use `-v agrobr-cache:/home/agrobr/.agrobr`.
- **Image ~2.3GB** (Chromium ~1.5GB + pandas ~100MB + duckdb ~80MB + other deps).
- **`agrobr/data/censo_1985/`** — 11MB of static CSVs included in the image (runtime data for `ibge.censo_municipal_1985()`).
```
