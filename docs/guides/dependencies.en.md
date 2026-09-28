# Dependency Policy

## Classification

### Core (required)

Installed with `pip install agrobr`:

| Dependency | Use | Pin |
|---|---|---|
| `httpx` | Async HTTP client | `>=0.28.1` |
| `httpcore` | HTTP transport security floor | `>=1.0.9` |
| `beautifulsoup4` | HTML parsing | `>=4.12.0` |
| `soupsieve` | BeautifulSoup CSS selectors | `>=2.9.0` |
| `lxml` | HTML/XML parser | `>=6.1.0` |
| `pandas` | DataFrames | `>=2.2.2` |
| `pydantic` | Model validation | `>=2.5.0` |
| `pydantic-settings` | Settings | `>=2.1.0` |
| `duckdb` | Local cache | `>=1.5.2` |
| `structlog` | Structured logging | `>=23.2.0` |
| `chardet` | Encoding detection | `>=5.2.0` |
| `typer` | CLI | `>=0.26.0` |
| `openpyxl` | Excel reading (.xlsx) | `>=3.1.0` |
| `python-calamine` | Excel fallback (Rust, ignores styles) | `>=0.3.0` |
| `xlrd` | Legacy Excel reading (.xls) | `>=2.0.1` |
| `requests` | Sync HTTP (basedosdados, utilities) | `>=2.33.0` |

### Optional

Installed via extras:

```bash
pip install agrobr[pdf]       # pdfplumber for PDFs
pip install agrobr[browser]   # Playwright for JS-heavy sites
pip install agrobr[polars]    # Polars DataFrame support
pip install agrobr[geo]       # GeoDataFrames (SICAR, deforestation, etc)
pip install agrobr[bigquery]  # BigQuery fallback (BCB/SICOR)
pip install agrobr[all]       # All optional runtime integrations
```

| Extra | Dependency | Use |
|---|---|---|
| `[pdf]` | `pdfplumber>=0.11.10` | PDF parsing |
| `[browser]` | `playwright>=1.55.1` | Sites that require JS |
| `[polars]` | `polars>=0.19.0`, `pyarrow>=14.0.1` | Polars DataFrames |
| `[bigquery]` | `basedosdados>=2.0.0` | BigQuery fallback (BCB/SICOR) |
| `[geo]` | `geopandas>=1.1.4`, `pyogrio>=0.8.0` | GeoDataFrames (SICAR, deforestation, etc) |

### Dev

```bash
pip install agrobr[dev]
```

Includes: pytest (+asyncio, cov, recording, timeout), ruff, mypy, pre-commit, pandas-stubs, types-requests, xlwt.

## Pinning Rules

- **Lower bound only** (`>=X.Y.Z`): allows compatible updates
- **No upper bound**: avoids "dependency hell" from pin conflicts
- **Exception**: if a specific version has a known bug, we use `!=`

## Adding dependencies

Criteria to accept a new core dependency:

1. **Necessary**: there's no reasonable way to implement without it
2. **Stable**: >= 1.0.0 or with a proven stability track record
3. **Maintained**: last release < 6 months ago
4. **Compatible license**: MIT, BSD, Apache 2.0
5. **No heavy transitive dependencies**: avoid complex C extensions

If a dependency is useful but not essential, it goes as an **optional extra**.

## Python

- Minimum supported: **Python 3.11**
- Main target: **Python 3.12**
- Tested in CI: 3.11, 3.12, 3.13

The minimum-version CI profile pins core dependencies and the PDF, geo, and Polars extras on Python 3.11 using `scripts/constraints-minimum.txt`, including NumPy 2. Current dependencies and core-only wheel installations are checked on Python 3.11–3.13. SIDRA uses the existing asynchronous HTTP client directly; `sidrapy` is no longer required.

`httpcore` is already installed transitively by HTTPX. Its explicit floor adds no new runtime component; version 1.0.9 requires h11 0.16 or newer. This prevents resolving the vulnerable older h11 series. The lxml and requests floors also exclude published security advisories, without claiming that those vulnerabilities were exploited through agrobr.

`soupsieve` also comes with BeautifulSoup. The explicit floor exists for 2 reasons. BeautifulSoup 4.12.0 accepts `soupsieve>1.2`, and 1.2.1 through 1.6 do not import on Python 3.10 or newer: BeautifulSoup then disables CSS selectors (`select`/`select_one`), which CONAB uses. And the previous floor, 1.6.1, accepted versions with 4 security issues from 2026 in the selector parser (ReDoS and memory); in agrobr the selector is always a literal, with no path to them, and the 2.9.0 floor is hygiene. With 2.9, the families that use CSS selectors (CONAB, IBGE, RNC and ZARC, 1,558 items) give the same result on BeautifulSoup 4.12.0 and 4.15.0.

In the geo extra, GeoPandas 1.1.4 includes the `to_postgis` fixes and hardening described in the [dependency changelog](https://geopandas.org/en/stable/docs/changelog.html#version-1-1-4-june-26-2026). Agrobr does not call this operation, but no longer permits older extra versions. CI upgrades pip, setuptools, and wheel separately: installation tools are not library runtime dependencies.

Wheel and sdist explicitly exclude local tool configuration and `.env` files, including builds from copies without `.gitignore`. The installed-package smoke check verifies that these files were not distributed.
