# Dependency Policy

## Classification

### Core (required)

Installed with `pip install agrobr`:

| Dependency | Use | Pin |
|---|---|---|
| `httpx` | Async HTTP client | `>=0.25.0` |
| `beautifulsoup4` | HTML parsing | `>=4.12.0` |
| `lxml` | HTML/XML parser | `>=5.0.0` |
| `pandas` | DataFrames | `>=2.0.0` |
| `pydantic` | Model validation | `>=2.5.0` |
| `pydantic-settings` | Settings | `>=2.1.0` |
| `duckdb` | Local cache | `>=1.5.0` |
| `structlog` | Structured logging | `>=23.2.0` |
| `chardet` | Encoding detection | `>=5.2.0` |
| `typer` | CLI | `>=0.9.0` |
| `openpyxl` | Excel reading (.xlsx) | `>=3.1.0` |
| `python-calamine` | Excel fallback (Rust, ignores styles) | `>=0.3.0` |
| `xlrd` | Legacy Excel reading (.xls) | `>=2.0.1` |
| `sidrapy` | IBGE SIDRA API | `>=0.1.4` |
| `requests` | Sync HTTP (basedosdados, utilities) | `>=2.32.0` |

### Optional

Installed via extras:

```bash
pip install agrobr[pdf]       # pdfplumber for PDFs
pip install agrobr[browser]   # Playwright for JS-heavy sites
pip install agrobr[polars]    # Polars DataFrame support
pip install agrobr[geo]       # GeoDataFrames (SICAR, deforestation, etc)
pip install agrobr[bigquery]  # BigQuery fallback (BCB/SICOR)
pip install agrobr[all]       # Everything (except app)
```

| Extra | Dependency | Use |
|---|---|---|
| `[pdf]` | `pdfplumber>=0.10.0` | PDF parsing |
| `[browser]` | `playwright>=1.55.1` | Sites that require JS |
| `[polars]` | `polars>=0.19.0` | Polars DataFrames |
| `[bigquery]` | `basedosdados>=2.0.0` | BigQuery fallback (BCB/SICOR) |
| `[geo]` | `geopandas>=1.0.0` | GeoDataFrames (SICAR, deforestation, etc) |
| `[app]` | `streamlit>=1.54.0` | Streamlit app/dashboard |

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
