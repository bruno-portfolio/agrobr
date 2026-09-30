# Troubleshooting

Guide to solving common problems.

## What to catch

Every agrobr exception inherits from `AgrobrError`. In sources and datasets, HTTP error statuses do not come out as
httpx exceptions: they come out as `SourceUnavailableError`.

| Exception | Meaning |
|-----------|---------|
| `SourceUnavailableError` | The source did not deliver the data: timeout, connection failure or HTTP error status. The message carries the source, the URL and the status ("HTTP 403: a fonte recusou o pedido (bloqueio de WAF ou permissão)", "HTTP 404: o recurso não existe na URL"), and `__cause__` keeps the original exception. In a dataset, every source failed: `attempted_sources` lists the ones tried, in order, `errors` gives the reason for each, and `__cause__` is the error of the last one |
| `ParseError` | The data arrived, but the source layout changed and the parser cannot read it |
| `InvalidParameterError` | Parameter rejected before any request; also a `ValueError` |
| `AgrobrError` | The base: catches any agrobr error |

```python
from agrobr import datasets
from agrobr.exceptions import AgrobrError, ParseError, SourceUnavailableError

try:
    df = await datasets.producao_anual("soja", ano=2023)
except SourceUnavailableError as error:
    print(error.attempted_sources, error.errors)
except ParseError:
    ...
except AgrobrError:
    ...
```

## Connection Errors

### `SourceUnavailableError`

**Cause:** Data source not reachable after all attempts.

**Solutions:**

1. Check your internet connection
2. Try offline mode:
   ```python
   df = await cepea.indicador('soja', offline=True)
   ```
3. Wait and try again (the source may be temporarily down)
4. Check whether a configured proxy might be blocking

### `SourceFallbackWarning`

**Cause:** The primary source failed, but the dataset returned data from an
alternative source. The message identifies the original source, a short failure
reason, and the fallback used. Execution continues normally.

### `TimeoutError`

**Cause:** The request took too long.

**Solutions:**

1. Increase the timeout:
   ```bash
   export AGROBR_HTTP_TIMEOUT_READ=60
   ```
2. Check your connection
3. Try during lower-traffic hours

### `403 Forbidden` (CEPEA)

**Cause:** Cloudflare blocking direct requests to CEPEA.

**Solution:** agrobr automatically uses Notícias Agrícolas as a fallback (pure httpx, no Playwright). If it still fails:

1. Try forcing a refresh:
   ```python
   df = await cepea.indicador('soja', force_refresh=True)
   ```
2. Use offline mode with cached data:
   ```python
   df = await cepea.indicador('soja', offline=True)
   ```

## Parsing Errors

### `ParseError`

**Cause:** The source layout changed and the parser cannot extract data.

**Solutions:**

1. Upgrade agrobr:
   ```bash
   pip install --upgrade agrobr
   ```
2. Check GitHub issues for known problems
3. Use cached data while the problem is fixed:
   ```python
   df = await cepea.indicador('soja', offline=True)
   ```

### Empty or Incomplete Data

**Cause:** The source returned partial data.

**Checks:**

1. Is the product correct?
   ```python
   produtos = await cepea.produtos()
   print(produtos)
   ```
2. Does the requested period have data?
3. Try a smaller period

## Validation Errors

### `InvalidParameterError`

**Cause:** A user-supplied parameter is invalid. This exception is also a
`ValueError`, preserving compatibility with code that already catches
`ValueError`, and it stops the cascade instead of masking the issue as source
unavailability.

### `ValidationError`

**Cause:** Data did not pass Pydantic or statistical validation.

**Solutions:**

1. Make sure you are using a valid product
2. Disable statistical validation if needed:
   ```python
   df = await cepea.indicador('soja', validate_sanity=False)
   ```

### Statistical Anomalies

**Cause:** Values outside the expected historical range.

With `validate_sanity=True`, anomalies are flagged in the DataFrame's `anomalies` column (they do not block the return) and logged. There is no specific exception or warning for anomalies.

**This is normal when:**
- Prices had atypical variation (market events)
- New-crop data with different volumes

**To check:**
- Compare with other sources
- Check sector news

## Cache Errors

### DuckDB Lock / Segfault in Multi-Thread

**Cause:** DuckDB's `DuckDBPyConnection` is not thread-safe. If agrobr
is used in a multi-thread process (e.g. MCP server, FastAPI with threads),
concurrent calls to the cache can cause a segfault or deadlock.

**Solution:** As of v0.10.1, `DuckDBStore` uses an internal
`threading.Lock` in all methods. If you are on an earlier version,
upgrade:

```bash
pip install --upgrade agrobr
```

### Corrupted Cache

**Cause:** `agrobr.duckdb` became unreadable (power outage, full disk or antivirus in the middle of a write).

**Solution:** agrobr moves the damaged database to `agrobr.duckdb.corrompido-<YYYYMMDDHHMM>`, warns with both paths and creates a new database on the next query. If the warning is the cache-unavailable one (the file was in use and could not be moved), close the other agrobr processes and delete the file (recreated on next use):

```bash
rm ~/.agrobr/cache/agrobr.duckdb
```

Everything agrobr writes, and how to clean it: [What agrobr writes to disk](disco.md).

### Cache Not Updating

**Cause:** A fresh cache is being returned.

**Solution:**
```python
df = await cepea.indicador('soja', force_refresh=True)
```

## Polars Issues

### `ImportError: polars not found`

**Cause:** Polars not installed.

**Solution:**
```bash
pip install agrobr[polars]
```

### Conversion Fails

**Cause:** Incompatible types in the pandas → polars conversion.

**Solution:** Use pandas (default) and convert manually if needed.

## CLI Issues

### Command not found: `agrobr`

**Cause:** CLI not installed on PATH.

**Solutions:**

1. Check the installation:
   ```bash
   pip show agrobr
   ```
2. Reinstall:
   ```bash
   pip install --force-reinstall agrobr
   ```
3. Use via Python:
   ```bash
   python -m agrobr.cli cepea indicador soja
   ```

### Encoding on Windows

**Cause:** The terminal does not support UTF-8.

**Solution:**
```bash
# PowerShell
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# Or export to a file
agrobr cepea indicador soja --formato csv > soja.csv
```

## Debug

### Enable Detailed Logs

As a library, agrobr never writes logs to standard output. Logs go through the standard library's `logging`, as JSON: without
configuration, only warnings and errors come out, on standard error.

```bash
# Via CLI (logs go to standard error)
agrobr --verbose cepea indicador soja
```

```python
import logging

logging.basicConfig(level=logging.DEBUG)             # all logs, on standard error
logging.getLogger("agrobr").setLevel(logging.INFO)   # or only agrobr's level
```

agrobr does not configure structlog: your application's structlog configuration applies only to its own logs, and agrobr's
stay in `logging`.

### View Current Configuration

```bash
agrobr config show
```

### Check Source Health

```bash
agrobr health           # all sources
agrobr health --deep    # deep check (does real parsing)
```

### Inspect Cache

```bash
agrobr doctor
```

## Reporting Bugs

If the problem persists:

1. Check whether an issue already exists: https://github.com/bruno-portfolio/agrobr/issues
2. Collect information:
   ```bash
   python --version
   pip show agrobr
   agrobr doctor
   ```
3. Open an issue with:
   - Python and agrobr versions
   - Operating system
   - Code that triggers the error
   - Full error message
   - Debug logs (if possible)

## FAQ

### Does agrobr work in Jupyter?

Yes! Use the async version directly:
```python
df = await cepea.indicador('soja')
```

Or the sync version:
```python
from agrobr.sync import cepea
df = cepea.indicador('soja')
```

### Can I use it with proxies?

There is currently no native support. Consider configuring a proxy at the system level.

### Is the data free?

Yes, all sources are public and free. agrobr just makes access easier.

### How often is the data updated?

| Source | Frequency |
|-------|------------|
| CEPEA | Daily (~18h) |
| CONAB | Monthly |
| IBGE PAM | Annual |
| IBGE LSPA | Monthly |
