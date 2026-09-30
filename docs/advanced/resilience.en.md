# Resilience and Fallbacks

agrobr is designed to be robust and resilient to failures. This document explains the implemented defense layers.

## Defense Layers

```
DEFENSE LAYERS - AGROBR

LAYER 1: PREVENTION
  ├─ Structure Monitor (6h)    → Detects changes early
  ├─ Golden Data Tests (CI)    → Ensures parsing doesn't regress
  └─ Fingerprint Baseline      → Reference for health --deep

LAYER 2: DETECTION
  ├─ R$ value column           → Parser rejects a page without it
  ├─ Fingerprint               → Only in health --deep and CI
  ├─ can_parse() Confidence    → Parser recognizes structure?
  └─ User-Agent Rotation       → Avoids IP blocking

LAYER 3: VALIDATION
  ├─ Pydantic Validation       → Correct types and formats?
  └─ Sanity Check (optional)   → validate_sanity=True: flags and warns

LAYER 4: FALLBACK
  ├─ Parser Cascade            → Tries next parser
  ├─ Cache Fallback            → Returns stale cache
  └─ Source Fallback           → Alternative source (NA)

LAYER 5: ALERTS
  ├─ Multi-channel             → Slack, Discord, Email
  ├─ GitHub Issue              → Automatic tracking
  └─ Structured Logging        → Easier debug
```

## Retry with Exponential Backoff

All HTTP requests use automatic retry:

```python
# Default configuration
max_retries = 3  # total attempts per request, counting the first one
base_delay = 1.0  # seconds
max_delay = 30.0  # seconds
exponential_base = 2

# 3 attempts, with waits of 1s → 2s (max 30s)
```

`max_retries` counts the total number of attempts, not only the new ones: the default 3 makes up to 3 requests, with 2
waits. `AGROBR_HTTP_MAX_RETRIES=0` (or `1`) makes a single request, with no retry; a negative value is rejected at
validation, with `ValidationError`. In `retry_async` and `with_retry`, `max_attempts=0` also means one attempt,
`base_delay=0` and `max_delay=0` mean no wait, and a negative value raises `InvalidParameterError`. When the attempts run
out, the message says `after N attempts`.

**Status codes that trigger retry:**
- 408 Request Timeout
- 429 Too Many Requests
- 500 Internal Server Error
- 502 Bad Gateway
- 503 Service Unavailable
- 504 Gateway Timeout

Beyond the HTTP status and minimum size, responses are validated before they
reach parsers. JSON clients convert maintenance HTML, WAF blocks, and empty
bodies into `SourceUnavailableError`. Downloads verify ZIP/XLSX, XLS, and PDF
magic bytes; CSV files reject content that starts as HTML. This prevents a
source outage from being misclassified as a parser layout failure.

## Rate Limiting

Each source has its own rate limit, configurable via env vars:

| Source | Interval | Env var |
|-------|-----------|---------|
| ABIOVE | 3 seconds | `AGROBR_HTTP_RATE_LIMIT_ABIOVE` |
| ANDA | 3 seconds | `AGROBR_HTTP_RATE_LIMIT_ANDA` |
| BCB | 1 second | `AGROBR_HTTP_RATE_LIMIT_BCB` |
| CEPEA | 5 seconds | `AGROBR_HTTP_RATE_LIMIT_CEPEA` |
| ComexStat | 2 seconds | `AGROBR_HTTP_RATE_LIMIT_COMEXSTAT` |
| CONAB | 3 seconds | `AGROBR_HTTP_RATE_LIMIT_CONAB` |
| DERAL | 3 seconds | `AGROBR_HTTP_RATE_LIMIT_DERAL` |
| IBGE | 1 second | `AGROBR_HTTP_RATE_LIMIT_IBGE` |
| IMEA | 1 second | `AGROBR_HTTP_RATE_LIMIT_IMEA` |
| INMET | 0.5 second | `AGROBR_HTTP_RATE_LIMIT_INMET` |
| NASA POWER | 1 second | `AGROBR_HTTP_RATE_LIMIT_NASA_POWER` |
| Notícias Agrícolas | 2 seconds | `AGROBR_HTTP_RATE_LIMIT_NOTICIAS_AGRICOLAS` |
| USDA | 1 second | `AGROBR_HTTP_RATE_LIMIT_USDA` |
| ZARC | 2 seconds | `AGROBR_HTTP_RATE_LIMIT_ZARC` |
| Default | 1 second | `AGROBR_HTTP_RATE_LIMIT_DEFAULT` |

The table shows the main sources; each supported source has its own rate limit (default 1 second). Four internal requests have no variable of their own and use `AGROBR_HTTP_RATE_LIMIT_DEFAULT`: CONAB's production cost and historical series, IBGE's legacy Agricultural Census (FTP), and MAPA PSR; `AGROBR_HTTP_RATE_LIMIT_CONAB` and `AGROBR_HTTP_RATE_LIMIT_IBGE` do not apply to them. Beyond the interval between requests, per-source concurrency is controlled by `AGROBR_HTTP_MAX_CONCURRENT_<SOURCE>` for four sources only: ANA (1), ANP Diesel (3), B3 (3), and IBGE (3); the others use `AGROBR_HTTP_MAX_CONCURRENT_DEFAULT` (1), and another source's variable (for example, `AGROBR_HTTP_MAX_CONCURRENT_CFTC`) has no effect. A value below 1 is rejected at validation, with `ValidationError`. Concurrency is enforced via semaphores that allow parallel requests to different sources. The interval and the concurrency hold for the whole process: across `agrobr.sync` calls (each with its own `asyncio.run`), across loops and across threads. The wait for a slot across threads is capped (`AGROBR_HTTP_TIMEOUT_READ`); at the cap, the request goes ahead with a warning, and the interval still holds.

## Centralized HTTP Configuration

All clients use `HTTPSettings` (env prefix `AGROBR_HTTP_`):

```bash
# Timeouts (seconds)
export AGROBR_HTTP_TIMEOUT_CONNECT=10
export AGROBR_HTTP_TIMEOUT_READ=30
export AGROBR_HTTP_TIMEOUT_WRITE=10
export AGROBR_HTTP_TIMEOUT_POOL=10

# Retry (MAX_RETRIES is the total number of attempts; 0 or 1 = no retry)
export AGROBR_HTTP_MAX_RETRIES=3
export AGROBR_HTTP_RETRY_BASE_DELAY=1.0
export AGROBR_HTTP_RETRY_MAX_DELAY=30.0
```

Source clients set the read timeout above the 30 s default: ComexStat 120 s; ZARC, PSR and SICAR 180 s;
INMET 600 s; the others between 30 and 300 s, in each `client.py`'s `get_timeout(read=...)`.
`AGROBR_HTTP_TIMEOUT_READ` works as a minimum: when larger than the client's value, it wins; when smaller,
the client's value stays. `CONNECT`, `WRITE` and `POOL` apply to every source HTTP client.

Via code:

```python
from agrobr.http import get_timeout

timeout = get_timeout()             # httpx.Timeout (defaults)
timeout = get_timeout(read=60.0)    # 60 s read, or AGROBR_HTTP_TIMEOUT_READ if larger
```

## Rotating User-Agent

Pool of real, current User-Agents:

- Chrome Windows (multiple versions)
- Chrome Mac
- Firefox Windows/Mac
- Edge
- Safari

Deterministic rotation per source to look like natural traffic.

## Encoding Fallback

Fallback chain for encoding:

1. UTF-8 (default)
2. Windows-1252 (CP1252, Excel BR default — superset of Latin-1, comes first because ISO-8859-1 decodes any byte and would kill the rest of the chain)
3. ISO-8859-1 (Latin-1, common in old BR sites). It decodes any byte sequence, so the chain ends there: no step after
   it would ever run.

## Excel Engine Fallback

XLSX spreadsheets from government sources may contain malformed styles/fills
that crash openpyxl (a known bug since 2021, with no upstream fix).

agrobr automatically falls back to `python-calamine` (Rust engine, MIT):

```
openpyxl (styles + data)
        ↓ failed (malformed stylesheet)?
calamine (ignores styles, extracts data only)
        ↓ failed?
ParseError
```

xlrd guard: OLE2/BIFF files (.xls) use xlrd directly, with no calamine fallback.

Helpers: `open_excel_safe()` (multi-sheet) and `read_excel_safe()` (single-sheet)
in `agrobr/utils/io.py`.

## Expansion limit (ZIP and XLSX)

A small compressed file can expand to gigabytes. Before decompressing, agrobr checks how much each ZIP member and each XLSX
expands against a per-source limit (`constants.MAX_EXPANDED_BYTES`), and raises `ResourceLimitError` when it goes over:

- **ZIP** (Queimadas, B3, MapBiomas, ANTAQ and the IBGE legacy census): the member goes through `read_zip_member` or
  `open_zip_member`, which check the declared size. `zipfile` never returns more than the declared size: a member that expands
  beyond it fails the CRC.
- **XLSX** (`read_excel_safe`, `open_excel_safe`, the CEPEA series, the MapBiomas municipal file and UNICA):
  `check_xlsx_expansion` adds up the declared size of the members and checks the CRC of each one as a stream, before the
  spreadsheet reader. calamine does not honour the declared size: without the CRC, an XLSX with a forged size would get through.

| Source | Limit | Largest file published (as of 2026-09-27) |
|---|---|---|
| Queimadas | 2 GiB | 2024 yearly CSV: 905 MB |
| B3 | 512 MiB | 13 MB inner ZIP; 144 MB XML |
| MapBiomas | 1 GiB | municipal XLSX: 267 MB |
| ANTAQ | 4 GiB | not measured: the site has been down since 2026-06-23 |
| IBGE (legacy census, FTP) | 16 MiB | 122 KB |
| ANP | 512 MiB | 2022–2023 price sheet: 152 MB |
| ABIOVE, UNICA, CONAB and CONAB progress | 64 MiB | 0.8 MB, 1.0 MB, 3.9 MB and 0.2 MB |
| Other sources | 256 MiB | the CEPEA series, DERAL and the CONAB historical series publish XLS, which is not compressed |

The CONAB production cost and the INMET history have their own limit (`CONAB_CUSTOS_MAX_EXPANDED_BYTES` and
`INMET_HISTORICO_MAX_*`). PDF has no limit: pdfplumber decompresses each stream in full.

## Request outside the source host

`conab.progresso_safra(semana_url=...)` only follows pages under `https://www.gov.br/conab/`. Any other URL, or a redirect or
link that leaves it, raises `InvalidParameterError` before the request goes out. In an application that passes the end user's
URL along, this closes requests to internal addresses.

## Source Fallback

When a dataset's primary source fails and a later source succeeds, agrobr emits
`SourceFallbackWarning` with the primary source, the error category and summary,
and the selected fallback. The notice uses `warnings.warn`, so standard Python
tools can capture or filter it, and it goes to stderr.

### CEPEA

```
CEPEA (www.cepea.org.br)
        ↓ blocked (Cloudflare)?
Notícias Agrícolas (httpx direct, SSR)
        ↓ soft block (consent/challenge page)?
        ↓ failed (HTTP error)?
Local cache (DuckDB)
```

Notícias Agrícolas republishes the same CEPEA/ESALQ indicators via server-side rendered HTML, with no need for Playwright.

Each step returns a `FetchResult(html, source)` that explicitly identifies the HTML origin ("cepea", "browser" or "noticias_agricolas"), avoiding fragile detection by content markers.

**Soft block detection:** Some users receive from NA a consent/challenge page (HTTP 200, ~10KB without a table) instead of the data page (~75KB with a table). The NA client validates the content before returning: if the HTML is < 20KB and does not contain `<table`, it raises `SourceUnavailableError`, activating the cache fallback.

## Cache

The local cache uses DuckDB and holds only CEPEA indicators, including the Notícias Agrícolas fallback rows. They expire at the first 18h BRT (21h UTC) turnover on a business day after collection (smart TTL). Other sources do not use this cache: IBGE, BCB, ComexStat, and SICAR query the source on every call, and those with a cache of their own (INMET, ZARC, RNC, and the CONAB cost catalog, among others) describe it on their page. When the CEPEA fetch fails, the stale cache is returned with `StaleDataWarning`. With no network and no cache, the behavior depends on the layer: the direct source `cepea.indicador()` raises `SourceUnavailableError` (up to 1.1.0, it returned an empty table); the datasets (`datasets.*`, which try sources in a cascade) raise `SourceUnavailableError` when all sources are exhausted.

### Cache Flow

Internal cache fallback also emits `SourceFallbackWarning` in datasets;
promoting it to an error interrupts the query. Warm/offline cache is not a new
fallback attempt. `MetaInfo.selected_source="cache"` identifies local retrieval,
while `data_sources` retains the providers of returned rows.

Migration failures differ from cache-opening unavailability: `CacheMigrationError`
interrupts access instead of silently degrading to empty data. The connection stays
open only during each operation, so another process (a 2nd notebook, a worker)
uses the same cache; the upsert writes all or nothing. Without access to the file
(read-only folder, full disk, another process writing), the operation goes on
without cache, and agrobr warns once (`UserWarning`) with the path, the reason and
the `AGROBR_CACHE_DIR` hint. An unreadable database (incomplete read, checksum or
invalid file) is moved aside as `agrobr.duckdb.corrompido-<YYYYMMDDHHMM>`, with a warning, and the
next query creates a new database ([what agrobr writes to disk](disco.md)). Pending migrations
preserve originals in quarantine and remove active rows only in the same
transaction that records the version. See [preservation and
recovery](../guides/migracao-2.md#18-automatic-preservation-of-existing-caches).

```
Request
   │
   ▼
Cache fresh? ──yes──→ Returns cache
   │no
   ▼
Fetch source
   │
   ├─success──→ Updates cache
   │
   └─fail──→ Stale cache? ──yes──→ Returns stale + warning
                 │no
                 ▼
           cepea.indicador() → empty DataFrame
           datasets.* → SourceUnavailableError
```

## Layout Fingerprinting

Compares the structure of the CEPEA page with a baseline. It runs only in `agrobr health --deep`
and in the CI Structure Monitor: collection (`cepea.indicador`, `datasets.preco_diario`) does not
compare fingerprints. During collection, the defense against layout changes is the parser: without
the R$ value column recognized by its header, it raises `ParseError`, and the query moves on to
Notícias Agrícolas (when enabled) and then to the cache. A US$ header never becomes a price in reais.

**Fingerprint components:**
- Table CSS classes
- Relevant IDs (price, indicator, etc.)
- Table headers
- Count of structural elements
- Hash of the tag hierarchy

**`health --deep` thresholds:**

| Similarity | Check result |
|--------------|------|
| > 85% | `ok` |
| 70-85% | `warning` (drift) |
| < 70% | `failed` (layout changed too much) |

The `health --deep` baseline ships with the package (`agrobr/health/baselines/cepea_baseline.json`,
the soybean page of 2026-09-27), and the check works from any folder. Without it, or when the page
came from Notícias Agrícolas, the check returns `warning` with the reason, without comparing.

## Statistical Validation

All 22 CEPEA identifiers have unit and value range rules. Enable these checks
with `cepea.indicador(produto, validate_sanity=True)`. An incompatible unit
produces `unit_mismatch` before numeric comparisons; currency, weight and cents
are not converted implicitly.

```python
from agrobr.validators.sanity import PRICE_RULES

rule = PRICE_RULES["soja"]
print(rule.expected_unit)         # BRL/sc60kg
print(rule.min_value)             # 30
print(rule.max_value)             # 300
print(rule.max_daily_change_pct)  # 15
```

| Product | Expected unit | Inclusive range | Maximum temporal change |
|---|---|---|---|
| `soja`, `soja_parana` | BRL/sc60kg | 30–300 | 15% |
| `milho` | BRL/sc60kg | 15–150 | 15% |
| `cafe`, `cafe_arabica` | BRL/sc60kg | 200–3000 | 10% |
| `cafe_robusta` | BRL/sc60kg | 100–3000 | 10% |
| `bezerro` | BRL/cabeca | 800–8000 | 10% |
| `boi`, `boi_gordo` | BRL/@ | 100–500 | 10% |
| `trigo` | BRL/ton | 20 × 1000/60 to 150 × 1000/60 | 15% |
| `algodao` | cBRL/lb | 50 × 100 × 0.45359237/15 to 250 × 100 × 0.45359237/15 | 10% |
| `arroz` | BRL/sc50kg | 8–300 | no limit |
| `acucar` | BRL/sc50kg | 8–400 | no limit |
| `acucar_refinado` | BRL/kg | 0.2–8 | no limit |
| `frango_congelado`, `frango_resfriado` | BRL/kg | 0.6–20 | no limit |
| `suino` | BRL/kg | 0.8–30 | no limit |
| `etanol_hidratado` | BRL/L | 0.1–8 | no daily limit; weekly series |
| `etanol_anidro` | BRL/L | 0.1–10 | no daily limit; weekly series |
| `leite` | BRL/L | 0.1–8 | no daily limit; monthly series |
| `laranja_industria`, `laranja_in_natura` | BRL/cx40.8kg | 4–300 | no limit |

These ranges are engineering choices and require revision as markets change.
For rice, sugars, chickens, live hogs, ethanols and milk, the new limits use half
the positive minimum and twice the maximum in the official history available
in September 2026, rounded outward to one significant digit. Published milk
zeros are excluded from calibration: `Indicador` already requires a positive
price.

Citrus uses individual official references from 2014/2015 and 2024, without a
complete historical scan. The nine preexisting rules were not recalibrated.
Paraná soybean inherits the soybean range policy while keeping its regional
series; arabica coffee is an alias of coffee. No temporal threshold was inferred
for the other eleven newly covered products.

Temporal comparisons separate product, market and unit; chicken, ethanol and
orange products remain distinct series. Ranges are not confidence intervals
and cannot detect every scale error. `validate_sanity=True` adds anomalies to
the response without rejecting it, with a summary of the flagged rows (how many and
which rules) in `MetaInfo.validation_warnings` and a `UserWarning`;
`validate_batch(..., strict=True)` rejects
critical anomalies. Custom rules may omit `expected_unit` to retain their
previous behavior.

## Health Checks

Automatic checks:

1. **Connectivity**: Does HTTP GET respond?
2. **Latency**: < 5 seconds?
3. **Parsing**: Does the parser extract data?
4. **Fingerprint** (CEPEA, `--deep` only): structure similar to the packaged baseline?
   Without a baseline, or when the page came from Notícias Agrícolas, the check returns
   `warning` with the reason.

HTTP requests that take more than 5 seconds are retried once. Health checks use
the lower latency and record the first measurement in `cold_start_ms`, preventing
the cold start of services such as ANA's ArcGIS from counting as a failure.
Comtrade is probed through the public guest endpoint without requiring an API key.

The workflow persists counters in DuckDB, closes the store before saving the
cache, and sends the previous failure count with recovery alerts. If corruption,
a lock, or permissions prevent the store from opening, the run reports the
degraded state and fails instead of silently resetting the counters.

### GitHub Actions

- **Daily Health Check**: twice a day (9h and 21h BRT)
- **Structure Monitor**: every 6 hours
- **Tests**: on every PR
- **Weekly Reconciliation**: Mondays at 6h BRT (below)

### Weekly reconciliation

The `reconciliacao.yml` workflow runs the `scripts/reconciliar_*.py` scripts against the
official sources, one at a time, and gives each source a state:

- `ok`;
- `mismatch`;
- `indisponível`: the source is down or blocked the runner;
- `não verificado`: the credential is missing, or the script is outside the weekly run (no live
  mode, for instance). It is never a success;
- `erro do script`.

The summary is kept as a run artifact. The job fails on `mismatch` or `erro do script`.
Locally, `python -m scripts.reconciliacao_semanal imea` runs one source and writes the summary
and the script's full JSON to `reports/reconciliacao_semanal/`.

Each source with `mismatch` gets the issue "Reconciliação semanal: mismatch em <fonte>". While
it stays open, the following weeks comment on it. The issue carries only the state, the counts,
the case identifiers and the artifact link. The details, which may contain source values, come
from the local run.

## Alerts

### Supported Channels

```python
# Slack
export AGROBR_ALERT_SLACK_WEBHOOK=https://hooks.slack.com/...

# Discord
export AGROBR_ALERT_DISCORD_WEBHOOK=https://discord.com/api/webhooks/...

# Email (SendGrid); the list goes as JSON, in single quotes in the shell
export AGROBR_ALERT_SENDGRID_API_KEY=SG...
export AGROBR_ALERT_EMAIL_TO='["admin@example.com"]'
```

The Slack and Discord webhook URL is the credential itself. When the application turns on `httpx` `INFO` logging, agrobr
replaces it with `[REDACTED]` in the `HTTP Request` line of the alert delivery. The error message for a non-JSON response
also masks agrobr's credentials, whether from the environment or passed as an argument.

### Alert Levels

| Level | Trigger | Channels |
|-------|---------|--------|
| Info | Health check OK | Logs only |
| Warning | Fingerprint drift, stale cache | Slack/Discord |
| Critical | Parse failed, source down | All + GitHub Issue |

## Offline Mode

To work without a connection:

```python
df = await cepea.indicador('soja', offline=True)
```

Uses the local cache only.

## Doctor Command

Use the `doctor` command to diagnose system health:

```bash
agrobr doctor
```

### Sample output

```
agrobr diagnostics v2.0.0
==================================================

Sources Connectivity
  [OK] CEPEA (Noticias Agricolas)             142ms
  [OK] CONAB                                   89ms
  [OK] IBGE/SIDRA                              67ms

Cache Status
  Location:      ~/.agrobr/cache/agrobr.duckdb
  Size:          2.40 MB
  Total records: 1,152

  By source:
    CEPEA: 847 records (2025-01-21 to 2026-02-04)
    NOTICIAS_AGRICOLAS: 305 records (2024-01-01 to 2026-02-04)

Cache Expiry
  CEPEA: Expira às 18h BRT (atualização CEPEA)

Configuration
  Alternative source: enabled (Notícias Agrícolas via httpx)

[OK] All systems operational
```

### JSON Output

For integration with monitoring systems:

```bash
agrobr doctor --json
```

### Verbose

For detailed information:

```bash
agrobr doctor --verbose
```
