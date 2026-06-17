# Resilience and Fallbacks

agrobr is designed to be robust and resilient to failures. This document explains the implemented defense layers.

## Defense Layers

```
DEFENSE LAYERS - AGROBR

LAYER 1: PREVENTION
  ├─ Structure Monitor (6h)    → Detects changes early
  ├─ Golden Data Tests (CI)    → Ensures parsing doesn't regress
  └─ Fingerprint Baseline      → Reference for comparison

LAYER 2: DETECTION
  ├─ Fingerprint Check         → HTML structurally different?
  ├─ can_parse() Confidence    → Parser recognizes structure?
  └─ User-Agent Rotation       → Avoids IP blocking

LAYER 3: VALIDATION
  ├─ Pydantic Validation       → Correct types and formats?
  ├─ Sanity Check              → Values within range?
  └─ Completeness Check        → Partial data (< 80%)?

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
max_retries = 3
base_delay = 1.0  # seconds
max_delay = 30.0  # seconds
exponential_base = 2

# Delays: 1s → 2s → 4s (max 30s)
```

**Status codes that trigger retry:**
- 408 Request Timeout
- 429 Too Many Requests
- 500 Internal Server Error
- 502 Bad Gateway
- 503 Service Unavailable
- 504 Gateway Timeout

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

The table shows the main sources; each supported source has its own rate limit (default 1 second). Beyond the interval between requests, per-source concurrency is controlled by `max_concurrent_<source>` (default 1; B3 and IBGE use 3), via semaphores that allow parallel requests to different sources.

## Centralized HTTP Configuration

All clients use `HTTPSettings` (env prefix `AGROBR_HTTP_`):

```bash
# Timeouts (seconds)
export AGROBR_HTTP_TIMEOUT_CONNECT=10
export AGROBR_HTTP_TIMEOUT_READ=30
export AGROBR_HTTP_TIMEOUT_WRITE=10
export AGROBR_HTTP_TIMEOUT_POOL=10

# Retry
export AGROBR_HTTP_MAX_RETRIES=3
export AGROBR_HTTP_RETRY_BASE_DELAY=1.0
export AGROBR_HTTP_RETRY_MAX_DELAY=30.0
```

Via code:

```python
from agrobr.http import get_timeout

timeout = get_timeout()             # httpx.Timeout (defaults)
timeout = get_timeout(read=60.0)    # override the read timeout only
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
3. ISO-8859-1 (Latin-1, common in old BR sites)
4. UTF-16 (rare)
5. ASCII
6. Automatic detection (chardet, confidence > 0.7)
7. UTF-8 with replacement (last resort)

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

## Source Fallback

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

The local cache uses DuckDB. CEPEA expires at 18h (smart TTL); other sources use a fixed TTL per policy (e.g. CONAB 24h, IBGE 7 days). When the fetch fails, the stale cache is returned with a warning. Without a stale cache, the behavior depends on the layer: the direct source `cepea.indicador()` returns an empty DataFrame (with a warning in `MetaInfo`); the datasets (`datasets.*`, which try sources in a cascade) raise `SourceUnavailableError` when all sources are exhausted.

### Cache Flow

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

Detects layout changes before they cause errors:

**Fingerprint components:**
- Table CSS classes
- Relevant IDs (price, indicator, etc.)
- Table headers
- Count of structural elements
- Hash of the tag hierarchy

**Thresholds:**

| Similarity | Action |
|--------------|------|
| > 85% | OK, normal parsing |
| 70-85% | Warning, attempts parsing |
| < 70% | Error, layout changed too much |

## Statistical Validation

Sanity checks based on historical ranges:

```python
# Example: Soybean
min_value = 30   # R$/bag (historical minimum ~R$40)
max_value = 300  # R$/bag (historical maximum ~R$200)
max_daily_change = 15%  # Maximum daily change
```

Anomalies are flagged in the data but do not block the return (soft validation).

## Health Checks

Automatic checks:

1. **Connectivity**: Does HTTP GET respond?
2. **Latency**: < 5 seconds?
3. **Parsing**: Does the parser extract data?
4. **Fingerprint**: Structure similar to baseline?

### GitHub Actions

- **Daily Health Check**: twice a day (9h and 21h BRT)
- **Structure Monitor**: every 6 hours
- **Tests**: on every PR

## Alerts

### Supported Channels

```python
# Slack
export AGROBR_ALERT_SLACK_WEBHOOK=https://hooks.slack.com/...

# Discord
export AGROBR_ALERT_DISCORD_WEBHOOK=https://discord.com/api/webhooks/...

# Email (SendGrid)
export AGROBR_ALERT_SENDGRID_API_KEY=SG...
export AGROBR_ALERT_EMAIL_TO=["admin@example.com"]
```

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
agrobr diagnostics v1.0.2
==================================================

Sources Connectivity
  [OK] CEPEA (Noticias Agricolas)             142ms
  [OK] CONAB                                   89ms
  [OK] IBGE/SIDRA                              67ms

Cache Status
  Location:      ~/.agrobr/cache/agrobr.duckdb
  Size:          2.40 MB
  Total records: 1,247

  By source:
    CEPEA: 847 records (2025-01-21 to 2026-02-04)
    CONAB: 305 records (2024-01-01 to 2026-02-04)
    IBGE: 95 records (2020-01-01 to 2023-12-31)

Cache Expiry
  CEPEA: Expira as 18h (atualizacao CEPEA)
  CONAB: TTL 24 horas
  IBGE: TTL 7 dias

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
