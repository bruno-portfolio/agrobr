# Environment variables

All agrobr variables start with `AGROBR_`. None is required, except the credentials of the sources that need sign-up
(USDA, MapBiomas Alerta and the INMET observational route).

## When they take effect

Set the variables before importing agrobr. In detail:

- the timeouts (`AGROBR_HTTP_TIMEOUT_*`) and the CEASA user and password are read when each client is imported;
- the CEPEA database folder and name are read on the first use of the cache, and the database stays open until the
  process ends. The other caches (ZARC, ANEC, Acervo Fundiário, IBAMA, RNC and Agrofit) read the folder on every query;
- a source's concurrency limit (`AGROBR_HTTP_MAX_CONCURRENT_*`) is read on the first request to it in the process;
- the others are read on every call.

## Cache and disk

| Variable | Default | What it does |
|---|---|---|
| `AGROBR_CACHE_DIR` | `~/.agrobr/cache` | Folder of every cache ([what goes to disk](disco.md)). Empty means the default |
| `AGROBR_CACHE_CACHE_DIR` | — | Old name of `AGROBR_CACHE_DIR`, accepted as an alias. With both set, `AGROBR_CACHE_DIR` wins; if they point to different folders, a `UserWarning` is issued |
| `AGROBR_CACHE_DB_NAME` | `agrobr.duckdb` | Name of the CEPEA DuckDB database, inside the cache folder |
| `AGROBR_ANEC_CACHE_DISABLED` | off | Boolean. When on, ANEC neither reads nor writes the PDFs in the cache |
| `AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED` | off | Boolean. When on, Acervo Fundiário neither reads nor writes the ZIPs in the cache, like `use_cache=False` |
| `AGROBR_ANEC_LIST_TTL` | `300` | Seconds the ANEC bulletin list stays in memory. A value that is not a number means `300`; a negative one means `0` |

`CacheSettings(cache_dir=...)` in code wins over the variables.

## HTTP

| Variable | Default | What it does |
|---|---|---|
| `AGROBR_HTTP_TIMEOUT_CONNECT` | `10` | Seconds to open the connection |
| `AGROBR_HTTP_TIMEOUT_READ` | `30` | Read seconds. It is a floor: a source client that sets a longer time keeps its own ([resilience](resilience.md#centralized-http-configuration)) |
| `AGROBR_HTTP_TIMEOUT_WRITE` | `10` | Write seconds |
| `AGROBR_HTTP_TIMEOUT_POOL` | `10` | Seconds waiting for a free connection |
| `AGROBR_HTTP_MAX_RETRIES` | `3` | **Total** attempts per request, counting the first. `0` means `1` (no retry); a negative value is rejected with `ValidationError` |
| `AGROBR_HTTP_RETRY_BASE_DELAY` | `1.0` | First wait between attempts, in seconds |
| `AGROBR_HTTP_RETRY_MAX_DELAY` | `30.0` | Cap of the wait between attempts, in seconds |
| `AGROBR_HTTP_RETRY_EXPONENTIAL_BASE` | `2` | Wait multiplier on each new attempt |
| `AGROBR_HTTP_RATE_LIMIT_<SOURCE>` | per source | Minimum interval, in seconds, between requests to the source, across the whole process |
| `AGROBR_HTTP_RATE_LIMIT_DEFAULT` | `1.0` | Interval of the sources without their own variable |
| `AGROBR_HTTP_MAX_CONCURRENT_<SOURCE>` | per source | Simultaneous requests to the source. Exists only for `ANA` (1), `ANP_DIESEL` (3), `B3` (3) and `IBGE` (3); a value below 1 is rejected with `ValidationError` |
| `AGROBR_HTTP_MAX_CONCURRENT_DEFAULT` | `1` | Simultaneous requests of the other sources; below 1 is rejected with `ValidationError` |

Interval `<SOURCE>`, with the default in seconds: `ABIOVE` (3), `ACERVO_FUNDIARIO` (3), `ANA` (2), `ANDA` (3), `ANEC` (3),
`ANP_DIESEL` (2), `ANTAQ` (1), `ANTT_PEDAGIO` (2), `B3` (1), `B3_ARQUIVOS` (5), `BCB` (1), `CEPEA` (5), `CFTC` (2),
`COMEXSTAT` (2), `COMTRADE` (2), `CONAB` (3), `CONAB_CEASA` (2), `DEFENSIVOS` (2), `DERAL` (3), `DESMATAMENTO` (2),
`EMBRAPA_SOLOS` (2), `FUNAI` (2), `IBAMA` (2), `IBGE` (1), `ICMBIO` (2), `IMEA` (1), `INCRA` (2), `INMET` (0.5),
`LISTA_SUJA` (2), `MAPBIOMAS` (2), `MAPBIOMAS_ALERTA` (3), `NASA_POWER` (1), `NOTICIAS_AGRICOLAS` (2), `QUEIMADAS` (1),
`RIO_VERDE` (3), `RNC` (3), `SFB` (2), `SICAR` (2), `UNICA` (3), `USDA` (1) and `ZARC` (2). A source variable outside these
lists has no effect. Retry and concurrency details are in [Resilience](resilience.md).

## Credentials

| Variable | Source | What it does |
|---|---|---|
| `AGROBR_INMET_TOKEN` | INMET | Token of the observational API (`inmet.estacao`, `inmet.clima_uf`). The historical ZIPs do not need it |
| `AGROBR_USDA_API_KEY` | USDA PSD | Required key of `usda.psd`. The `api_key=` argument wins over the variable |
| `AGROBR_COMTRADE_API_KEY` | UN Comtrade | Optional key; without it, the query uses the public preview. The `api_key=` argument wins over the variable |
| `AGROBR_MAPBIOMAS_ALERTA_TOKEN` | MapBiomas Alerta | Required token. The `token=` argument wins over the variable |
| `AGROBR_BQ_BILLING_PROJECT` | BCB/SICOR | GCP billing project for BigQuery, in the rural credit fallback (`[bigquery]` extra). Without it, basedosdados' `billing_project_id` applies |
| `AGROBR_CONAB_CEASA_USER` and `AGROBR_CONAB_CEASA_PASS` | CONAB CEASA | User and password of the CEASA price service. The default is the public access; set them only if your environment requires another one |

agrobr does not write credentials to disk. The values of these variables (except `AGROBR_BQ_BILLING_PROJECT` and
`AGROBR_CONAB_CEASA_USER`) and those of the alert webhooks and e-mail key come out as `[REDACTED]` in error messages.

## Certificates

`SSL_CERT_FILE` (file) or `SSL_CERT_DIR` (folder) replaces the certificate authorities; without them, `certifi` applies. The
clients follow the httpx convention, and SICAR and ComexStat, which build their own TLS context, read the same variables. Use
them on a network with a proxy that re-signs TLS.

## Alerts

Used by the health check and by alert delivery, which are internal infrastructure, with no SemVer guarantee
([public API](../api/index.md)).

| Variable | Default | What it does |
|---|---|---|
| `AGROBR_ALERT_ENABLED` | `true` | Boolean. When off, no alert is sent |
| `AGROBR_ALERT_SLACK_WEBHOOK` | — | Slack webhook URL |
| `AGROBR_ALERT_DISCORD_WEBHOOK` | — | Discord webhook URL |
| `AGROBR_ALERT_SENDGRID_API_KEY` | — | SendGrid key for e-mail |
| `AGROBR_ALERT_EMAIL_FROM` | `alerts@agrobr.dev` | E-mail sender |
| `AGROBR_ALERT_EMAIL_TO` | empty | Recipients, as a JSON list: `'["a@example.com"]'` |
| `AGROBR_ALERT_ALERT_ON_PARSE_ERROR`, `_LAYOUT_CHANGE`, `_SOURCE_DOWN`, `_ANOMALY`, `_SOFT_BLOCK` | `true` | Booleans: alert per failure category. The name repeats `ALERT` (prefix plus field) |
| `AGROBR_ALERT_ALERT_ON_RECOVERY` | `true` | Boolean: alert when the source comes back |
| `AGROBR_ALERT_CONSECUTIVE_FAILURES_WARNING` | `2` | Consecutive failures before the warning alert |
| `AGROBR_ALERT_CONSECUTIVE_FAILURES_CRITICAL` | `3` | Consecutive failures before the critical alert |
| `AGROBR_ALERT_DISCORD_EMBED_CHAR_LIMIT` | `3900` | Character cap of the Discord message |

## Booleans

`AGROBR_ANEC_CACHE_DISABLED` and `AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED` turn on with `1`, `true`, `yes` or `on` (and `t` or
`y`) and off with `0`, `false`, `no` or `off` (and `f` or `n`), case-insensitive. Only these 2 variables strip surrounding
spaces, and missing or empty means off. Any other value (for example, `sim`) raises `InvalidParameterError` on the query.

The alert booleans (`AGROBR_ALERT_ENABLED` and `AGROBR_ALERT_ALERT_ON_*`) take the same values, case-insensitive, but do not
strip spaces nor accept empty: `' YES '`, the empty variable and any other value raise `ValidationError`.

## Checking the configuration

`agrobr config show` prints the cache folder, the database name, the read timeout and the total attempts in effect
([CLI](cli.md)).
