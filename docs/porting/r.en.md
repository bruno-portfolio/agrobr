# R Developer Guide

Practical guide to accessing Brazilian agricultural data in R,
using agrobr as the reference implementation.

!!! warning "Data Licenses"
    Before implementing access to any source, check the
    [licenses page](../licenses.md). This guide includes examples
    only for sources with a `livre` or `CC BY-NC` license (non-commercial
    with attribution). For technical pitfalls of all sources
    (including restricted ones), see [Pitfalls by Source](gotchas.md).

---

## Python → R Equivalences

| Python (agrobr) | R equivalent | Package |
|-----------------|---------------|--------|
| `httpx` (async HTTP) | `httr2::request()` | httr2 |
| `BeautifulSoup` + `lxml` | `rvest::read_html()` | rvest, xml2 |
| `Playwright` (headless) | `chromote::ChromoteSession` | chromote |
| `pandas.DataFrame` | `tibble` / `data.frame` | tibble |
| `DuckDB` (cache) | `DBI` + `duckdb` | duckdb |
| `Pydantic v2` (validation) | `checkmate` or manual validation | checkmate |
| `structlog` (logging) | `logger::log_info()` | logger |
| `chardet` (encoding) | `stringi::stri_enc_detect()` | stringi |
| `openpyxl` / `calamine` / `read_excel` | `readxl::read_excel()` | readxl |
| `pdfplumber` (PDF) | `pdftools::pdf_text()` | pdftools |
| `asyncio` (parallelism) | `furrr` + `future` | furrr |

!!! note "About async"
    agrobr is async-first (`httpx` + `asyncio`). R is single-threaded,
    so sequential requests with `httr2` + `Sys.sleep()` for rate
    limiting work well. For parallelism, `furrr` + `future` helps.

---

## Existing R Packages

These packages already cover part of the scope:

| Package | What it does | Covers which source |
|--------|-----------|-----------------|
| [`sidrar`](https://CRAN.R-project.org/package=sidrar) | Access to the SIDRA/IBGE API | IBGE (PAM, LSPA, PPM) |
| [`nasapower`](https://CRAN.R-project.org/package=nasapower) | NASA POWER data | NASA POWER |
| [`GetBCBData`](https://CRAN.R-project.org/package=GetBCBData) | BCB series | BCB (partial) |
| [`rbcb`](https://github.com/wilsonfreitas/rbcb) | BCB API | BCB (partial) |
| [`deflateBR`](https://CRAN.R-project.org/package=deflateBR) | Deflate BR series | Auxiliary utility |

No R package covers CEPEA, CONAB (any module), ANDA, ABIOVE,
IMEA, DERAL, ComexStat, Desmatamento, Queimadas, MapBiomas or B3.

---

## Examples by Source

### CEPEA (headless browser)

!!! info "License: CC BY-NC 4.0"
    Free non-commercial use with attribution.

CEPEA uses Cloudflare, so `httr2` directly gets a 403.
Use `chromote` (R-native headless Chrome):

```r
library(chromote)
library(rvest)

buscar_cepea <- function(produto) {
  slugs <- list(
    soja = "soja", milho = "milho", boi = "boi-gordo",
    cafe = "cafe", algodao = "algodao", trigo = "trigo",
    arroz = "arroz", acucar = "acucar", frango = "frango",
    suino = "suino", etanol = "etanol", leite = "leite",
    laranja = "laranja"
  )
  slug <- slugs[[produto]]
  if (is.null(slug)) stop(paste("Unsupported product:", produto))

  url <- paste0("https://www.cepea.org.br/br/indicador/", slug, ".aspx")

  b <- ChromoteSession$new()
  b$Page$navigate(url = url)
  Sys.sleep(3)

  html <- b$Runtime$evaluate("document.documentElement.outerHTML")$result$value
  b$close()

  page <- read_html(html)
  tabelas <- page |> html_table()
  tabelas[[1]]
}

df_soja <- buscar_cepea("soja")
```

### CONAB CEASA (pure HTTP)

!!! info "License: Public data"
!!! tip "No browser"
    Pentaho REST API accessible with `httr2` directly.

```r
library(httr2)
library(jsonlite)

buscar_ceasa <- function(produto = NULL) {
  url <- paste0(
    "https://pentahoportaldeinformacoes.conab.gov.br",
    "/pentaho/plugin/cda/api/doQuery"
  )

  req <- request(url) |>
    req_url_query(
      path = "/public/Prohort/Precos.cda",
      dataAccessId = "precos",
      userid = "pentaho",
      password = "password"
    ) |>
    req_headers(
      `Accept` = "application/json",
      `Accept-Language` = "pt-BR"
    ) |>
    req_timeout(30) |>
    req_retry(max_tries = 3, backoff = ~ 2)

  resp <- req |> req_perform()

  dados <- resp |> resp_body_json()
  rows <- dados$resultset

  df <- do.call(rbind, lapply(rows, function(r) {
    data.frame(
      produto = r[[1]], ceasa = r[[2]], preco = r[[3]],
      stringsAsFactors = FALSE
    )
  }))

  if (!is.null(produto)) {
    df <- df[grepl(produto, df$produto, ignore.case = TRUE), ]
  }

  tibble::as_tibble(df)
}

df <- buscar_ceasa("tomate")
```

### CONAB Historical Series (pure HTTP)

!!! info "License: Public data"
!!! tip "No browser"
    Direct XLS download via fixed URLs.

```r
library(httr2)
library(readxl)

buscar_serie_historica <- function(produto) {
  urls <- list(
    soja = "https://www.gov.br/conab/.../soja/view",
    milho = "https://www.gov.br/conab/.../milho/view"
  )

  url <- urls[[produto]]
  if (is.null(url)) stop(paste("Unmapped product:", produto))

  tmp <- tempfile(fileext = ".xls")
  req <- request(url) |>
    req_headers(`User-Agent` = "Mozilla/5.0") |>
    req_timeout(60)

  resp <- req |> req_perform()
  writeBin(resp_body_raw(resp), tmp)

  readxl::read_xls(tmp)
}
```

### IBGE/SIDRA

```r
library(sidrar)

pam <- get_sidra(
  api = "/t/5457/n3/all/v/214,216/p/2023/c81/2713"
)

lspa <- get_sidra(
  api = "/t/6588/n3/all/v/214,216/p/202406/c81/2713"
)
```

!!! warning "SIDRA rate limit"
    Add `Sys.sleep(1)` between SIDRA calls.

### NASA POWER

```r
library(nasapower)

clima <- get_power(
  community = "ag",
  lonlat = c(-55.0, -12.5),
  pars = c("T2M", "T2M_MAX", "T2M_MIN", "PRECTOTCORR", "RH2M"),
  dates = c("2024-01-01", "2024-12-31"),
  temporal_api = "daily"
)
```

### ComexStat (pure HTTP)

```r
library(httr2)

buscar_exportacao <- function(ano) {
  url <- paste0(
    "https://balanca.economia.gov.br/balanca/bd/",
    "comexstat-bd/ncm/EXP_", ano, ".csv"
  )

  req <- request(url) |>
    req_headers(`User-Agent` = "Mozilla/5.0") |>
    req_timeout(120)

  resp <- req |> req_perform()

  tmp <- tempfile(fileext = ".csv")
  writeBin(resp_body_raw(resp), tmp)

  read.csv2(tmp, stringsAsFactors = FALSE)
}

df <- buscar_exportacao(2024)
```

!!! note "Semicolon separator"
    ComexStat CSVs use `;` as the separator. Use `read.csv2()` or
    `readr::read_csv2()` instead of `read.csv()`.

---

## Normalization in R

### Crops

Essential port of `agrobr/normalize/crops.py` (144 variants → 41 canonical):

```r
CULTURAS <- c(
  "soja" = "soja", "soja em grao" = "soja",
  "soja em grao" = "soja", "soybean" = "soja", "soybeans" = "soja",
  "milho" = "milho", "milho total" = "milho",
  "corn" = "milho", "maize" = "milho",
  "milho 1a safra" = "milho_1", "milho 2a safra" = "milho_2",
  "cafe" = "cafe", "coffee" = "cafe",
  "algodao" = "algodao", "cotton" = "algodao",
  "trigo" = "trigo", "wheat" = "trigo",
  "arroz" = "arroz", "rice" = "arroz",
  "feijao" = "feijao",
  "boi" = "boi", "boi gordo" = "boi", "cattle" = "boi",
  "acucar" = "acucar", "sugar" = "acucar",
  "cana" = "cana", "sugarcane" = "cana"
  # Full mapping (144 variants) in agrobr/normalize/crops.py
)

normalizar_cultura <- function(nome) {
  key <- tolower(trimws(nome))

  if (key %in% names(CULTURAS)) return(CULTURAS[[key]])

  key_sem_acento <- stringi::stri_trans_general(key, "Latin-ASCII")
  nomes_sem_acento <- stringi::stri_trans_general(names(CULTURAS), "Latin-ASCII")
  idx <- match(key_sem_acento, nomes_sem_acento)
  if (!is.na(idx)) return(CULTURAS[[idx]])

  gsub(" ", "_", key)
}

normalizar_cultura("Soja em Grao")    # "soja"
normalizar_cultura("milho 2a safra")  # "milho_2"
normalizar_cultura("ALGODAO")         # "algodao"
```

### Crop Years

```r
INICIO_SAFRA_MES <- 7L  # July

normalizar_safra <- function(safra) {
  safra <- trimws(safra)

  if (grepl("^\\d{4}/\\d{2}$", safra)) return(safra)

  if (grepl("^\\d{2}/\\d{2}$", safra)) {
    partes <- strsplit(safra, "/")[[1]]
    ano <- as.integer(partes[1])
    prefixo <- ifelse(ano >= 50, "19", "20")
    return(paste0(prefixo, partes[1], "/", partes[2]))
  }

  if (grepl("^\\d{4}/\\d{4}$", safra)) {
    partes <- strsplit(safra, "/")[[1]]
    return(paste0(partes[1], "/", substr(partes[2], 3, 4)))
  }

  stop(paste("Invalid crop-year format:", safra))
}

safra_atual <- function(data = Sys.Date()) {
  ano <- as.integer(format(data, "%Y"))
  mes <- as.integer(format(data, "%m"))
  if (mes >= INICIO_SAFRA_MES) {
    paste0(ano, "/", substr(as.character(ano + 1L), 3, 4))
  } else {
    paste0(ano - 1L, "/", substr(as.character(ano), 3, 4))
  }
}

normalizar_safra("24/25")       # "2024/25"
normalizar_safra("2024/2025")   # "2024/25"
safra_atual()                   # depends on the date
```

### Units

```r
PESO_SACA_KG <- list(sc60kg = 60, sc50kg = 50, sc40kg = 40)
PESO_ARROBA_KG <- 15
PESO_BUSHEL_KG <- list(soja = 27.2155, milho = 25.4012, trigo = 27.2155)

sacas_para_toneladas <- function(sacas, tipo = "sc60kg") {
  peso <- PESO_SACA_KG[[tipo]]
  if (is.null(peso)) stop(paste("Invalid bag type:", tipo))
  sacas * peso / 1000
}

preco_saca_para_tonelada <- function(preco_saca, tipo = "sc60kg") {
  peso <- PESO_SACA_KG[[tipo]]
  preco_saca * (1000 / peso)
}

sacas_para_toneladas(100, "sc60kg")       # 6.0
preco_saca_para_tonelada(150, "sc60kg")   # 2500
```

### Encoding

```r
library(stringi)

decodificar_response <- function(raw_bytes) {
  det <- stri_enc_detect(raw_bytes)[[1]]
  encoding <- det$Encoding[1]
  confianca <- det$Confidence[1]

  if (confianca > 0.7) {
    return(stri_encode(raw_bytes, from = encoding, to = "UTF-8"))
  }

  for (enc in c("UTF-8", "Windows-1252", "ISO-8859-1")) {
    tryCatch(
      return(stri_encode(raw_bytes, from = enc, to = "UTF-8")),
      error = function(e) NULL
    )
  }

  iconv(rawToChar(raw_bytes), from = "UTF-8", to = "UTF-8", sub = "?")
}
```

---

## Rate Limiting in R

```r
rate_limiters <- new.env(parent = emptyenv())

com_rate_limit <- function(fonte, delay_s, expr) {
  agora <- proc.time()["elapsed"]
  ultimo <- rate_limiters[[fonte]] %||% 0

  espera <- delay_s - (agora - ultimo)
  if (espera > 0) Sys.sleep(espera)

  resultado <- force(expr)
  rate_limiters[[fonte]] <- proc.time()["elapsed"]
  resultado
}

# Usage with httr2:
com_rate_limit("cepea", 5.0, {
  request("https://...") |> req_perform()
})
```

Idiomatic alternative with `httr2`:

```r
req <- request("https://apisidra.ibge.gov.br/...") |>
  req_throttle(rate = 1 / 1)  # 1 request per second
```

---

## Retry with httr2

```r
req <- request("https://...") |>
  req_retry(
    max_tries = 3,
    is_transient = \(resp) resp_status(resp) %in% c(408, 429, 500, 502, 503, 504),
    backoff = ~ 2  # exponential backoff base 2
  )
```

---

## Cache with DuckDB

```r
library(DBI)
library(duckdb)

con <- dbConnect(duckdb(), dbdir = "~/.agrobr/cache/agrobr.duckdb")

cache_get <- function(con, fonte, produto, ttl_horas = 4) {
  query <- sprintf(
    "SELECT * FROM cache
     WHERE fonte = '%s' AND produto = '%s'
     AND collected_at > NOW() - INTERVAL '%d hours'
     ORDER BY collected_at DESC LIMIT 1",
    fonte, produto, ttl_horas
  )
  tryCatch(dbGetQuery(con, query), error = function(e) NULL)
}

cache_set <- function(con, fonte, produto, dados) {
  # Create table if not exists, insert data with timestamp
  # History accumulates -- never delete old data
}
```

---

## Suggested Structure for an R Package

```
agrobr.r/
+-- DESCRIPTION
+-- NAMESPACE
+-- R/
|   +-- cepea.R              # Via chromote (CC BY-NC)
|   +-- conab_ceasa.R        # Pure HTTP (httr2)
|   +-- conab_serie.R        # Pure HTTP (httr2)
|   +-- conab_progresso.R    # Pure HTTP (httr2)
|   +-- conab_custo.R        # Pure HTTP (httr2)
|   +-- conab_safras.R       # Via chromote
|   +-- ibge.R               # Via sidrar or direct
|   +-- nasa_power.R         # Via nasapower or direct
|   +-- bcb.R
|   +-- comexstat.R          # Pure HTTP (httr2)
|   +-- normalize_crops.R    # Essential from day 1
|   +-- normalize_dates.R    # Crop years
|   +-- normalize_units.R    # Conversions
|   +-- normalize_encoding.R
|   +-- http_utils.R         # Rate limit, retry, user-agent
|   +-- cache.R              # DuckDB
+-- inst/
|   +-- golden_data/         # Copy from tests/golden_data/
|   +-- municipios_ibge.json # Copy from agrobr/normalize/_municipios_ibge.json
+-- tests/
|   +-- testthat/
|       +-- test-cepea.R
|       +-- test-conab.R
|       +-- test-normalize.R
|       +-- test-golden.R    # Validate against golden data
+-- man/
```

!!! tip "4 of the 5 CONAB modules work without a browser"
    CEASA, production cost, progress and historical series use pure HTTP.
    Only the current-crop bulletin needs `chromote`. This
    significantly simplifies an R port.

---

## Implementation Priority

| Phase | What to implement | Browser? | Existing R package? |
|:----:|-------------------|:--------:|:-------------------:|
| **1** | `normalize_crops.R` + `http_utils.R` | None | -- |
| **2** | CONAB CEASA (pure HTTP) | None | -- |
| **3** | CONAB Historical Series (pure HTTP) | None | -- |
| **4** | IBGE/SIDRA | None | `sidrar` |
| **5** | NASA POWER | None | `nasapower` |
| **6** | ComexStat (pure HTTP) | None | -- |
| **7** | CEPEA (headless) | `chromote` | -- |
| **8** | CONAB Bulletin (headless) | `chromote` | -- |
| **9** | DuckDB cache | None | -- |
| **10** | Other free sources | Varies | -- |

!!! note "Different order from Python"
    In Python, CEPEA is priority 1 because it has a fallback via Notícias
    Agrícolas (pure HTTP). In R, pure-HTTP sources should come first since
    `chromote` adds complexity. CONAB CEASA and Historical Series provide
    valuable data without any browser dependency.

---

## Resources

- **Full crop mapping:** `agrobr/normalize/crops.py`
- **Crop years and dates:** `agrobr/normalize/dates.py`
- **Unit conversion:** `agrobr/normalize/units.py`
- **States and regions:** `agrobr/normalize/regions.py`
- **IBGE municipalities (JSON):** `agrobr/normalize/_municipios_ibge.json`
- **Golden tests:** `tests/golden_data/`
- **URL mappings:** `agrobr/constants.py`
```
