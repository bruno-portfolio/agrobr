# Guia para Desenvolvedores R

Guia prático para acessar dados agrícolas brasileiros em R,
usando o agrobr como referência de implementação.

!!! warning "Licenças dos Dados"
    Antes de implementar acesso a qualquer fonte, consulte a
    [página de licenças](../licenses.md). Este guia inclui exemplos
    apenas para fontes com licença `livre` ou `CC BY-NC` (não-comercial
    com atribuição). Para armadilhas técnicas de todas as fontes
    (incluindo as restritas), veja [Armadilhas por Fonte](gotchas.md).

---

## Equivalências Python → R

| Python (agrobr) | R equivalente | Pacote |
|-----------------|---------------|--------|
| `httpx` (async HTTP) | `httr2::request()` | httr2 |
| `BeautifulSoup` + `lxml` | `rvest::read_html()` | rvest, xml2 |
| `Playwright` (headless) | `chromote::ChromoteSession` | chromote |
| `pandas.DataFrame` | `tibble` / `data.frame` | tibble |
| `DuckDB` (cache) | `DBI` + `duckdb` | duckdb |
| `Pydantic v2` (validação) | `checkmate` ou validação manual | checkmate |
| `structlog` (logging) | `logger::log_info()` | logger |
| `chardet` (encoding) | `stringi::stri_enc_detect()` | stringi |
| `openpyxl` / `read_excel` | `readxl::read_excel()` | readxl |
| `pdfplumber` (PDF) | `pdftools::pdf_text()` | pdftools |
| `asyncio` (paralelismo) | `furrr` + `future` | furrr |

!!! note "Sobre async"
    O agrobr é async-first (`httpx` + `asyncio`). R é single-thread,
    então requests sequenciais com `httr2` + `Sys.sleep()` para rate
    limiting funcionam bem. Para paralelismo, `furrr` + `future` ajuda.

---

## Pacotes R Existentes

Estes pacotes já cobrem parte do escopo:

| Pacote | O que faz | Cobre qual fonte |
|--------|-----------|-----------------|
| [`sidrar`](https://CRAN.R-project.org/package=sidrar) | Acesso à API SIDRA/IBGE | IBGE (PAM, LSPA, PPM) |
| [`nasapower`](https://CRAN.R-project.org/package=nasapower) | Dados NASA POWER | NASA POWER |
| [`GetBCBData`](https://CRAN.R-project.org/package=GetBCBData) | Séries BCB | BCB (parcial) |
| [`rbcb`](https://github.com/wilsonfreitas/rbcb) | API BCB | BCB (parcial) |
| [`deflateBR`](https://CRAN.R-project.org/package=deflateBR) | Deflacionar séries BR | Utilidade auxiliar |

Nenhum pacote R cobre CEPEA, CONAB (nenhum módulo), ANDA, ABIOVE,
IMEA, DERAL, ComexStat, Desmatamento, Queimadas, MapBiomas ou B3.

---

## Exemplos por Fonte

### CEPEA (headless browser)

!!! info "Licença: CC BY-NC 4.0"
    Uso não-comercial livre com atribuição.

O CEPEA usa Cloudflare, então `httr2` direto recebe 403.
Usar `chromote` (headless Chrome nativo do R):

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
  if (is.null(slug)) stop(paste("Produto nao suportado:", produto))

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

### CONAB CEASA (HTTP puro)

!!! info "Licença: Dados públicos"
!!! tip "Sem browser"
    API REST do Pentaho acessível com `httr2` direto.

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

### CONAB Série Histórica (HTTP puro)

!!! info "Licença: Dados públicos"
!!! tip "Sem browser"
    Download direto de XLS via URLs fixas.

```r
library(httr2)
library(readxl)

buscar_serie_historica <- function(produto) {
  urls <- list(
    soja = "https://www.gov.br/conab/.../soja/view",
    milho = "https://www.gov.br/conab/.../milho/view"
  )

  url <- urls[[produto]]
  if (is.null(url)) stop(paste("Produto nao mapeado:", produto))

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

!!! warning "Rate limit SIDRA"
    Adicione `Sys.sleep(1)` entre chamadas ao SIDRA.

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

### ComexStat (HTTP puro)

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

!!! note "Separador ponto e vírgula"
    CSVs do ComexStat usam `;` como separador. Usar `read.csv2()` ou
    `readr::read_csv2()` em vez de `read.csv()`.

---

## Normalização em R

### Culturas

Porte essencial do `agrobr/normalize/crops.py` (156 variantes → 41 canônicos):

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
  # Mapeamento completo (156 variantes) em agrobr/normalize/crops.py
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

### Safras

```r
INICIO_SAFRA_MES <- 7L  # Julho

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

  stop(paste("Formato de safra invalido:", safra))
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
safra_atual()                   # depende da data
```

### Unidades

```r
PESO_SACA_KG <- list(sc60kg = 60, sc50kg = 50, sc40kg = 40)
PESO_ARROBA_KG <- 15
PESO_BUSHEL_KG <- list(soja = 27.2155, milho = 25.4012, trigo = 27.2155)

sacas_para_toneladas <- function(sacas, tipo = "sc60kg") {
  peso <- PESO_SACA_KG[[tipo]]
  if (is.null(peso)) stop(paste("Tipo de saca invalido:", tipo))
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

  for (enc in c("UTF-8", "ISO-8859-1", "Windows-1252")) {
    tryCatch(
      return(stri_encode(raw_bytes, from = enc, to = "UTF-8")),
      error = function(e) NULL
    )
  }

  iconv(rawToChar(raw_bytes), from = "UTF-8", to = "UTF-8", sub = "?")
}
```

---

## Rate Limiting em R

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

# Uso com httr2:
com_rate_limit("cepea", 2.0, {
  request("https://...") |> req_perform()
})
```

Alternativa idiomática com `httr2`:

```r
req <- request("https://apisidra.ibge.gov.br/...") |>
  req_throttle(rate = 1 / 1)  # 1 request por segundo
```

---

## Retry com httr2

```r
req <- request("https://...") |>
  req_retry(
    max_tries = 3,
    is_transient = \(resp) resp_status(resp) %in% c(408, 429, 500, 502, 503, 504),
    backoff = ~ 2  # exponential backoff base 2
  )
```

---

## Cache com DuckDB

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
  # Criar tabela se nao existe, inserir dados com timestamp
  # Historico acumula -- nunca deletar dados antigos
}
```

---

## Estrutura Sugerida para Pacote R

```
agrobr.r/
+-- DESCRIPTION
+-- NAMESPACE
+-- R/
|   +-- cepea.R              # Via chromote (CC BY-NC)
|   +-- conab_ceasa.R        # HTTP puro (httr2)
|   +-- conab_serie.R        # HTTP puro (httr2)
|   +-- conab_progresso.R    # HTTP puro (httr2)
|   +-- conab_custo.R        # HTTP puro (httr2)
|   +-- conab_safras.R       # Via chromote
|   +-- ibge.R               # Via sidrar ou direto
|   +-- nasa_power.R         # Via nasapower ou direto
|   +-- bcb.R
|   +-- comexstat.R          # HTTP puro (httr2)
|   +-- normalize_crops.R    # Essencial desde o dia 1
|   +-- normalize_dates.R    # Safras
|   +-- normalize_units.R    # Conversoes
|   +-- normalize_encoding.R
|   +-- http_utils.R         # Rate limit, retry, user-agent
|   +-- cache.R              # DuckDB
+-- inst/
|   +-- golden_data/         # Copiar de tests/golden_data/
|   +-- municipios_ibge.json # Copiar de agrobr/normalize/_municipios_ibge.json
+-- tests/
|   +-- testthat/
|       +-- test-cepea.R
|       +-- test-conab.R
|       +-- test-normalize.R
|       +-- test-golden.R    # Validar contra golden data
+-- man/
```

!!! tip "4 dos 5 módulos CONAB funcionam sem browser"
    CEASA, custo de produção, progresso e série histórica usam HTTP puro.
    Apenas o boletim de safras correntes precisa de `chromote`. Isso
    simplifica significativamente um port em R.

---

## Prioridade de Implementação

| Fase | O que implementar | Browser? | Pacote R existente? |
|:----:|-------------------|:--------:|:-------------------:|
| **1** | `normalize_crops.R` + `http_utils.R` | Nenhum | -- |
| **2** | CONAB CEASA (HTTP puro) | Nenhum | -- |
| **3** | CONAB Série Histórica (HTTP puro) | Nenhum | -- |
| **4** | IBGE/SIDRA | Nenhum | `sidrar` |
| **5** | NASA POWER | Nenhum | `nasapower` |
| **6** | ComexStat (HTTP puro) | Nenhum | -- |
| **7** | CEPEA (headless) | `chromote` | -- |
| **8** | CONAB Boletim (headless) | `chromote` | -- |
| **9** | Cache DuckDB | Nenhum | -- |
| **10** | Demais fontes livres | Varia | -- |

!!! note "Ordem diferente do Python"
    No Python, CEPEA é prioridade 1 por ter fallback via Notícias Agrícolas
    (HTTP puro). Em R, fontes HTTP puro devem vir primeiro pois `chromote`
    adiciona complexidade. CONAB CEASA e Série Histórica fornecem dados
    valiosos sem nenhuma dependência de browser.

---

## Recursos

- **Mapeamento de culturas completo:** `agrobr/normalize/crops.py`
- **Safras e datas:** `agrobr/normalize/dates.py`
- **Conversão de unidades:** `agrobr/normalize/units.py`
- **UFs e regiões:** `agrobr/normalize/regions.py`
- **Municípios IBGE (JSON):** `agrobr/normalize/_municipios_ibge.json`
- **Golden tests:** `tests/golden_data/`
- **Mapeamentos de URLs:** `agrobr/constants.py`
