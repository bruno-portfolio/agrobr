# Pitfalls by Source

Everything that will break if you port agrobr looking only at the README.
These pitfalls are **language-independent** — they apply to R, Julia,
JavaScript or any other implementation.

!!! warning "Data Licenses"
    This guide documents **technical** pitfalls. Before implementing access
    to any source, check the [licenses page](../licenses.md).

---

## Cross-cutting HTTP Infrastructure

Before the individual sources: problems that affect **all** of them.

### Encoding

BR government sources mix encodings without declaring them correctly
in the `Content-Type` header.

| Source | Real encoding | What it declares |
|-------|--------------|---------------|
| CEPEA | Windows-1252 or ISO-8859-1 | UTF-8 (wrong) |
| CONAB | UTF-8 (Excel, calamine fallback) | -- |
| IBGE/SIDRA | UTF-8 | Correct |
| B3 | ISO-8859-1 | -- |
| DERAL | ISO-8859-1 (old XLS) | None |
| INMET | UTF-8 | Correct |
| Notícias Agrícolas | ISO-8859-1 | Variable |
| ComexStat | UTF-8 (CSV) | Correct |

agrobr uses a fallback chain of 5 encodings + automatic detection
with `chardet` (threshold > 0.7). If everything fails, it forces UTF-8 with replacement.

```
ENCODING_CHAIN = ("utf-8", "windows-1252", "iso-8859-1", "utf-16", "ascii")
```

!!! warning "Without handling, names break"
    "Feijão", "Açúcar", "São Paulo", "Paraná" turn into unreadable characters
    if the encoding is interpreted wrong.

### Excel Engine (openpyxl → calamine)

XLSX spreadsheets from government sources (CONAB, ANP, etc.) may contain
malformed styles/fills that crash openpyxl (a known bug since 2021,
pandas#40499, with no upstream fix). agrobr uses `python-calamine` (Rust, MIT)
as an automatic fallback: it ignores styles, extracts only data. OLE2/BIFF
files (.xls, e.g. DERAL, legacy historical series) use xlrd directly.

If you port to another language, make sure your Excel lib tolerates
invalid styles or has an equivalent fallback.

### Rate Limiting

Government sources block IPs if they receive requests too fast.
The interval is **per source**, not global.

| Source | Minimum interval | Sensitivity |
|-------|-----------------|---------------|
| IBGE/SIDRA | 1.0s | High |
| INMET | 0.5s | Medium |
| BCB/SICOR | 1.0s | Medium |
| CEPEA | 5.0s | High (Cloudflare) |
| CONAB | 3.0s | Medium |
| CONAB CEASA | 2.0s | Medium |
| ANDA | 3.0s | Low |
| ABIOVE | 3.0s | Low |
| NASA POWER | 1.0s | Low |
| USDA | 1.0s | Low |
| IMEA | 1.0s | Medium |
| ComexStat | 2.0s | Medium |
| DERAL | 3.0s | Low |
| Notícias Agrícolas | 2.0s | Medium |
| B3 | 1.0s | Medium |
| Desmatamento | 2.0s | Low |
| MapBiomas | 2.0s | Low |
| Queimadas | 1.0s | Low |
| ZARC | 2.0s | Low |

### Retry and Backoff

- **Retriable status codes:** `408, 429, 500, 502, 503, 504`
- **Backoff:** `delay = min(base * 2^attempt, max_delay)`
- **Retry-After:** respect the header when present (usually on 429)
- **Maximum:** 3 attempts
- **Retriable exceptions:** timeout, network error, remote protocol

### User-Agent

CEPEA and Notícias Agrícolas block generic User-Agents.
agrobr keeps a pool of **10 User-Agents** (Chrome, Firefox, Edge, Safari)
with round-robin rotation per source. Required headers:

- `Accept-Language: pt-BR,pt;q=0.9`
- `Sec-Fetch-Dest: document`
- `Sec-Fetch-Mode: navigate`

ComexStat also requires a browser User-Agent (Mozilla).

### Timeouts

| Setting | Default value |
|-------------|-------------|
| Connection | 10s |
| Read | 30s |
| Write | 10s |
| Pool | 10s |
| ComexStat (read) | 120s (large CSVs) |
| USDA (read) | 60s |
| ABIOVE (read) | 60s |
| ANDA (read) | 60s |

---

## CEPEA (Prices)

!!! info "License: CC BY-NC 4.0"
    Free non-commercial use with attribution to CEPEA.
    Commercial use requires authorization: cepea@usp.br

!!! danger "Cloudflare"
    The CEPEA site uses Cloudflare protection that blocks direct HTTP requests
    with status 403.

**agrobr fallback chain:**

```
direct httpx → Notícias Agrícolas (restricted)
```

Headless Playwright is an **optional/internal** fallback (`_use_browser=False` by default); when enabled, it sits between direct httpx and Notícias Agrícolas. It is not part of the default chain.

**Headless browser anti-detection:**

- `--disable-blink-features=AutomationControlled`
- Property masking: `navigator.webdriver → undefined`
- Viewport: 1920x1080
- Locale: pt-BR, timezone America/Sao_Paulo

**Indicator URLs:**

Base: `https://www.cepea.org.br/br/indicador/{slug}.aspx`

| agrobr | URL Slug |
|--------|---------|
| `soja` | soja |
| `milho` | milho |
| `boi` / `boi_gordo` | boi-gordo |
| `cafe` / `cafe_arabica` | cafe |
| `algodao` | algodao |
| `trigo` | trigo |
| `arroz` | arroz |
| `acucar` / `acucar_refinado` | acucar |
| `frango_congelado` / `frango_resfriado` | frango |
| `suino` | suino |
| `etanol_hidratado` / `etanol_anidro` | etanol |
| `leite` | leite |
| `laranja_industria` / `laranja_in_natura` | laranja |

Total: 20 mappings → 13 indicator pages.

**Layout fingerprinting:**

agrobr generates an MD5 hash of the DOM structure and compares it with a baseline.
Weighted score:

| Component | Weight |
|-----------|------|
| Table headers | 30% |
| Overall structure | 25% |
| Table classes | 20% |
| Key IDs | 15% |
| Element count | 10% |

Thresholds: > 85% OK, 70-85% warning, < 70% layout changed.

---

## CONAB — Crop Bulletin

!!! info "License: Public data"

!!! warning "Requires headless browser"
    The bulletin page is rendered with JavaScript.
    Of the 5 CONAB modules, **only this one** needs a browser.

**Flow:**

1. Navigate to `gov.br/conab/.../boletim-da-safra-de-graos`
2. Wait ~3s for JavaScript to render
3. Extract XLSX links via regex: `{N}o-levantamento-safra-{YYYY}-{YY}/...\.xlsx`
4. Download the Excel file (also via browser — simulates a click)

**Excel parsing — dynamic position:**

- Each product has a **tab** with a specific name
- The header row is **not at a fixed position** — search dynamically
- Data starts 3 rows below the found header
- CONAB adds/removes columns from previous crop years without notice

**Product → Excel tab mapping (25 products):**

| agrobr | Excel tab |
|--------|----------|
| `soja` | Soja |
| `milho` | Milho Total |
| `milho_1` | Milho 1a |
| `milho_2` | Milho 2a |
| `milho_3` | Milho 3a |
| `arroz` | Arroz Total |
| `arroz_irrigado` | Arroz Irrigado |
| `arroz_sequeiro` | Arroz Sequeiro |
| `feijao` | Feijão Total |
| `feijao_1` | Feijão 1a Total |
| `feijao_2` | Feijão 2a Total |
| `feijao_3` | Feijão 3a Total |
| `algodao` | Algodao Total |
| `algodao_pluma` | Algodao em Pluma |
| `trigo` | Trigo |
| `sorgo` | Sorgo |
| `aveia` | Aveia |
| `cevada` | Cevada |
| `canola` | Canola |
| `girassol` | Girassol |
| `mamona` | Mamona |
| `amendoim` | Amendoim Total |
| `centeio` | Centeio |
| `triticale` | Triticale |
| `gergelim` | Gergelim |

---

## CONAB — CEASA/PROHORT (Wholesale Produce Prices)

!!! info "License: Public data (public API)"

!!! tip "Pure HTTP"
    Direct access via the Pentaho REST API — **no headless browser needed**.

**Endpoint:** `https://pentahoportaldeinformacoes.conab.gov.br/pentaho/plugin/cda/api/doQuery`

- 48 products (fruits, vegetables, eggs)
- 43 CEASAs with mapping by state
- Returns JSON
- Rate limit: 2.0s

---

## CONAB — Production Cost

!!! info "License: Public data"

!!! tip "Pure HTTP"
    HTML scraping on gov.br + XLSX download — no browser.

- Detailed costs per hectare: COE, COT, CT
- By crop, state, crop year and technology level
- Excel spreadsheets with a specific layout

---

## CONAB — Weekly Crop Progress

!!! info "License: Public data"

!!! tip "Pure HTTP"
    Plone portal scraping (gov.br) + XLSX download — no browser.

- Weekly sowing and harvest percentage
- By crop, state and week
- Comparison with the previous year and the 5-year average
- Portal pagination to list available weeks

---

## CONAB — Historical Series

!!! info "License: Public data"

!!! tip "Pure HTTP"
    Direct XLS download via pre-mapped URLs — no browser.

- Data from ~1976 to the current crop
- ~60 products with a product → URL mapping
- Planted area, production and yield by state and region
- XLS format (Excel 97-2003) or XLSX (with calamine fallback)

---

## IBGE/SIDRA (PAM, LSPA, PPM, Slaughter, Census)

!!! info "License: Public data"

The SIDRA API is the most stable source, but it has quirks:

- **Strict rate limit** — 1s between requests, blocks quickly
- The response format changes depending on the parameter combination
- CSV responses with the header on line 1

**Table codes:**

| Dataset | Table | Geographic level |
|---------|--------|-----------------|
| PAM | 5457 | N1, N2, N3, N6 |
| LSPA | 6588 | N1, N2, N3 |
| PPM | 3939 | N1, N2, N3, N6 |
| Slaughter | 1093 | N1, N3 |
| Agricultural Census | 6780 | N1, N2, N3, N6 |
| PEVS Silviculture | 291 | N1, N2, N3, N6 |
| PEVS Silviculture Area | 5930 | N1, N2, N3, N6 |
| PEVS Plant Extraction | 289 | N1, N2, N3, N6 |
| Quarterly Milk | 1086 | N1, N3 |
| Agro GDP (current) | 1846 | N1 |
| Agro GDP (real) | 6612 | N1 |

Levels: `N1` (Brazil), `N2` (Region), `N3` (State), `N6` (Municipality)

### Historical Series Agricultural Census (tables 263-283, 1730, 1731)

**Maximum territorial level:** State (N3). Municipal **does NOT exist** in SIDRA
for these tables — it returns an error or empty data.

**Mixed units per category:**

- Table 281 (animal headcount): Poultry = "Mil cabeças" (thousand head), others = "Cabeças" (head)
- Table 282 (animal production): Milk = "Mil litros" (thousand liters), Eggs = "Mil dúzias" (thousand dozens), Wool = "Toneladas" (tonnes)
- Tables 283, 1730, 1731: unit depends on the product (Toneladas, Mil frutos, Mil cachos)

**Classifications without Total (`sumarizacao=false`):**
Tables 281, 282, 283, 1730, 1731 have no "Total" category in the classification.
You cannot request aggregation via SIDRA — sum manually if needed.

**Missing values:** `".."` = unavailable, `"..."` = suppressed, `"-"` = not applicable,
`"X"` = confidential. All must become NaN.

**`v/all` vs `v/allxp` parameter:** use `v/all` to include percentage variables.
`v/allxp` silently excludes percentages.

**Invalid `c/all` parameter:** use `c{ID}/all` (e.g. `c220/all`), not `c/all`.

---

## NASA POWER (Climate)

!!! info "License: CC BY 4.0"

Clean REST API, no authentication. The easiest source.

- **Per-request limit:** 366 days — paginate for long series
- **Coordinates:** point (lat/lon), not polygon
- Rate limit: 1s

**Available parameters:**

| Parameter | Description |
|-----------|-----------|
| `T2M` | Mean temperature at 2m |
| `T2M_MAX` | Maximum temperature |
| `T2M_MIN` | Minimum temperature |
| `PRECTOTCORR` | Precipitation |
| `ALLSKY_SFC_SW_DWN` | Solar radiation |
| `RH2M` | Relative humidity |
| `WS2M` | Wind speed |

---

## BCB/SICOR (Rural Credit)

!!! info "License: Public data"

- Public API via OData (`olinda.bcb.gov.br`) — functional but slow
- **BigQuery fallback:** public dataset when OData fails
- Rate limit: 1s

---

## ComexStat (Exports)

!!! info "License: Public data"

**Access:** annual bulk CSV download.

**URL:** `https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_{YYYY}.csv`

- **Separator:** semicolon (`;`), not comma
- **Extended timeout:** 120s (files can be large)
- **Required User-Agent:** Mozilla (the site filters generic agents)
- **Filter by NCM:** 8-digit code for a specific product
- Returns an empty list on 404 (handles nonexistent years gracefully)

**Products mapped via NCM:** soybean (grain, oil, meal), corn, rice,
wheat, cotton, coffee (arabica, conilon), sugar, ethanol, meats
(beef, poultry, pork).

---

## USDA PSD (International Estimates)

!!! info "License: Public data"

!!! danger "API key required"
    Requires a free key from [api.data.gov/signup](https://api.data.gov/signup).
    Without a key, it returns HTTP 401.

**REST API:** `https://apps.fas.usda.gov/OpenData/api`

- Header: `API_KEY: {key}`
- agrobr env var: `AGROBR_USDA_API_KEY`
- Uses USDA commodity codes (e.g. `"2222000"` for soybean)
- Returns JSON
- Timeout: 60s
- Rate limit: 1s

---

## INMET (Meteorology)

!!! info "License: Public data"

!!! danger "Silent failure without token"
    Without `AGROBR_INMET_TOKEN`, the API returns **HTTP 204 (No Content)**
    — not 401, not 403. The request "works" but returns empty.

- API declared as unstable
- Recommendation: use **NASA POWER** as an alternative

---

## DERAL (Paraná Crops)

!!! info "License: Public data"

- Direct URL to `.xls` (Excel 97-2003, not XLSX)
- **Encoding:** ISO-8859-1
- Parsing specific to the spreadsheet layout
- Rate limit: 3s

---

## B3 Agro Futures

!!! warning "Gray area"
    No specific terms of use for programmatic access.
    Redistribution in a commercial product should be checked with B3
    (marketdata@b3.com.br).

**Access:** BVBG-086 ZIP/XML.

**URL:** `https://www.b3.com.br/pesquisapregao/download?filelist=PR{yymmdd}.zip`

- Nested ZIP: outer → inner (`BVBG086.zip`) → XML snapshots (use the last one, definitive)
- XML namespace: `urn:bvmf.217.01.xsd`
- Parsing via `lxml.etree.iterparse` (streaming)
- Rate limit: 1s
- Weekend handling required (the exchange does not operate)

**Available contracts:**

| BMF code | agrobr | Product |
|-----------|--------|---------|
| BGI | `boi` | Live Cattle |
| CCM | `milho` | Corn |
| ICF | `cafe_arabica` | Arabica Coffee |
| CNL | `cafe_conillon` | Conillon Coffee |
| ETH | `etanol` | Hydrous Ethanol |
| SJC | `soja_cross` | Soybean Cross |
| SOY | `soja_fob` | Soybean FOB |

---

## IMEA (Mato Grosso)

!!! danger "Redistribution prohibited"
    IMEA's terms of use prohibit sharing data without written
    authorization. API not officially documented.

- Endpoint: `api1.imea.com.br/api/v2/mobile/cadeias`
- Discovered by reverse engineering — no public documentation
- Returns JSON
- 6 chains: soybean, corn, cotton, cattle, timber, rice
- Rate limit: 1s

---

## ABIOVE (Soybean Complex Exports)

!!! warning "Gray area"
    No public terms of use. Formal authorization pending.

- **Access:** XLSX download by month/year
- URL: `https://abiove.org.br/abiove_content/Abiove/exp_{YYYYMM}.xlsx`
- Fallback: tries months in descending order (12→1) if the month is unspecified
- Monthly publication, not always on schedule
- Timeout: 60s
- Rate limit: 3s

---

## ANDA (Fertilizers)

!!! warning "Gray area"
    No public terms of use. Formal authorization pending.

- **Indirect access:** HTML scraping to extract PDF links, then PDF parsing
- URL: `https://anda.org.br/recursos/`
- Search by keywords "entrega" or "fertilizante" in the links
- **Optional dependency:** requires `pdfplumber` (`pip install agrobr[pdf]`)
- Timeout: 60s
- Rate limit: 3s

---

## Notícias Agrícolas (CEPEA Fallback)

!!! danger "Restrictive license"
    All rights reserved (Law 9.610/98). **Not recommended as a
    primary source in ports.** Prefer CEPEA directly via headless browser.

URL mapping (not standardized, must be hardcoded):

| agrobr | Path |
|--------|------|
| `soja` | `soja/soja-indicador-cepea-esalq-porto-paranagua` |
| `soja_parana` | `soja/indicador-cepea-esalq-soja-parana` |
| `milho` | `milho/indicador-cepea-esalq-milho` |
| `boi` | `boi-gordo/boi-gordo-indicador-esalq-bmf` |
| `cafe` | `cafe/indicador-cepea-esalq-cafe-arabica` |
| `algodao` | `algodao/algodao-indicador-cepea-esalq-a-prazo` |
| `trigo` | `trigo/preco-medio-do-trigo-cepea-esalq` |
| `arroz` | `arroz/arroz-em-casca-esalq-bbm` |
| `acucar` | `sucroenergetico/acucar-cristal-cepea` |
| `acucar_refinado` | `sucroenergetico/acucar-refinado-amorfo` |
| `etanol_hidratado` | `sucroenergetico/indicador-semanal-etanol-hidratado-cepea-esalq` |
| `etanol_anidro` | `sucroenergetico/indicador-semanal-etanol-anidro-cepea-esalq` |
| `frango_congelado` | `frango/precos-do-frango-congelado-cepea-esalq` |
| `frango_resfriado` | `frango/precos-do-frango-resfriado-cepea-esalq` |
| `suino` | `suinos/indicador-do-suino-vivo-cepea-esalq` |
| `leite` | `leite/leite-precos-ao-produtor-cepea-rs-litro` |
| `laranja_industria` | `laranja/laranja-industria` |
| `laranja_in_natura` | `laranja/laranja-pera-in-natura` |

Total: 20 mappings (including aliases), 18 unique URLs.

Base: `https://www.noticiasagricolas.com.br/cotacoes/{path}`

---

## Desmatamento (PRODES + DETER)

!!! info "License: Public data"

- Source: TerraBrasilis/INPE via GeoServer (WFS)
- **PRODES** = annual consolidated
- **DETER** = near-real-time alerts
- CSV responses can be large
- Rate limit: 2s

---

## Queimadas (INPE)

!!! info "License: Public data"

- Daily CSVs by satellite, biome, state
- Files can be large (hundreds of MB in dry months)
- Rate limit: 1s

---

## MapBiomas

!!! info "License: Free with citation"

- Land-cover data via Google Cloud Storage (GCS)
- Static CSVs/XLSX organized by collection/year
- The most stable source in the ecosystem
- Rate limit: 2s

---

## Cross-Source Normalization

### Crop names

Each source uses different names for the same crop.
**Without normalization, joins between sources break.**

```
CEPEA:     "soja"
CONAB:     "Soja"
IBGE:      "Soja (em grão)"
USDA:      "Soybeans"
ComexStat: "SOJA MESMO TRITURADA"
```

agrobr normalizes **144 variants → 41 canonical names**,
with case-insensitive and accent-insensitive lookup.

Full mapping in `agrobr/normalize/crops.py`.

### Crop years

| Source | Format | Example |
|-------|---------|---------|
| CONAB | crop year | `"2024/25"` |
| IBGE | calendar year | `2024` |
| USDA | marketing year | `"2024/25"` |

The BR crop year starts in July. Conversion depends on the crop and region.
Logic in `agrobr/normalize/dates.py`.

### Units

Prices and volumes appear in different units across sources.
Conversions in `agrobr/normalize/units.py` (bags, tonnes, arrobas,
bushels — 14 types).

### Municipalities

JSON with 5,571 BR municipalities + 7-digit IBGE code.
Needed to cross municipal data between sources.
File: `agrobr/normalize/_municipios_ibge.json` (259 KB).
