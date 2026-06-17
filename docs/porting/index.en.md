# Porting agrobr to Other Languages

agrobr is written in Python, but the **data and the pitfalls are universal**.
If the goal is to access Brazilian agricultural data in R, Julia, JavaScript
or any other language, this guide documents everything needed
to avoid reinventing months of reverse engineering.

!!! warning "Data Licenses"
    agrobr (the code) is MIT, but the **data** belongs to the respective sources
    and has its own licenses — some restrictive. Before implementing
    a port, read the [licenses page](../licenses.md) and check whether the
    use case complies with each source.

---

## Philosophy

agrobr is the **reference specification** for accessing Brazilian agricultural
data. The Python code is one implementation — but the knowledge
about how each source works, breaks and changes is the real value.

This guide exists so the community can build equivalent implementations
in any language, with minimal surprises.

---

## Principles for a Port

1. **Start with normalization, not infra** — cache, alerts and fingerprinting
   are optional. Normalization of crops, crop years and units is essential
   from day 1.

2. **Access CEPEA directly via headless browser** — works around Cloudflare
   without depending on restrictively licensed sources.

3. **Respect rate limits** — BR government sources block IPs. Each source
   has its own minimum interval (see [Pitfalls by Source](gotchas.md)).

4. **Normalize crop names from day 1** — without it, joins between
   CEPEA, CONAB and IBGE don't work.

5. **Test against golden data** — the files in `tests/golden_data/` serve
   as a reference to validate parsers in any language.

---

## Architecture

### Library Layers

```
┌─────────────────────────────────────────────┐
│              Public API                      │
│   cepea.indicador()  conab.safras()  ...    │
├─────────────────────────────────────────────┤
│           Semantic Layer (datasets/)         │
│   automatic fallback, contracts, MetaInfo    │
├─────────────────────────────────────────────┤
│        Individual Sources (cepea/, conab/,   │
│        ibge/, nasa_power/, bcb/, ...)        │
│   client → parser → models → public API     │
├─────────────────────────────────────────────┤
│           Infrastructure (http/, cache/,     │
│           normalize/, health/, contracts/)   │
└─────────────────────────────────────────────┘
```

**Sources** are autonomous — each has its own HTTP client, parser and
internal models. The **datasets** layer only orchestrates, normalizes and
guarantees the final contract. Never move parsing logic into datasets.

### Dataset Orchestration

The heart of agrobr is the **source-fallback** mechanism:

```
DatasetSource(name, priority, fetch_fn)
       │
       ▼
BaseDataset._try_sources(produto)
       │
       ├─ Priority 1 source → success? → returns (df, source, meta, attempted)
       ├─ Priority 2 source → success? → returns
       ├─ Priority N source → success? → returns
       └─ All failed → SourceUnavailableError(errors=[...])
```

Each `DatasetSource` encapsulates:

- `name` — source identifier
- `priority` — attempt order (lower = first)
- `fetch_fn` — async callable that returns `(DataFrame, metadata)`

The `_try_sources()` method tries sources by priority, captures errors
by category (network, parsing, contract, unexpected) and returns full
provenance.

**MetaInfo** includes:

- `attempted_sources` — list of sources tried, in order
- `selected_source` — source that provided the data
- `fetch_timestamp` — collection time
- `schema_version` — contract version

Datasets are registered automatically via a registry with auto-discovery.

### Exception Hierarchy

Any port should implement equivalents for consistent error
handling.

| Exception | When |
|---------|--------|
| `AgrobrError` | Base of all exceptions |
| `SourceUnavailableError` | All sources failed after retries |
| `NetworkError` | Timeout, HTTP error, DNS |
| `ParseError` | Layout changed, unexpected HTML/JSON |
| `ContractViolationError` | DataFrame doesn't match contract (columns, types) |
| `ValidationError` | Pydantic or statistical validation failed |
| `FingerprintMismatchError` | Page structure changed significantly |

**Warnings** (don't interrupt execution):

| Warning | When |
|---------|--------|
| `StaleDataWarning` | Expired cache data, but returned |

---

## Normalization — Modules to Port

Normalization is what enables joins between sources. **Port these modules
first**, before any HTTP client.

### Crops (`normalize/crops.py`)

**144 variants → 41 canonical names**, with case-insensitive and
accent-insensitive lookup.

```
CEPEA:     "soja"
CONAB:     "Soja"
IBGE:      "Soja (em grão)"
USDA:      "Soybeans"
ComexStat: "SOJA MESMO TRITURADA"
     ↓ normalizar_cultura()
     → "soja"
```

Functions: `normalizar_cultura()`, `listar_culturas()`, `is_cultura_valida()`

### Crop Years (`normalize/dates.py`)

Each source uses a different crop-year format:

| Source | Format | Example |
|-------|---------|---------|
| CONAB | crop year | `"2024/25"` |
| IBGE | calendar year | `2024` |
| USDA | marketing year | `"2024/25"` |

The Brazilian crop year starts in **July** (month 7). The "2024/25" crop year
runs from July 1, 2024 to June 30, 2025.

Functions: `normalizar_safra()`, `safra_atual()`, `safra_anterior()`,
`safra_posterior()`, `lista_safras()`, `periodo_safra()`,
`safra_para_anos()`, `anos_para_safra()`

Accepted formats: `2024/25`, `24/25`, `2024/2025`

### Units (`normalize/units.py`)

Sources report prices and volumes in different units.

| Unit | Weight | Use |
|---------|------|-----|
| 60kg bag | 60 kg | Soybean, corn, coffee, wheat |
| 50kg bag | 50 kg | Rice |
| Arroba | 15 kg | Live cattle |
| Soybean bushel | 27.2155 kg | USDA, CBOT |
| Corn bushel | 25.4012 kg | USDA, CBOT |
| Wheat bushel | 27.2155 kg | USDA, CBOT |

14 unit types with cross conversions. Functions: `converter()`,
`sacas_para_toneladas()`, `toneladas_para_sacas()`,
`preco_saca_para_tonelada()`, `preco_tonelada_para_saca()`

### Regions and States (`normalize/regions.py`)

- 27 states with IBGE code and region
- 5 regions (North, Northeast, Center-West, Southeast, South)
- CEPEA markets per product (soja, milho, boi_gordo, café)

Functions: `normalizar_uf()`, `uf_para_nome()`, `uf_para_regiao()`,
`uf_para_ibge()`, `ibge_para_uf()`, `normalizar_praca()`

### Municipalities (`normalize/municipalities.py`)

- 5,571 municipalities with 7-digit IBGE code + centroids
- Lookup by name (case/accent-insensitive) with disambiguation by state
- Offline reverse geocoding: `(lat, lon)` → nearest municipality (sub-ms)
- File: `normalize/_municipios_ibge.json` (259 KB)

Functions: `municipio_para_ibge()`, `ibge_para_municipio()`,
`buscar_municipios()`, `coordenada_para_municipio()`, `total_municipios()`

### Encoding (`normalize/encoding.py`)

BR government sources mix encodings without declaring them correctly.
Fallback chain of 5 encodings + automatic detection with chardet
(threshold > 0.7):

```
UTF-8 → Windows-1252 → ISO-8859-1 → UTF-16 → ASCII → chardet → replace
```

Functions: `decode_content()`, `detect_encoding()`

---

## Environment Variables

Some sources require configuration via environment variables:

| Variable | Source | Required? | Consequence without it |
|----------|-------|:------------:|----------------------|
| `AGROBR_USDA_API_KEY` | USDA PSD | Yes | `SourceUnavailableError(401)` |
| `AGROBR_INMET_TOKEN` | INMET | Yes | HTTP 204 — returns empty without error |

Rate limits and timeouts are also configurable via env vars with the
`AGROBR_HTTP_` prefix (e.g. `AGROBR_HTTP_RATE_LIMIT_CEPEA=5.0`).

---

## Golden Data

The files in `tests/golden_data/` contain static reference data
to validate parsers in any language:

1. Feed your parser the golden input (HTML, JSON, CSV, XLSX)
2. Compare the output with `expected.json`
3. If it matches, your parser is correct

### Available test sets (sample: 26 sources, 35 cases)

| Source | Test case | Files |
|-------|--------------|----------|
| ABIOVE | `exportacao_sample` | response.xlsx, expected.json |
| ANDA | `entregas_sample` | response.json, expected.json |
| B3 | `posicoes_sample` | response.csv, expected.json |
| BCB | `custeio_sample` | response.json, expected.json |
| CEPEA | `soja_sample` | response.html, expected.json |
| Comtrade | `comercio_sample` | response.json, expected.json |
| Comtrade | `mirror_sample` | response_reporter.json, response_partner.json, expected.json |
| ComexStat | `exportacao_soja_sample` | response.csv, expected.json |
| CONAB | `safra_sample` | response.xlsx, expected.json |
| CONAB CEASA | `precos_sample` | ceasas_response.json, precos_response.json, expected.json |
| CONAB Progresso | `progresso_sample` | progresso_sample.xlsx, expected.json |
| DERAL | `pc_sample` | response.xlsx, expected.json |
| Desmatamento | `deter_sample` | response.csv, expected.json |
| Desmatamento | `prodes_sample` | response.csv, expected.json |
| IBGE | `abate_bovino_sample` | response.csv, expected.json |
| IBGE | `censo_agro_efetivo_sample` | response.csv, expected.json |
| IBGE | `pam_soja_sample` | response.csv, expected.json |
| IBGE | `ppm_bovino_sample` | response.csv, expected.json |
| IBGE | `silvicultura_sample` | response.csv, expected.json |
| IBGE | `extracao_vegetal_sample` | response.csv, expected.json |
| IBGE | `leite_trimestral_sample` | response.csv, expected.json |
| IBGE | `pib_agro_sample` | response.csv, expected.json |
| IMEA | `cotacoes_soja_sample` | response.json, expected.json |
| INMET | `observacoes_sample` | response.json, expected.json |
| MapBiomas | `biome_state_sample` | biome_state_sample.xlsx, expected.json |
| Notícias Agrícolas | `soja_sample` | response.html, expected.json |
| NASA POWER | `daily_sample` | response.json, expected.json |
| Queimadas | `focos_sample` | response.csv, expected.json |
| USDA | `psd_soja_sample` | response.json, expected.json |
| RNC | `registradas_sample` | registradas_sample.csv (25 rows), expected.json |
| Rio Verde | `ensaio_soja_pages` | ensaio_soja_pages.json (5 pages), expected.json |
| BCB SGS | `sgs_sample` | sgs_sample.json (10 rows), expected.json |
| BCB PTAX | `ptax_sample` | ptax_sample.json (5 rows), expected.json |
| BCB Focus | `focus_sample` | focus_sample.json (5 rows), expected.json |
| ZARC | `tabua_risco_sample` | response.csv, expected.json |

The table above is a sample; the `tests/golden_data/` directory contains 41 sources and 60 cases in total. Each directory also contains `metadata.json` with the test context.

---

## Sources by Implementation Priority

| Priority | Source | License | Access | Rationale |
|:---:|--------|---------|--------|---------------|
| 1 | CEPEA | CC BY-NC | Headless browser | Daily prices, high demand |
| 2 | IBGE/SIDRA | Free | REST API | Clean API, official public data |
| 3 | CONAB Historical Series | Free | Direct HTTP | Crops since 1976, no browser |
| 4 | CONAB CEASA | Free | Direct HTTP | 48 produce items, 43 CEASAs, no browser |
| 5 | CONAB Progresso | Free | Direct HTTP | Weekly planting/harvest, no browser |
| 6 | CONAB Bulletin | Free | Headless browser | Current crop, requires JS |
| 7 | NASA POWER | CC BY 4.0 | REST API | Climate, clean API |
| 8 | BCB/SICOR | Free | OData API | Rural credit |
| 9 | ComexStat | Free | Direct HTTP | Exports, bulk CSV |
| 10 | CONAB Production Cost | Free | Direct HTTP | Costs per crop/state |
| 11+ | DERAL, USDA, Queimadas, Desmatamento, MapBiomas | Free | Varies | As needed |

!!! warning "Restricted sources"
    IMEA and Notícias Agrícolas have a **restricted** license (redistribution
    prohibited). B3, ANDA and ABIOVE are in a **gray area** (no clear terms
    for programmatic access). Check the [licenses page](../licenses.md)
    before implementing access to these sources.

---

## Guides by Language

| Language | Guide |
|-----------|------|
| R | [R Developer Guide](r.md) |

---

## Contributing Ports

If you implement a port in another language:

- Open an issue in the [agrobr repository](https://github.com/bruno-portfolio/agrobr) with the link
- Consider using the same canonical crop names (see `agrobr/normalize/crops.py`)
- Use the golden tests as a validation suite
- The [pitfalls-by-source](gotchas.md) documentation applies to any language

---

## Known Implementations

| Language | Repo | Status |
|-----------|------|--------|
| Python | [agrobr](https://github.com/bruno-portfolio/agrobr) | Reference |
