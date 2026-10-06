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
       ├─ All failed due to layout → ParseError(errors=[...])
       └─ Other failures exhausted the sources → SourceUnavailableError(errors=[...])
```

Each `DatasetSource` encapsulates:

- `name` — source identifier
- `priority` — attempt order (lower = first)
- `fetch_fn` — async callable that returns `(DataFrame, metadata)`

The `_try_sources()` method tries only enabled sources, by priority, and returns
the first result with full provenance. An empty result is also accepted; when a
contract is registered, it receives that contract's columns and types.

Network failures (`httpx.HTTPError`, `httpx.TimeoutException`, and `OSError`),
`ParseError`, `ContractViolationError` raised by the fetcher, and
`SourceUnavailableError` are recorded and allow the next source to be tried. If
every attempt fails due to layout, the cascade raises an aggregated `ParseError`.
Other exhausted cascades, including mixed or contract failures, raise
`SourceUnavailableError`. Both errors include `errors`, `attempted_sources`,
and the last cause in `__cause__`.

`InvalidParameterError`, `TypeError`, `CacheMigrationError`, and `ResourceLimitError`
stop the cascade. `SourceFallbackWarning` also propagates when configured as an
error. Other programming errors are not caught and do not trigger fallback.

**MetaInfo** includes:

- `attempted_sources` — identifiers tried in order, following the convention below
- `selected_source` — selected identifier, following the convention below
- `fetch_timestamp` — UTC time of the acquisition of the body the top level describes (equal to `fetched_at`; null for records from CEPEA's DuckDB cache)
- `fetched_at` — original source acquisition time, preserved when reading cache
- `schema_version` — contract version

`fetched_at`, `timestamp`, `cache_expires_at`, and `fetch_timestamp` are normalized to timezone-aware UTC during construction and subsequent assignments. Naive values are interpreted as UTC; offsets are converted while preserving the instant. Optional fields still accept `None`. The constructor and assignments accept `datetime` objects; raw strings raise `AttributeError`. `from_dict()` accepts ISO timestamps with or without an offset; `to_dict()` emits `+00:00` for populated values. Use `datetime.now(UTC)` for time comparisons.

Published identifiers follow two conventions:

- **Source route:** `comercio_internacional`, `desmatamento`, `empregadores_lista_suja`, `unidades_conservacao`, `unidades_conservacao_federais`, `uso_do_solo`, `cultivares_registradas`, and `cultivares_protegidas` preserve the route and attempts reported by the source, even for a single attempt. The two cultivar functions share this rule in `_rnc.py`. Without that provenance, the adapter name provides the identifier.
- **Dataset adapter:** other datasets use `DatasetSource.name` for `selected_source` and list attempted adapters in `attempted_sources`. For example, `cadastro_rural` publishes `selected_source="sicar"` and `["sicar"]` for a single attempt, whereas the source API publishes `sicar_wfs`.

The base rule adopts internal provenance when the source reports more than one attempt or `selected_source="cache"`: it preserves the selected route and combines previous adapters with internal attempts, removing duplicates while retaining order. Without a selected internal identifier, it retains the adapter name. `from_cache` is propagated independently; `from_cache=True` alone does not change the naming convention.

Datasets are registered automatically via a registry with auto-discovery.

The base also propagates `raw_content_hash`, `raw_content_size`, `cache_key`, `cache_expires_at`, `fetch_duration_ms`, and `parse_duration_ms` from the selected source. These fields describe source content, cache, and work; they are not a normalized DataFrame hash, a dataset-owned cache, or total wrapper duration. Without source metadata, they retain None/0. A key or TTL does not determine `from_cache`, and `source_details` remains an independent copy.

### Exception Hierarchy

Any port should implement equivalents for consistent error
handling.

| Exception | When |
|---------|--------|
| `AgrobrError` | Base of all exceptions |
| `InvalidParameterError` | Invalid user parameter; also a `ValueError` and stops the cascade |
| `SourceUnavailableError` | The source did not deliver the data: timeout, connection failure or HTTP error status (after the retries, for the statuses that are retried), with the source, the URL and the status, and the httpx exception as `__cause__`. In a dataset, the sources were exhausted without all failures being layout errors: `attempted_sources` and `errors` say which and why |
| `NetworkError` | Reserved: still exported, but not raised in 2.0; HTTP error statuses come out as `SourceUnavailableError` |
| `ParseError` | Layout changed, unexpected HTML/JSON; in a dataset, aggregates `errors` and `attempted_sources` when all sources fail due to layout |
| `ContractViolationError` | DataFrame doesn't match contract (columns, types) |
| `ValidationError` | Pydantic or statistical validation failed |
| `FingerprintMismatchError` | Page structure changed significantly |

**Warnings** (don't interrupt execution):

| Warning | When |
|---------|--------|
| `SourceFallbackWarning` | Primary source failed and the dataset returned a fallback |
| `StaleDataWarning` | Expired cache data, but returned |

---

## Normalization — Modules to Port

Normalization is what enables joins between sources. **Port these modules
first**, before any HTTP client.

### Crops (`normalize/crops.py`)

**158 variants → 43 canonical names**, with case-insensitive and
accent-insensitive lookup.

```
CEPEA:     "soja"
CONAB:     "Soja"
USDA:      "Soybeans"
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
| USDA | marketing year (first year) | `2024` (2024/25 season) |

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
Fallback chain of 3 encodings. ISO-8859-1 decodes any byte, so the chain ends there:

```
UTF-8 → Windows-1252 → ISO-8859-1
```

Functions: `decode_content()`, `detect_encoding()`

---

## Environment Variables

Some sources require configuration via environment variables:

| Variable | Source | Required? | Consequence without it |
|----------|-------|:------------:|----------------------|
| `AGROBR_USDA_API_KEY` | USDA PSD | Yes | `SourceUnavailableError` before the network |
| `AGROBR_INMET_TOKEN` | INMET | Yes, for the observational API | `SourceUnavailableError`, with instructions to set the token. The station catalog and historical ZIP archives are public |
| `AGROBR_MAPBIOMAS_ALERTA_TOKEN` | MapBiomas Alerta | Yes | `SourceUnavailableError` before the network |

In `clima_uf`, a missing token is rejected before listing stations. When accessing
`estacao`, HTTP 204 without a token and HTTP 403 are converted into
`SourceUnavailableError` with instructions to set `AGROBR_INMET_TOKEN`.

Rate limits and timeouts are also configurable via env vars with the
`AGROBR_HTTP_` prefix (e.g. `AGROBR_HTTP_RATE_LIMIT_CEPEA=5.0`).

---

## Golden Data

The files in `tests/golden_data/` contain static reference data
to validate parsers in any language:

1. Feed your parser the golden input (HTML, JSON, CSV, XLSX, PDF)
2. Compare the output with `expected.json`, the case observations in the manifest (ANDA) or the case oracle
   (Rio Verde: the rows in `oraculo_20260923.json`; IMEA: the official JSON itself, as in `tests/test_imea/oficial.py`;
   Desmatamento: the properties of each feature in the official JSON, as in `tests/test_desmatamento/test_json_parser.py`)
3. If it matches, your parser is correct

### Available test sets (sample: 26 sources, 33 cases)

| Source | Test case | Files |
|-------|--------------|----------|
| ABIOVE | `exportacao_sample` | response.xlsx, expected.json |
| ANDA | `reconciliacao_boletins_anec_anda_deral_20260918` | anda/anda_Principais_Indicadores_2024.pdf, anda_manifest.json (`anda_2024`) |
| B3 | `posicoes_sample` | response.csv, expected.json |
| BCB | `custeio_sample` | response.json, expected.json |
| CEPEA | `soja_sample` | response.html, expected.json |
| Comtrade | `comercio_sample` | response.json, expected.json |
| Comtrade | `mirror_sample` | response_reporter.json, response_partner.json, expected.json |
| ComexStat | `exportacao_soja_sample` | response.csv, expected.json |
| CONAB | `safra_2025_26_agosto` | response.xlsx, expected.json |
| CONAB CEASA | `precos_sample` | ceasas_response.json, precos_response.json, expected.json |
| CONAB Progresso | `progresso_sample` | response.xlsx, expected.json |
| DERAL | `pc_sample` | response.xlsx, expected.json |
| Desmatamento | `selecao_20260907` | 8 official JSON bodies (PRODES in 6 biomes, DETER Amazônia and Cerrado), manifest.json |
| Desmatamento | `geo_20260923` | hits (XML) and GeoJSON page of 3 selections (PRODES Pantanal, DETER Amazônia and Cerrado), manifest.json |
| IBGE | `abate_bovino_sample` | response.csv, expected.json |
| IBGE | `censo_agro_efetivo_sample` | response.csv, expected.json |
| IBGE | `pam_soja_sample` | response.csv, expected.json |
| IBGE | `reconciliacao_canais_ibge_20260918` (PPM, PEVS) | agregados/*.json, manifest.json |
| IBGE | `leite_trimestral_sample` | response.csv, expected.json |
| IBGE | `pib_agro_sample` | response.csv, expected.json |
| IMEA | `oficial_20260923` | cadeias.json, cotacoes_{id}.json and indicadores_{id}.json (8 chains), manifest.json |
| INMET | `observacoes_sample` | response.json, expected.json |
| MapBiomas | `biome_state_sample` | response.xlsx, expected.json |
| Notícias Agrícolas | `soja_sample` | response.html, expected.json |
| NASA POWER | `daily_sample` | response.json, expected.json |
| Queimadas | `focos_sample` | response.csv, expected.json |
| USDA | `psd_gateway_20260926` | 34 bodies from two independent captures, gateway captures (404, 403, `[]`, old series), 72- and 48-value oracles, manifest.json |
| RNC | `registradas_sample` | registradas_sample.csv (25 rows) |
| Rio Verde | `oraculo_20260923` | ensaio_soja_2023_2024.pdf, ensaio_soja_2024_2025.pdf, ensaio_soja_2025_2026.pdf, oraculo_20260923.json |
| BCB SGS | `sgs_sample` | sgs_sample.json (10 rows) |
| BCB PTAX | `ptax_sample` | ptax_sample.json (5 rows) |
| BCB Focus | `focus_sample` | focus_sample.json (5 rows) |
| ZARC | `edicoes_20260923` | 9 CSVs (2017/2018 to 2025/2026 seasons), catalogo.json, manifest.json, oracle.json |

The table above is a sample; the `tests/golden_data/` directory has 45 source directories plus the reconciliation cases (`reconciliacao_*`); the older goldens include `metadata.json` with the test context.

---

## Sources by Implementation Priority

| Priority | Source | License | Access | Rationale |
|:---:|--------|---------|--------|---------------|
| 1 | CEPEA | CC BY-NC | Headless browser | Daily prices, high demand |
| 2 | IBGE/SIDRA | Free | REST API | Clean API, official public data |
| 3 | CONAB Historical Series | Free | Direct HTTP | Crops since 1976, no browser |
| 4 | CONAB CEASA | Gray area | Direct HTTP | 48 produce items, 43 CEASAs, no browser |
| 5 | CONAB Progresso | Free | Direct HTTP | Weekly planting/harvest, no browser |
| 6 | CONAB Bulletin | Free | Direct HTTP (browser only as fallback) | Current crop |
| 7 | NASA POWER | CC BY 4.0 | REST API | Climate, clean API |
| 8 | BCB/SICOR | Free | OData API | Rural credit |
| 9 | ComexStat | Free | Direct HTTP | Exports, bulk CSV |
| 10 | CONAB Production Cost | Free | Direct HTTP | Costs per crop/state |
| 11+ | DERAL, USDA, Queimadas, Desmatamento, MapBiomas | Free | Varies | As needed |

!!! warning "Source licenses"
    IMEA (public series), Notícias Agrícolas, B3, ANDA, ABIOVE and CONAB CEASA are classified as `zona_cinza`. CEPEA-origin data retains `nc`. Comtrade is `restrito`, with explicit exceptions in the UN policy. Check the scope and conditions on the [licenses page](../licenses.md).

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
