# ZARC (Agroclimatic Risk Zoning)

Recommended planting windows by municipality, crop, soil type and cultivar cycle.

## zoneamento

Query the ZARC Risk Table (Tabua de Risco).

```python
import agrobr

df = await agrobr.zarc.zoneamento(cultura="soja", uf="MT", safra="2025/2026")
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|-------------|-----------|
| cultura | str | No | Canonical crop name (e.g. "soja", "milho_1", "trigo") |
| uf | str | No | State abbreviation (e.g. "MT", "SP") |
| municipio | int \| str | No | 7-digit IBGE code (int) or partial name (str), case- and accent-insensitive |
| safra | str | No | "2025/2026" or "perene" (default: most recent crop year) |
| solo | int | No | Soil type code (1-3 legacy, 11-16 new 6-AD) |
| ciclo | int | No | Cultivar cycle code (13, 19, 20, 21, 22, 24, 25, 26) |
| as_polars | bool | No | If True, return a polars DataFrame |
| return_meta | bool | No | If True, return (DataFrame, MetaInfo) |
| use_cache | bool | No | Default True; False bypasses the catalog and table caches |

The first query for each revision downloads and parses the complete table. Later queries read validated data from the local DuckDB cache, including in another Python process: a 24-hour TTL from acquisition and up to three revisions. The ZARC file is separate from the CEPEA cache. The catalog uses a one-hour in-memory cache. `use_cache=False` bypasses reads and writes for both; local cache failures log a warning and fall back to download and parsing. Metadata retains the SHA, original acquisition time and crops observed across the complete table. The download is checked against the size the server publishes (the MAPA portal's `Content-Range`, or `Content-Length`): a shorter body raises `SourceUnavailableError` and does not reach the cache. Without a published size, the result warns in `validation_warnings` ("tamanho do arquivo não conferido") and is not stored; a cache entry without a checked size or without records is downloaded again.

### Returned columns

| Column | Type | Description |
|--------|------|-----------|
| cultura | str | Canonical name (e.g. "soja", "milho_1") |
| safra | str | "2025/2026" or "perene" |
| geocodigo | str | Municipality IBGE code (7 digits) |
| uf | str | State abbreviation |
| municipio | str | Municipality name |
| solo_codigo | int | Soil type |
| ciclo_codigo | int | Cultivar cycle |
| clima | str | Climatic restriction |
| manejo | str | Specific management |
| portaria | str | MAPA ordinance number |
| dec1-dec36 | int | Risk per 10-day period (0/20/30/40/50) |

### Examples

```python
# Soybean in Mato Grosso
df = await agrobr.zarc.zoneamento(cultura="soja", uf="MT")

# Specific municipality by geocode
df = await agrobr.zarc.zoneamento(municipio=5107925, safra="2025/2026")

# Search by partial municipality name
df = await agrobr.zarc.zoneamento(municipio="Sorriso", cultura="soja")

# Filter by soil and cycle
df = await agrobr.zarc.zoneamento(cultura="milho_1", solo=2, ciclo=20)

# Perennial crops
df = await agrobr.zarc.zoneamento(cultura="cafe_arabica", safra="perene")

# With metadata
df, meta = await agrobr.zarc.zoneamento(cultura="soja", uf="MT", return_meta=True)
print(meta.records_count, meta.fetch_duration_ms)
```

## culturas

Crops outside the catalog are rejected before network access, with suggestions when similar names are available. Valid catalog crops missing from the selected season are rejected after reading the table, pointing to the perennial table or to the seasons in which the crop appears (tables published up to 2026-09-23). For the 11 crop labels renamed in the 2024/2025 season, the message also points to the equivalent key in the queried table (table on the [source page](../sources/zarc.md)).

List of 107 crops represented by canonical aliases, one for each label published in the 12 official tables checked on 2026-09-23, including two legacy names from 2016/2017 and the nine labels of the 2017/2018 to 2023/2024 seasons (`milho`, `arroz_irrigado`, `feijao_1`, `trigo_irrigado`, `mamona_semiarido_sequeiro`, `cevada_graos_irrigada`, `cevada_graos_sequeiro`, `aveia_sequeiro`, `aveia_irrigada`). Fruit and coffee crops use `safra="perene"`. Availability varies by season; a crop missing from the requested season reports the seasons in which it appears. With `return_meta=True`, `meta.source_details["parser"]["culturas_observadas"]` lists crops observed in the complete table before filtering.

```python
culturas = agrobr.zarc.culturas()
# ['abacaxi', 'acai', 'acai_implantacao', 'algodao', 'alho_nobre', ...]
```

Synchronous function (no await).

## safras_disponiveis

Crop years available in the CKAN portal (performs online discovery).

```python
safras = await agrobr.zarc.safras_disponiveis()
# ['2016/2017', '2017/2018', ..., '2025/2026', 'perene']
```

## Synchronous usage

```python
from agrobr import sync

df = sync.zarc.zoneamento(cultura="soja", uf="MT")
culturas = sync.zarc.culturas()
safras = sync.zarc.safras_disponiveis()
```

## Data source

- **Provider:** MAPA / Embrapa
- **Portal:** [dados.agricultura.gov.br](https://dados.agricultura.gov.br/dataset/tabua-de-risco-zoneamento-agricola-de-risco-climatico)
- **License:** CC-BY (federal government public data)
- **Update:** weekly in the CKAN catalogue; PDF declares daily, without establishing actual cadence

## Reconciliation and legacy crops

The filter catalogue includes `Arroz Sequeiro`/`arroz_sequeiro` and `Trigo Sequeiro`/`trigo_sequeiro`, published in the 2016/2017 table. These aliases retain values already returned by the parser and are not converted to `arroz`/`trigo`. Unknown names are still rejected before network access; crop availability depends on the selected table.

The 59 columns in contract 2.1 retain all 55 published fields, plus normalized crop, derived season, CSV position and `cod_municipio` (the `geocodigo` as an integer). Repeated records are preserved. `registro_origem` is meaningful only together with `meta.raw_content_hash`: the three bodies captured on 2026-09-18 had different hashes from September 7, but a complete multiset comparison confirmed identical records in a different order. A changed hash does not establish that values changed.

UTC acquisition time and body hash remain in `meta.fetched_at`, `meta.raw_content_hash` and `meta.source_details["resource"]`, including cache hits. Reading to EOF confirms that the received body was processed, without certifying an external municipality total or transactional snapshot. The CKAN catalogue queried by the API declares weekly updates, while the PDF dictionary declares daily updates. The dataset uses `update_frequency="weekly"`, taking the active discovery catalogue as its operational reference; the conflicting PDF declaration remains recorded. Neither statement establishes the actual revision cadence of each season. Productivity and NM codes retain the literal text, without inferring a unit missing from the dictionary.
