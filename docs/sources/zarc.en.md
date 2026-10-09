# ZARC (Agricultural Climate Risk Zoning — Zoneamento Agricola de Risco Climatico)

## About

**ZARC** is the official system of MAPA (Ministry of Agriculture) and Embrapa
that defines recommended planting windows by municipality, crop, soil type
and cultivar cycle. Published as an Ordinance (Portaria) in the Official Gazette of the Union,
ZARC is a requirement for access to subsidized rural credit (Proagro, PSR).

Data published as CSV on the [dados.agricultura.gov.br](https://dados.agricultura.gov.br) portal
(CKAN), CC-BY license, weekly cadence in the active catalogue (the PDF declares daily updates; see the conflict below).

## Available data

- **Risk Table:** planting windows (36 ten-day periods) by municipality/crop/soil/cycle
- **Crops:** 107 crops in the alias catalogue, one per label published in the 12 official tables, including legacy names; availability varies by season; fruit and coffee crops use `safra="perene"`
- **Crop years:** 2016/2017 to current + perennial (coffee, sugarcane, banana, etc.)
- **Soils:** 3 classic types (sandy/medium/clayey) + 6 AW levels (available water)
- **Coverage:** municipalities present in each publication; complete national coverage is not asserted

## Returned fields

| Field | Type | Description |
|-------|------|-----------|
| cultura | string | Canonical crop name (e.g. "soja", "milho_1", "trigo") |
| safra | string | Crop year (`"2025/2026"`); in the `safra="perene"` table, `"perene"`, `"olericola"` or `"sem_safra"` |
| geocodigo | string | IBGE code of the municipality (7 digits) |
| uf | string | State abbreviation |
| municipio | string | Municipality name |
| solo_codigo | int | Soil type (1-3 classic, 11-16 AW) |
| ciclo_codigo | int | Cultivar cycle (13, 19, 20, 21, 22, 24, 25, 26) |
| clima | string | Climate restriction (e.g. "Sem restricao") |
| manejo | string | Specific management (e.g. "Sem restricao", "Irrigado") |
| portaria | string | MAPA ordinance number |
| dec1-dec36 | Int64, nullable | Published risk per ten-day period (0/20/30/40/50); empty cells are null |

## Ten-day periods (decendios)

Each month is divided into 3 ten-day periods:

- dec1-dec3: January
- dec4-dec6: February
- ...
- dec34-dec36: December

Published values: 0, 20, 30, 40 and 50. Empty cells are null, distinct from zero. The value 50 occurs in the perennial table; 0 and 50 are preserved without assuming an agronomic interpretation.

## Notes

- **Large CSV:** approximately 224 MB per annual crop year and 535 MB for the perennial table. The first query for each revision downloads and parses the complete table (about 3 minutes, almost all of it validating each record). Later queries read validated data from the local DuckDB cache, including in another Python process: a 24-hour TTL from acquisition and up to three revisions. The ZARC file is separate from the CEPEA cache. The catalog uses a one-hour in-memory cache. `use_cache=False` bypasses reads and writes for both; local cache failures log a warning and fall back to download and parsing. Metadata retains the SHA, original acquisition time and crops observed across the complete table. The download is checked against the size the server publishes (the MAPA portal's `Content-Range`, or `Content-Length`): a shorter body raises `SourceUnavailableError` and does not reach the cache. Without a published size, the result warns in `validation_warnings` ("tamanho do arquivo não conferido") and is not stored; a cache entry without a checked size or without records is downloaded again. Where it lives and how to clean it: [What agrobr writes to disk](../advanced/disco.md).
- **CKAN discovery:** URLs change with each publication; the client performs discovery via the CKAN API
- **User-Agent:** the portal requires browser-like headers (returns 403 with a bot UA)
- **Encoding:** UTF-8 with BOM, separator `;`
- **Yield:** published as text in `produtividade_texto` (almost always empty; decimal commas preserved; unit not inferred)

## License

Brazilian federal government public data (CC-BY). Free use with citation of the source.

## Links

- [CKAN portal](https://dados.agricultura.gov.br/dataset/tabua-de-risco-zoneamento-agricola-de-risco-climatico)
- [ZARC - MAPA](https://www.gov.br/agricultura/pt-br/assuntos/riscos-seguro/programa-nacional-de-zoneamento-agricola-de-risco-climatico)

## Crops by season

Both source names and aliases are accepted: `Milho 2ª Safra` maps to `milho_2`, `Algodão Herbáceo` to `algodao`, and `Sorgo Granífero 2ª Safra` to `sorgo_2`. `culturas()` lists the general catalog, not guaranteed availability in every season. Missing crops report aliases available in the queried table; use `safras_disponiveis()` to choose another season.

Crops outside the catalog are rejected before network access, with suggestions when similar names are available. Valid catalog crops missing from the selected season are rejected after reading the table, with guidance to the appropriate table when known.

## Renamings in the 2024/2025 season

Starting with the 2024/2025 table, ZARC renames 11 crop labels. The content is the same: in the official 2023/2024 and 2024/2025 tables, each pair has the same municipality, soil, cycle and ten-day records. agrobr does not convert one key into the other. Requesting the old key in a new season, or the reverse, raises `InvalidParameterError` with the equivalent key in that table.

| Up to 2023/2024 | `cultura_codigo` | From 2024/2025 | Filter in the new table | `cultura_codigo` |
|---|---|---|---|---|
| `milho` (Milho) | 12015080000011 | `milho_1` (Milho 1ª Safra) | — | 12015080000011 |
| `feijao_1` (Feijão 1ª Safra) | 12013560000011 | `feijao` (Feijão) | — | 12013560000011 |
| `arroz_sequeiro` (Arroz Sequeiro) | 12010900000011 | `arroz` (Arroz) | `manejo == "Sequeiro"` | 12010900000011 |
| `arroz_irrigado` (Arroz Irrigado) | 12010900000051 | `arroz` (Arroz) | `manejo == "Irrigado"` | 12010900000011 |
| `aveia_sequeiro` (Aveia Sequeiro) | 12011000000031 | `aveia` (Aveia) | `manejo == "Sequeiro"` | 12011000000031 |
| `aveia_irrigada` (Aveia Irrigada) | 12011000000051 | `aveia` (Aveia) | `manejo == "Irrigado"` | 12011000000031 |
| `cevada_graos_sequeiro` (Cevada Grãos Sequeiro) | 12012320000031 | `cevada_graos` (Cevada Grãos) | `manejo == "Sequeiro"` | 12012320000031 |
| `cevada_graos_irrigada` (Cevada Grãos Irrigada) | 12012320000051 | `cevada_graos` (Cevada Grãos) | `manejo == "Irrigado"` | 12012320000031 |
| `trigo_sequeiro` (Trigo Sequeiro) | 12017100000031 | `trigo` (Trigo) | `manejo == "Sequeiro"` | 12017100000031 |
| `trigo_irrigado` (Trigo Irrigado) | 12017100000051 | `trigo` (Trigo) | `manejo == "Irrigado"` | 12017100000031 |
| `mamona_semiarido_sequeiro` (Mamona Semi-árido Sequeiro) | 12014720000011 | `mamona` (Mamona) | `cultura_codigo == "12014720000011"` | 12014720000011 |

To join seasons, use `cultura_codigo` where it is kept (maize, beans, semi-arid castor bean and the rainfed versions) and `manejo` for the irrigated versions, whose code changes. In 2024/2025, `feijao` is first-season beans, not the total, and `mamona` now includes the semi-arid castor bean: 51,784 records in 2023/2024 and 62,441 in 2024/2025, of which 10,657 are semi-arid.

## Legacy crops and record identity

The filter catalogue includes `Arroz Sequeiro`/`arroz_sequeiro` and `Trigo Sequeiro`/`trigo_sequeiro`, published in the 2016/2017 table. These aliases retain values already returned by the parser and are not converted to `arroz`/`trigo`. Unknown names are still rejected before network access; crop availability depends on the selected table.

The 59 columns in contract 2.1 retain all 55 published fields, plus normalized crop, derived season, CSV position and `cod_municipio` (the `geocodigo` as an integer). Repeated records are preserved. `registro_origem` is meaningful only together with `meta.raw_content_hash`: the three bodies of 2026-09-18 had different hashes from those of September 7, with the same records in a different order. A changed hash does not establish that values changed.

UTC acquisition time and body hash remain in `meta.fetched_at`, `meta.raw_content_hash` and `meta.source_details["resource"]`, including cache hits. Reading to EOF confirms that the received body was processed, without certifying an external municipality total or transactional snapshot. The CKAN catalogue queried by the API declares weekly updates, while the PDF dictionary declares daily updates. The dataset uses `update_frequency="weekly"`, taking the active discovery catalogue as its operational reference; the conflicting PDF declaration remains recorded. Neither statement establishes the actual revision cadence of each season. Productivity and NM codes retain the literal text, without inferring a unit missing from the dictionary.
