# MAPA PSR — Rural Insurance

> **License:** CC-BY (Brazilian federal government public data).
> Classification: `livre`

Open data from SISSER/MAPA — the Rural Insurance Premium Subsidy System
(Sistema de Subvencao Economica ao Premio do Seguro Rural). Policies and claims
(indemnities) of Brazilian rural insurance with federal subsidy, published by
the Ministry of Agriculture.

Production revision proxy: high soybean claims in Q1 precede
cuts in the CONAB Q2 estimate.

## Installation

Does not require optional dependencies. Uses only httpx + pandas (core).

## API

```python
from agrobr.alt import mapa_psr

# Rural insurance claims (indemnities paid)
df = await mapa_psr.sinistros()

# Filter by crop and state
df = await mapa_psr.sinistros(cultura="SOJA", uf="MT")

# Filter by year or range
df = await mapa_psr.sinistros(ano=2023)
df = await mapa_psr.sinistros(ano_inicio=2020, ano_fim=2024)

# Filter by predominant event
df = await mapa_psr.sinistros(evento="seca")

# Filter by municipality
df = await mapa_psr.sinistros(municipio="SORRISO")

# All policies (including those without a claim)
df = await mapa_psr.apolices()

# Filtered policies
df = await mapa_psr.apolices(cultura="MILHO", uf="PR", ano=2023)

# Synchronous API
from agrobr.sync import alt
df = alt.mapa_psr.sinistros(cultura="SOJA")
df = alt.mapa_psr.apolices(uf="MT")
```

## Parameters — `sinistros`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `cultura` | str \| None | None | Filter by crop (partial match, accent-insensitive, e.g. "cafe" matches "CAFE ARABICA") |
| `uf` | str \| None | None | Filter by state (abbreviation, e.g. "MT") |
| `ano` | int \| None | None | Single-year filter (e.g. 2023) |
| `ano_inicio` | int \| None | None | Start year of the range (inclusive) |
| `ano_fim` | int \| None | None | End year of the range (inclusive) |
| `municipio` | str \| None | None | Filter by municipality (partial match) |
| `evento` | str \| None | None | Filter by predominant event (e.g. "seca") |
| `return_meta` | bool | False | Returns a (DataFrame, MetaInfo) tuple |

## Columns — `sinistros`

| Column | Type | Nullable | Description |
|---|---|---|---|
| `nr_apolice` | str | No | Policy number |
| `ano_apolice` | int | No | Policy year |
| `uf` | str | No | State abbreviation of the property |
| `municipio` | str | Yes | Municipality name |
| `cd_ibge` | str | Yes | IBGE code of the municipality |
| `cultura` | str | No | Insured crop (uppercase) |
| `classificacao` | str | Yes | Product classification (AGRICOLA, PECUARIO, etc.) |
| `evento` | str | No | Predominant event (lowercase) |
| `area_total` | float | Yes | Total insured area (ha) |
| `valor_indenizacao` | float | No | Indemnity amount (R$) — always > 0 |
| `valor_premio` | float | Yes | Net premium (R$) |
| `valor_subvencao` | float | Yes | Federal subsidy (R$) |
| `valor_limite_garantia` | float | Yes | Coverage limit (R$) |
| `produtividade_estimada` | float | Yes | Estimated yield |
| `produtividade_segurada` | float | Yes | Insured yield |
| `nivel_cobertura` | float | Yes | Coverage level (%) |
| `seguradora` | str | Yes | Insurer legal name |

## Parameters — `apolices`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `cultura` | str \| None | None | Filter by crop (partial match, accent-insensitive) |
| `uf` | str \| None | None | Filter by state |
| `ano` | int \| None | None | Single-year filter |
| `ano_inicio` | int \| None | None | Start year of the range |
| `ano_fim` | int \| None | None | End year of the range |
| `municipio` | str \| None | None | Filter by municipality |
| `return_meta` | bool | False | Returns a (DataFrame, MetaInfo) tuple |

## Columns — `apolices`

Same columns as `sinistros`, plus:

| Column | Type | Nullable | Description |
|---|---|---|---|
| `taxa` | float | Yes | Premium rate (%) |

## Data pipeline

1. Resolves the required periods (2006-2015, 2016-2024, 2025) based on the year filters
2. Bulk CSV download from the dados.agricultura.gov.br portal (3 files, variable encoding)
3. Detects encoding (UTF-8 → UTF-8-sig → Windows-1252 → ISO-8859-1 → chardet) and separator (`;` or `,`)
4. Removes PII columns (NM_SEGURADO, NR_DOCUMENTO_SEGURADO) and geolocation
5. Normalizes column names (standardized snake_case)
6. Converts types (float64 for monetary values, int for year)
7. For claims: filters VALOR_INDENIZACAO > 0 and non-empty EVENTO_PREPONDERANTE

## MetaInfo

```python
df, meta = await mapa_psr.sinistros(return_meta=True)
print(meta.source)           # "mapa_psr"
print(meta.source_method)    # "httpx"
print(meta.parser_version)   # 1
print(meta.records_count)    # varies by filter
```

## Performance note

The SISSER CSVs can be large (the 2006-2015 file has ~500k rows).
The module downloads only the periods required based on the year filters.
Read timeout: 180 seconds.

## Datasets

- [`seguro_rural`](../contracts/seguro_rural.md) — wraps `mapa_psr.apolices()` and `mapa_psr.sinistros()` via `tipo=` dispatch

## Source

- URL: `https://dados.agricultura.gov.br/dataset/sisser3`
- Format: CSV (3 files per period)
- Update frequency: annual
- History: 2006+
- License: `livre` (CC-BY, federal government public data)
