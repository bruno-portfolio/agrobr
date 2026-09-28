# MAPA PSR — Rural Insurance

> **License:** CC-BY (Brazilian federal government public data).
> Classification: `livre`

Open data from SISSER/MAPA — the Rural Insurance Premium Subsidy System
(Sistema de Subvencao Economica ao Premio do Seguro Rural). Policies and claims
(indemnities) of Brazilian rural insurance with federal subsidy, published by
the Ministry of Agriculture.

Indemnities are associated with the year the policy was contracted. This output
does not report the quarter of the event or payment.

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

# Filter by the municipality label (see "Municipality and IBGE code")
df = await mapa_psr.sinistros(municipio="SORRISO")

# Filter by the municipality's IBGE code
df = await mapa_psr.apolices(cd_ibge="4305108")

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
| `municipio` | str \| None | None | Filter by the published municipality label (partial match); see "Municipality and IBGE code" |
| `evento` | str \| None | None | Filter by predominant event (e.g. "seca") |
| `cd_ibge` | str \| None | None | Filter by the municipality's IBGE code, 7 digits as text (e.g. "4305108"); any other format raises `InvalidParameterError` before the download |
| `as_polars` | bool | False | If True, returns a `polars.DataFrame` |
| `return_meta` | bool | False | Returns a (DataFrame, MetaInfo) tuple |

`cultura` is filtered by a substring of the name published in the CSV (`NM_CULTURA_GLOBAL`). There is no static
catalog: the list comes in the CSV itself (311 MB in the most recent period), so a crop outside it only shows up as an
empty result, after the download.

## Columns — `sinistros`

| Column | Type | Nullable | Description |
|---|---|---|---|
| `nr_apolice` | str | No | Policy number |
| `ano_apolice` | int | No | Policy year |
| `uf` | str | No | State abbreviation of the property |
| `municipio` | str | Yes | Municipality name |
| `cd_ibge` | str | Yes | IBGE code of the municipality; null when MAPA publishes "-" instead of the geocode (1,516 policies between 2006 and 2025), and `municipio` keeps the published name |
| `cultura` | str | No | Insured crop (uppercase) |
| `classificacao` | str | Yes | Product classification (AGRICOLA, PECUARIO, etc.) |
| `evento` | str | No | Predominant event (lowercase) |
| `area_total` | float | Yes | Total insured area (ha) |
| `valor_indenizacao` | float | No | Indemnity amount (R$) — always > 0 |
| `valor_premio` | float | Yes | Net premium (R$) |
| `valor_subvencao` | float | Yes | Federal subsidy (R$) |
| `valor_limite_garantia` | float | Yes | Coverage limit (R$) |
| `produtividade_estimada` | float | Yes | Estimated yield; MAPA does not publish the unit |
| `produtividade_segurada` | float | Yes | Insured yield; MAPA does not publish the unit |
| `nivel_cobertura` | float | Yes | Coverage level as a fraction (0.65 = 65%): insured ÷ estimated yield, as published by MAPA |
| `seguradora` | str | Yes | Insurer legal name |

## Parameters — `apolices`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `cultura` | str \| None | None | Filter by crop (partial match, accent-insensitive) |
| `uf` | str \| None | None | Filter by state |
| `ano` | int \| None | None | Single-year filter |
| `ano_inicio` | int \| None | None | Start year of the range |
| `ano_fim` | int \| None | None | End year of the range |
| `municipio` | str \| None | None | Filter by the published municipality label (partial match) |
| `cd_ibge` | str \| None | None | Filter by the municipality's IBGE code (7 digits as text) |
| `as_polars` | bool | False | If True, returns a `polars.DataFrame` |
| `return_meta` | bool | False | Returns a (DataFrame, MetaInfo) tuple |

## Columns — `apolices`

Same columns as `sinistros`, plus:

| Column | Type | Nullable | Description |
|---|---|---|---|
| `taxa` | float | Yes | Premium rate as a fraction (0.1369 = 13.69%): net premium ÷ guarantee limit, as published by MAPA |

## Data pipeline

1. Resolves the periods from the year filters: 2006-2015, 2016-2024 and 2025 come from the fixed dictionary; a year after 2025 (and, without `ano_fim`, up to the current year) is looked up in the package catalog on MAPA's CKAN. A year with no file in the catalog comes with a warning (`UserWarning` and `meta.validation_warnings`), and, with no file at all, `meta.source_url` points to the catalog queried. With the catalog down, the fixed years go ahead with a warning; with no fixed year in the request, the failure is raised as `SourceUnavailableError`
2. Streams one period at a time into a temporary file
3. Validates encoding across the complete file (UTF-8 with BOM support → Windows-1252 → ISO-8859-1), separator (`;` or `,`), quotes, unique headers, record width and policy years
4. Reads chunks of 10,000 rows and removes PII columns (NM_SEGURADO, NR_DOCUMENTO_SEGURADO) and geolocation
5. Normalizes headers/text and applies year, state, crop, municipality and IBGE code filters to each chunk
6. Converts selected numeric values (float64 for money, int for year)
7. For claims, filters VALOR_INDENIZACAO > 0 and non-empty EVENTO_PREPONDERANTE; assembles the result and closes each period's temporary file

Accented headers such as `VALOR_INDENIZAÇÃO` are normalized before mapping.
If the indemnity column cannot be identified, `sinistros` raises `ParseError`;
policies without indemnity amounts are not returned as claims.

## Municipality and IBGE code

`municipio=` searches the text of the label MAPA publishes in `NM_MUNICIPIO_PROPRIEDADE`, and some
policies are labelled with the district name. In Caxias do Sul (code 4305108), in 2024, 274 of the
693 policies with that code carry "Caxias do Sul"; the other 419 appear as Fazenda Souza (241),
Criúva (78), Vila Oliva (48), Vila Seca (32) and Santa Lúcia do Piaí (20). For the whole
municipality, use `cd_ibge=`, which compares the code published in `CD_GEOCMU`. The code filter does
not catch policies published with "-" instead of the geocode (1,516 between 2006 and 2025, with a
null `cd_ibge`): those only have the label. Without the `CD_GEOCMU` column in the file, `cd_ibge=`
raises `ParseError`.

## MetaInfo

```python
df, meta = await mapa_psr.sinistros(return_meta=True)
print(meta.source)           # "mapa_psr"
print(meta.source_method)    # "httpx"
print(meta.parser_version)   # 4
print(meta.records_count)    # varies by filter
```

## Performance note

SISSER CSVs can be large. The module downloads only the required periods and
filters each chunk before converting all numeric columns. Selective filters
reduce processing memory; the source still requires a full download of each
selected period.

The temporary file uses disk space and is closed on success, error or
cancellation. The API returns a complete DataFrame: unfiltered queries still
need memory proportional to their result. Whole-file CSV validation and chunked
reading may increase processing time. HTTP read timeout: 180 seconds.

Results are sorted by `ano_apolice`. Order within the same year is not
guaranteed; sort explicitly by the columns relevant to your analysis.

## Datasets

- [`seguro_rural`](../contracts/seguro_rural.md) — wraps `mapa_psr.apolices()` and `mapa_psr.sinistros()` via `tipo=` dispatch

## Source

- URL: `https://dados.agricultura.gov.br/dataset/sisser3`
- Format: CSV (3 files per period)
- Update frequency: annual
- History: 2006+
- License: `livre` (CC-BY, federal government public data)

## Policy integrity and periods

The complete CSV is validated before filters are applied. Duplicate headers, records with too many or too few fields, and invalid policy years raise `ParseError` with the record position; these rows are not silently discarded. Quoted fields may contain delimiters and line breaks. The parser is version 4; the policies contract is at 1.1 and the claims contract remains at 1.0.

**Key and record published twice (`mapa_psr_apolices` contract 1.1).** The policy key is `nr_apolice`, `ano_apolice`, `uf`, `cultura`, `cd_ibge` and `seguradora`: the policy number is only unique within the insurer (in 2007, 2008, 2009, 2011 and 2012 MAPA publishes the same number under two insurers, with different area and premium). A record published twice and identical in every column agrobr delivers (in 2009, Mapfre policy 1977000249501, with a resubmitted proposal) is returned once, with a warning (`warn_once`) and the count in `source_details["duplicatas_colapsadas"]`. A repeated key with any different value raises `ContractViolationError` (`SourceUnavailableError` in the dataset).

`ano_apolice` is the year the policy was contracted, according to the SISSER dictionary; it is not the event or payment date. `sinistros` selects positive indemnities with a non-empty event. Published zeros remain zero in `apolices`; missing values remain null. Monetary values are not rounded to cents. Policy numbers and geographic codes retain leading zeros. Apart from the identical record published twice, described above, no row is deduplicated.

In the 2026-09-18 capture, the catalogue offered three CSVs, through 2025. The 2025 file had missing indemnity amounts, which does not establish that no claims occurred. EOF confirms that the published file was fully read; it does not guarantee complete programme coverage or up-to-date payments.

Text fields preserve literals such as `NULL`, `NA`, `None` and `N/A`, subject only to the existing whitespace and case normalization; the CSV reader does not convert them into missing values. Empty text remains empty, except `cd_ibge`, which becomes null. Numeric fields retain the existing conversion: missing or uninterpretable values remain null, without turning text tokens into zero.
