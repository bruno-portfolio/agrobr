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
df = await mapa_psr.sinistros(produto="SOJA", uf="MT")

# Filter by year or range
df = await mapa_psr.sinistros(ano=2023)
df = await mapa_psr.sinistros(ano_inicio=2020, ano_fim=2024)

# Filter by predominant event
df = await mapa_psr.sinistros(evento="seca")

# Filter by municipality, by name or IBGE code (see "Municipality and IBGE code")
df = await mapa_psr.sinistros(municipio="Sorriso", uf="MT")
df = await mapa_psr.apolices(municipio=4305108)

# All policies (including those without a claim)
df = await mapa_psr.apolices()

# Filtered policies
df = await mapa_psr.apolices(produto="MILHO", uf="PR", ano=2023)

# Synchronous API
from agrobr.sync import alt
df = alt.mapa_psr.sinistros(produto="SOJA")
df = alt.mapa_psr.apolices(uf="MT")
```

## Parameters — `sinistros`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `produto` | str \| None | None | Filter by crop (partial match, accent-insensitive, e.g. "cafe" matches "CAFE ARABICA") |
| `uf` | str \| None | None | Filter by state (abbreviation, e.g. "MT") |
| `ano` | int \| None | None | Single-year filter (e.g. 2023) |
| `ano_inicio` | int \| None | None | Start year of the range (inclusive) |
| `ano_fim` | int \| None | None | End year of the range (inclusive) |
| `municipio` | int \| str \| None | None | 7-digit IBGE code or full municipality name; see "Municipality and IBGE code" |
| `evento` | str \| None | None | Filter by predominant event (e.g. "seca") |
| `as_polars` | bool | False | If True, returns a `polars.DataFrame`; keyword-only, like `return_meta` |
| `return_meta` | bool | False | Returns a (DataFrame, MetaInfo) tuple |

`produto` is filtered by a substring of the name published in the CSV (`NM_CULTURA_GLOBAL`). There is no static
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
| `cod_municipio` | int | Yes | IBGE municipality code (`Int64`) taken from `cd_ibge`; null when `cd_ibge` is null |
| `inicio_vigencia` | datetime | Yes | Coverage start (`DT_INICIO_VIGENCIA`); null where start equals end (coverage not published; all of 2006–2015) |
| `fim_vigencia` | datetime | Yes | Coverage end (`DT_FIM_VIGENCIA`); null together with `inicio_vigencia` when the published start and end are equal; an unreadable date, or one with a year outside 1900–2099, nulls only this column |
| `data_apolice` | datetime | Yes | Policy date (`DT_APOLICE`); its year is `ano_apolice` |

## Parameters — `apolices`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `produto` | str \| None | None | Filter by crop (partial match, accent-insensitive) |
| `uf` | str \| None | None | Filter by state |
| `ano` | int \| None | None | Single-year filter |
| `ano_inicio` | int \| None | None | Start year of the range |
| `ano_fim` | int \| None | None | End year of the range |
| `municipio` | int \| str \| None | None | 7-digit IBGE code or full municipality name; see "Municipality and IBGE code" |
| `as_polars` | bool | False | If True, returns a `polars.DataFrame`; keyword-only, like `return_meta` |
| `return_meta` | bool | False | Returns a (DataFrame, MetaInfo) tuple |

## Columns — `apolices`

Same columns as `sinistros`, plus:

| Column | Type | Nullable | Description |
|---|---|---|---|
| `taxa` | float | Yes | Premium rate as a fraction (0.1369 = 13.69%): net premium ÷ guarantee limit, as published by MAPA |

## Data pipeline

1. Resolves the periods from the year filters: 2006-2015, 2016-2024 and 2025 come from the fixed dictionary; a year after 2025 (and, without `ano_fim`, up to the current year) is looked up in the package catalog on MAPA's CKAN. A year with no file in the catalog comes with a warning (`UserWarning` and `meta.validation_warnings`), and, with no file at all, `meta.source_url` points to the catalog queried. With the catalog down, the fixed years go ahead with a warning; with no fixed year in the request, the failure is raised as `SourceUnavailableError`
2. Streams one period at a time into a temporary file
3. Validates encoding across the complete file (UTF-8 with BOM support → Windows-1252 → ISO-8859-1), separator (`;` or `,`), quotes, unique headers, record width and policy years, and records the file's latest `DT_APOLICE` (in `source_details["corpos"][i]["ultima_apolice"]`), before any filter
4. Reads chunks of 10,000 rows and removes PII columns (NM_SEGURADO, NR_DOCUMENTO_SEGURADO) and geolocation
5. Normalizes headers/text and applies year, state, crop, municipality and IBGE code filters to each chunk
6. Converts selected numeric values (float64 for money, int for year)
7. For claims, filters VALOR_INDENIZACAO > 0 and non-empty EVENTO_PREPONDERANTE; assembles the result and closes each period's temporary file

Accented headers such as `VALOR_INDENIZAÇÃO` are normalized before mapping.
If the indemnity column cannot be identified, `sinistros` raises `ParseError`;
policies without indemnity amounts are not returned as claims.

## Municipality and IBGE code

`municipio=` accepts the 7-digit IBGE code (`int` or `str`) or the full municipality name, ignoring
case and accents, and is resolved by `normalize.resolver_municipio` before the download. A fragment of
a name, a name from another state and a name shared by more than one municipality without `uf` raise
`InvalidParameterError` listing the candidates. The filter compares the code published in
`CD_GEOCMU`, so it also catches the policies MAPA labels with the district name in
`NM_MUNICIPIO_PROPRIEDADE`: in Caxias do Sul (code 4305108), in 2024, 274 of the 693 policies with that
code carry "Caxias do Sul", and the other 419 appear as Fazenda Souza (241), Criúva (78), Vila Oliva
(48), Vila Seca (32) and Santa Lúcia do Piaí (20). Policies published with "-" instead of the geocode
(1,516 between 2006 and 2025, with a null `cd_ibge`) are included when the label is the full
municipality name, in the same state. Without the `CD_GEOCMU` column in the file, the filter uses only
the full name and the state.

## MetaInfo

```python
df, meta = await mapa_psr.sinistros(return_meta=True)
print(meta.source)           # "mapa_psr"
print(meta.source_method)    # "httpx"
print(meta.parser_version)   # 5
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

The complete CSV is validated before filters are applied. Duplicate headers, records with too many or too few fields, and invalid policy years raise `ParseError` with the record position; these rows are not silently discarded. Quoted fields may contain delimiters and line breaks. The parser is version 5; the policies contract is at 2.1 and the claims contract at 1.2.

**Key and record published twice (`mapa_psr_apolices` contract since 2.0).** The policy key is `nr_apolice`, `ano_apolice`, `uf`, `cultura`, `cd_ibge` and `seguradora`: the policy number is only unique within the insurer (in 2007, 2008, 2009, 2011 and 2012 MAPA publishes the same number under two insurers, with different area and premium). A record published twice and identical in every column agrobr delivers (in 2009, Mapfre policy 1977000249501, with a resubmitted proposal) is returned once, with a warning (`warn_once`) and the count in `source_details["duplicatas_colapsadas"]`. A repeated key with any different value raises `ContractViolationError` (`SourceUnavailableError` in the dataset).

`ano_apolice` is the year the policy was contracted, according to the SISSER dictionary; it is not the event or payment date. `sinistros` selects positive indemnities with a non-empty event. Published zeros remain zero in `apolices`; missing values remain null. Monetary values are not rounded to cents. Policy numbers and geographic codes retain leading zeros. Apart from the identical record published twice, described above, no row is deduplicated.

In the 2026-09-18 capture, the catalogue offered three CSVs, through 2025. The 2025 file had missing indemnity amounts, which does not establish that no claims occurred. EOF confirms that the published file was fully read; it does not guarantee complete programme coverage or up-to-date payments.

**Year published halfway.** MAPA publishes the current year before it ends and does not update the file afterwards: on 2026-10-08, the 2025 CSV was still the one from 2025-09-03, with 46,137 policies from 2025-01-02 to 2025-08-21 and none for soybean, first-crop corn, rice or cotton. The gap is not explained by the August cutoff: in the 2016–2024 file, 43,157 soybean policies from 2024 have a `DT_APOLICE` between January and August. `apolices(produto="soja", ano=2025)` comes back empty because the published file does not carry soybean, which does not establish a lack of insurance. When the file's latest `DT_APOLICE` falls before October 1 of its own year and that year is in the request, `apolices`, `sinistros` and `datasets.seguro_rural` warn (`UserWarning` and `meta.validation_warnings`): "PSR: o arquivo publicado pelo MAPA tem apólices até 21/08/2025; 2025 pode estar incompleto". The cutoff comes from the closed files: from 2006 to 2024, the earliest-ending year was 2022, on 10/24. A file cut between October and December passes without a warning, and a year in the middle of a file is not checked, because the file continues past it.

**Policy dates (contracts `mapa_psr_apolices` 2.1 and `mapa_psr_sinistros` 1.2).** `inicio_vigencia`, `fim_vigencia` and `data_apolice` come from `DT_INICIO_VIGENCIA`, `DT_FIM_VIGENCIA` and `DT_APOLICE`, published as `dd/mm/yyyy` in all three files, and are returned as `datetime64[ns]`. The year of `data_apolice` is `ano_apolice` in all 1,712,385 rows from 2006 to 2025 (checked on 2026-10-08). Coverage dates exist from 2016 on: in the 2006–2015 file, start and end are equal in all 617,683 rows (2016-07-22 or 2016-07-23, after the policies), so coverage was not published. Where start equals end, both coverage columns are null, with the warning "PSR: vigência não publicada (início igual ao fim) em N registro(s); saem nulas. Use data_apolice" (`UserWarning` and `meta.validation_warnings`, once per query; N counts the published records that passed the filters, before the record published twice is collapsed). An unreadable date, or one with a year outside 1900–2099, becomes `NaT`, with one warning per column in the query. A file without the column gives an all-null column. `DT_PROPOSTA` is not published: the 2009 resubmitted policy differs only there, and the record published twice is returned once.

Text fields preserve literals such as `NULL`, `NA`, `None` and `N/A`, subject only to the existing whitespace and case normalization; the CSV reader does not convert them into missing values. Empty text remains empty, except `cd_ibge`, which becomes null. Numeric fields retain the existing conversion: missing or uninterpretable values remain null, without turning text tokens into zero.
