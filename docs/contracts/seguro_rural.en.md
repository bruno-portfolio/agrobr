# seguro_rural v2.1

Rural insurance — PSR policies and claims (MAPA).

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | MAPA PSR | Rural Insurance Premium Subsidy Program |

## Products

100+ dynamic PSR crops (validation delegated to the source).

## Query types

The dataset supports two query types via the `tipo` parameter:

- `tipo="apolices"` (default) — all policies with federal subsidy
- `tipo="sinistros"` — reported positive indemnities with a non-empty event

Each type has its own contract (`mapa_psr_apolices` 2.1 and `mapa_psr_sinistros` 1.2). `evento` only filters `tipo="sinistros"`: with `tipo="apolices"`, it raises `InvalidParameterError` before the request.

The current policies contract is `MAPA_PSR_APOLICES_V2`, effective since agrobr 2.0.0.

## Schema — Policies

| Column | Type | Nullable | Unit | Stable |
|--------|------|----------|------|--------|
| `nr_apolice` | str | ❌ | - | Yes |
| `ano_apolice` | int | ❌ | - | Yes |
| `uf` | str | ❌ | - | Yes |
| `municipio` | str | ✅ | - | Yes |
| `cd_ibge` | str | ✅ | - | Yes |
| `cod_municipio` | int | ✅ | - | No |
| `cultura` | str | ❌ | - | Yes |
| `classificacao` | str | ✅ | - | Yes |
| `area_total` | float | ✅ | ha | Yes |
| `valor_premio` | float | ✅ | BRL | Yes |
| `valor_subvencao` | float | ✅ | BRL | Yes |
| `valor_limite_garantia` | float | ✅ | BRL | Yes |
| `valor_indenizacao` | float | ✅ | BRL | Yes |
| `evento` | str | ✅ | - | Yes |
| `produtividade_estimada` | float | ✅ | not published | Yes |
| `produtividade_segurada` | float | ✅ | not published | Yes |
| `nivel_cobertura` | float | ✅ | - | Yes |
| `taxa` | float | ✅ | - | Yes |
| `seguradora` | str | ❌ | - | Yes |
| `inicio_vigencia` | date | ✅ | - | No |
| `fim_vigencia` | date | ✅ | - | No |
| `data_apolice` | date | ✅ | - | No |

`produtividade_estimada` and `produtividade_segurada` have no canonical unit: MAPA publishes both numbers without one. Their
ratio is `nivel_cobertura`.

## Schema — Claims

| Column | Type | Nullable | Unit | Stable |
|--------|------|----------|------|--------|
| `nr_apolice` | str | ❌ | - | Yes |
| `ano_apolice` | int | ❌ | - | Yes |
| `uf` | str | ❌ | - | Yes |
| `municipio` | str | ✅ | - | Yes |
| `cd_ibge` | str | ✅ | - | Yes |
| `cod_municipio` | int | ✅ | - | No |
| `cultura` | str | ❌ | - | Yes |
| `classificacao` | str | ✅ | - | Yes |
| `evento` | str | ❌ | - | Yes |
| `area_total` | float | ✅ | ha | Yes |
| `valor_indenizacao` | float | ❌ | BRL | Yes |
| `valor_premio` | float | ✅ | BRL | Yes |
| `valor_subvencao` | float | ✅ | BRL | Yes |
| `valor_limite_garantia` | float | ✅ | BRL | Yes |
| `produtividade_estimada` | float | ✅ | not published | Yes |
| `produtividade_segurada` | float | ✅ | not published | Yes |
| `nivel_cobertura` | float | ✅ | - | Yes |
| `seguradora` | str | ✅ | - | Yes |
| `inicio_vigencia` | date | ✅ | - | No |
| `fim_vigencia` | date | ✅ | - | No |
| `data_apolice` | date | ✅ | - | No |

## Guarantees

- `uf` is a valid state code
- Monetary values in BRL
- Data since 2006 (PSR inception)
- Claims: `valor_indenizacao` is always > 0, `evento` is always filled
- Policies: `valor_indenizacao` may be null/0

## Example

```python
from agrobr import datasets

# Policies (default)
df = await datasets.seguro_rural()
df = await datasets.seguro_rural("soja", uf="MT", ano=2023)

# Whole municipality, by IBGE code or full name
df = await datasets.seguro_rural(municipio=4305108, ano=2024)
df = await datasets.seguro_rural(municipio="Caxias do Sul", ano=2024)

# Claims
df = await datasets.seguro_rural(tipo="sinistros")
df = await datasets.seguro_rural(tipo="sinistros", evento="SECA")

# With metadata
df, meta = await datasets.seguro_rural(return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.seguro_rural()
```

## JSON Schema

```python
from agrobr.contracts import get_contract
# Policies
contract = get_contract("mapa_psr_apolices")
# Claims
contract = get_contract("mapa_psr_sinistros")
```

## Policy integrity and periods

The complete CSV is validated before filters are applied. Duplicate headers, records with too many or too few fields, and invalid policy years raise `ParseError` with the record position; these rows are not silently discarded. Quoted fields may contain delimiters and line breaks. The parser is version 5; the policies contract is at 2.1 and the claims contract at 1.2. `ano_apolice` comes as `Int64`, with rows and when empty.

**Key and record published twice (`mapa_psr_apolices` contract since 2.0).** The policy key is `nr_apolice`, `ano_apolice`, `uf`, `cultura`, `cd_ibge` and `seguradora`: the policy number is only unique within the insurer (in 2007, 2008, 2009, 2011 and 2012 MAPA publishes the same number under two insurers, with different area and premium). A record published twice and identical in every column agrobr delivers (in 2009, Mapfre policy 1977000249501, with a resubmitted proposal) is returned once, with a warning (`warn_once`) and the count in `source_details["duplicatas_colapsadas"]`. A repeated key with any different value raises `ContractViolationError` (`SourceUnavailableError` in the dataset).

**Policy dates (contracts `mapa_psr_apolices` 2.1 and `mapa_psr_sinistros` 1.2).** `inicio_vigencia`, `fim_vigencia` and `data_apolice` come from `DT_INICIO_VIGENCIA`, `DT_FIM_VIGENCIA` and `DT_APOLICE`, published as `dd/mm/yyyy` in all three files, and are returned as `datetime64[ns]`. The year of `data_apolice` is `ano_apolice` in all 1,712,385 rows from 2006 to 2025 (checked on 2026-10-08). Coverage dates exist from 2016 on: in the 2006–2015 file, start and end are equal in all 617,683 rows (2016-07-22 or 2016-07-23, after the policies), so coverage was not published. Where start equals end, both coverage columns are null, with the warning "PSR: vigência não publicada (início igual ao fim) em N registro(s); saem nulas. Use data_apolice" (`UserWarning` and `meta.validation_warnings`, once per query; N counts the published records that passed the filters, before the record published twice is collapsed). An unreadable date, or one with a year outside 1900–2099, becomes `NaT`, with one warning per column in the query. A file without the column gives an all-null column. `DT_PROPOSTA` is not published: the 2009 resubmitted policy differs only there, and the record published twice is returned once.

**Municipality and IBGE code.** `municipio=` accepts the 7-digit IBGE code or the full municipality name (ignoring case and accents; a fragment of a name raises `InvalidParameterError` listing the candidates) and filters by the published code. MAPA labels some policies with the district name: in Caxias do Sul (4305108), in 2024, 274 of the 693 policies with that code carry the municipality name, and the filter returns all 693. Policies published with "-" instead of the geocode (null `cd_ibge`) are included when the label is the full municipality name, in the same state. Details in the [MAPA PSR source](../sources/mapa_psr.md#municipality-and-ibge-code).

`ano_apolice` is the year the policy was contracted, according to the SISSER dictionary; it is not the event or payment date. `sinistros` selects positive indemnities with a non-empty event. Published zeros remain zero in `apolices`; missing values remain null. Monetary values are not rounded to cents. Policy numbers and geographic codes retain leading zeros. Apart from the identical record published twice, described above, no row is deduplicated.

In the 2026-09-18 capture, the catalogue offered three CSVs, through 2025. The 2025 file had missing indemnity amounts, which does not establish that no claims occurred. EOF confirms that the published file was fully read; it does not guarantee complete programme coverage or up-to-date payments.

Text fields preserve literals such as `NULL`, `NA`, `None` and `N/A`, subject only to the existing whitespace and case normalization; the CSV reader does not convert them into missing values. Empty text remains empty, except `cd_ibge`, which becomes null. Numeric fields retain the existing conversion: missing or uninterpretable values remain null, without turning text tokens into zero.
