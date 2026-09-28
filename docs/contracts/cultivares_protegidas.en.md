# Contract: cultivares_protegidas

The dataset reuses source contract **`rnc_protegidas` 1.0**, constant `RNC_PROTEGIDAS_V1`, internal name `rnc.protegidas`. This table describes the current SNPC query; it is not joined to RNC by cultivar name. See the [dataset API](../api/cultivares.en.md).

## Schema

The previous eleven columns remain as a prefix. `termino_protecao_texto` is the twelfth column. Text fields contain strings emitted by the parser as pandas `object`; civil dates use `datetime64[ns]`, without timezone or time of day.

| Column | pandas type | Nullable | Meaning |
|---|---|---|---|
| `cultivar` | str | No | Published cultivar name |
| `nome_cientifico` | str | No | Published scientific name |
| `nome_comum` | str | No | Common species name |
| `nr_processo` | str | No | Process number, a nonblank textual key |
| `situacao` | str | No | Published protection status |
| `nr_certificado` | str | No | Certificate number; may repeat across processes |
| `inicio_protecao` | datetime64[ns] | Yes | Published civil start date |
| `termino_protecao` | datetime64[ns] | Yes | Civil end date when determined |
| `titular` | str | No | Holder text, without splitting names |
| `representante_legal` | str | No | Legal representative text |
| `melhoristas` | str | No | Breeder text; published empty strings are retained |
| `termino_protecao_texto` | str | No | Trimmed original end cell, including date, condition or empty value |

**Primary key:** `nr_processo`, within the processed family and resource. A certificate is not a key: distinct processes may publish the same number. Do not deduplicate by certificate, cultivar, holder or date. IDs remain strings, preserving punctuation and leading zeros.

## Determined, conditional and missing end dates

| Trimmed published text | `termino_protecao` | `termino_protecao_texto` |
|---|---|---|
| `08/08/2034` | Civil date 2034-08-08 | `"08/08/2034"` |
| `até a emissão do certificado definitivo` | `NaT` | The full original phrase |
| Empty cell | `NaT` | `""` |

Only an empty cell or that exact official phrase allows a null end date. A valid textual date requires the matching scalar; invalid text, impossible dates and text/scalar disagreement fail. No assumed date is chosen, and arbitrary text is not silently converted to `NaT`. The condition does not automatically determine status: records with different statuses may retain it.

`inicio_protecao` allows absence without imputation. Both dates require ns precision, without timezone or time of day, including empty results. Civil dates do not receive the UTC collection timestamp.

## Validation, empties and limits

The source and dataset always validate the contract, including `return_meta=False`. The entire population is validated before filtering. Missing required columns, duplicate labels, missing/blank/duplicate keys and incompatible types cause an error. The source also validates the external layout and fields through row models.

Empty strings remain strings, rather than numbers or null markers. Compound name fields are not split and published repetitions are not removed. No-match selections retain all twelve columns and their types; an invalid CSV is not reinterpreted as an empty selection.

The table is not an assessment of legal validity. Status, dates and holders are published fields, without inferring marketing rights, protection extensions or an automatic relationship with RNC registration.

## Metadata and filters

`schema_version` and `contract_version` are `"1.0"`; there is no contract alias named `cultivares_protegidas`. The route is `rnc_protegidas`, retained in `selected_source` and `attempted_sources`, including cache reuse.

Hash and size describe the raw CSV; original collection time, expiry and source diagnostics remain in metadata. `records_count` describes the selection; the full count and comparison against the search total are in `source_details.coverage`. Matching counts do not guarantee a transactional snapshot.

`nr_processo` and `nr_certificado` use full textual equality after trimming surrounding whitespace. Other filters use literal case-insensitive substrings; `especie` searches the common name. The dataset rejects `deterministic` contexts before I/O because it cannot select historical revisions. See the [source](../sources/rnc.en.md) and the [registered-cultivar contract](cultivares_registradas.en.md).

## Reconciliation of the 2026-09-18 capture

The complete CSVs captured on this date contain 38,325 RNC records and 5,424 SNPC records. An independent oracle built from the original CSVs without importing the parser or contract checks all 10/12 columns and all 43,749 records through source and dataset APIs, with and without cache. The preceding HTML search count matches the CSV in the same session; this agreement establishes neither a transactional snapshot nor historical completeness.

In RNC, the published 22,946 blank form numbers, 4,256 blank cultivar names and 4,271 blank maintainers remain empty text; both dates are populated in this capture. There are 924 repeated form-number groups with 15,142 occurrences. In SNPC, protection end has 5,267 dates, 155 conditions and two blanks; the 155 conditions and two blanks produce null dates while retaining the text. There are 102 certificate numbers repeated across distinct processes, with 204 occurrences. These secondary-identifier repetitions are not deduplicated.

The original acquisition in `fetched_at` retains timezone-aware UTC through cache and dataset calls. At both source and dataset level, `fetch_timestamp` equals acquisition time. Table dates are civil dates independent of these timestamps. Parser version remains 2 and contracts remain 1.0; this reconciliation does not change public behavior.
