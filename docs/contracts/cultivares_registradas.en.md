# Contract: cultivares_registradas

The dataset reuses source contract **`rnc_registradas` 1.0**, constant `RNC_REGISTRADAS_V1`, internal name `rnc.registradas`. There is no contract alias using the dataset name. See the [cultivar dataset API](../api/cultivares.en.md).

## Schema

The ten columns retain the RNC API names and order. Text fields contain strings; the parser produces pandas `object` columns. Civil dates use `datetime64[ns]`, without timezone or time of day.

| Column | pandas type | Nullable | Meaning |
|---|---|---|---|
| `cultivar` | str | No | Published name; legitimate empty strings are retained |
| `nome_comum` | str | No | Common species name |
| `nome_cientifico` | str | No | Published scientific name |
| `grupo` | str | No | Species group |
| `situacao` | str | No | Published registration status |
| `nr_formulario` | str | No | Form number; may repeat or be empty |
| `nr_registro` | str | No | Registration number, a nonblank textual key |
| `data_registro` | datetime64[ns] | Yes | Civil registration date |
| `data_validade` | datetime64[ns] | Yes | Published civil validity date |
| `mantenedor` | str | No | Maintainer text; empty values and compound names are retained |

**Primary key:** `nr_registro`, within the processed family and resource. Registration and form numbers are different identifiers. A form number is not a key or an automatic replacement for a missing registration number. Identifiers are not converted to numbers; leading zeros and punctuation remain textual.

## Guarantees and limits

The source and dataset always validate the contract, including calls without metadata. The source validates all rows before filtering. Missing required columns, duplicate column labels, missing/blank/duplicate keys and incompatible types fail; cultivar or maintainer names are not deduplication keys.

A published empty text value remains `""`, distinct from null. Missing dates remain `NaT`; invalid date text is not silently coerced. Both dates allow absence without imputation. The contract requires ns civil dates and rejects timezone or time of day, including an all-null date column with the wrong physical type.

A filtered empty result retains all ten columns and their types. A physically empty or invalid CSV is not a legitimate empty search result. Status, validity and maintainer text are not interpreted as marketing authorization, ownership rights or agronomic advice. Multi-name fields are not split.

## Metadata and selection

`schema_version` and `contract_version` are `"1.0"`. At the dataset layer, `selected_source="rnc_registradas"` and `attempted_sources=["rnc_registradas"]`; caching does not switch the route to another family. Hash, size and collection time describe the complete CSV, while `records_count` and `columns` describe the selected result. Population diagnostics, comparison against the search total and filters are in `source_details`.

`nr_registro` and `nr_formulario` use full textual equality after trimming surrounding whitespace. Other filters use literal case-insensitive substrings; `especie` searches `nome_comum`. Blank or invalid parameters are rejected before cache or network access, without prohibiting published empty values in the table.

The export is current. Its hash identifies an acquisition rather than a selectable historical revision; the dataset rejects `deterministic` contexts before I/O. See the [RNC/SNPC source](../sources/rnc.en.md) and the [separate protected-cultivar contract](cultivares_protegidas.en.md).

## Reconciliation of the 2026-09-18 capture

The complete CSVs captured on this date contain 38,325 RNC records and 5,424 SNPC records. An independent oracle built from the original CSVs without importing the parser or contract checks all 10/12 columns and all 43,749 records through source and dataset APIs, with and without cache. The preceding HTML search count matches the CSV in the same session; this agreement establishes neither a transactional snapshot nor historical completeness.

In RNC, the published 22,946 blank form numbers, 4,256 blank cultivar names and 4,271 blank maintainers remain empty text; both dates are populated in this capture. There are 924 repeated form-number groups with 15,142 occurrences. In SNPC, protection end has 5,267 dates, 155 conditions and two blanks; the 155 conditions and two blanks produce null dates while retaining the text. There are 102 certificate numbers repeated across distinct processes, with 204 occurrences. These secondary-identifier repetitions are not deduplicated.

The original acquisition in `fetched_at` retains timezone-aware UTC through cache and dataset calls. At both source and dataset level, `fetch_timestamp` equals acquisition time. Table dates are civil dates independent of these timestamps. Parser version remains 2 and contracts remain 1.0; this reconciliation does not change public behavior.
